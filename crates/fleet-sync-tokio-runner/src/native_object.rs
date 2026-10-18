use crate::ObjectKey;
use durable_steps::{
    AtomicStore, BoxFuture, Host, ReborrowScope, StoreError,
    turso_store::{TursoScope, TursoStore, migrate_journal},
};
use sqlx_turso::{
    TursoConnectOptions, TursoConnection,
    sqlx::{ConnectOptions, Connection},
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{Mutex, OnceCell, watch};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectOptions {
    pub foreign_keys: bool,
}
impl Default for ObjectOptions {
    fn default() -> Self {
        Self { foreign_keys: true }
    }
}

type ObjectSlot<H> = OnceCell<Arc<TokioObject<H>>>;

pub struct ObjectStore<H: Host> {
    directory: PathBuf,
    objects: Mutex<BTreeMap<ObjectKey, Arc<ObjectSlot<H>>>>,
    #[cfg(test)]
    pub(crate) initialization_connections: watch::Sender<(Option<ObjectKey>, usize)>,
}
pub struct TokioObject<H: Host> {
    key: ObjectKey,
    path: PathBuf,
    pub(crate) options: ObjectOptions,
    pub(crate) connection: Mutex<ObjectConnection>,
    host: Arc<H>,
    /// A publish also acquires this lock before draining the file connection.
    pub(crate) engine_work: Arc<Mutex<()>>,
    committed: watch::Sender<()>,
    #[cfg(feature = "test-fault")]
    transaction_fault: std::sync::atomic::AtomicU8,
}
#[cfg(feature = "test-fault")]
#[derive(Clone, Copy, Debug)]
pub enum TransactionReplyFault {
    Commit,
    Rollback,
}
pub(crate) enum ObjectConnection {
    Open(Box<TursoConnection>),
    Publishing,
    Closed,
}
impl<H: Host> ObjectStore<H> {
    pub async fn open(directory: impl AsRef<Path>) -> Result<Self, StoreError> {
        tokio::fs::create_dir_all(directory.as_ref())
            .await
            .map_err(StoreError::backend)?;
        Ok(Self {
            directory: directory.as_ref().into(),
            objects: Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            initialization_connections: watch::channel((None, 0)).0,
        })
    }
    pub async fn object(
        &self,
        key: ObjectKey,
        options: ObjectOptions,
        host: Arc<H>,
    ) -> Result<Arc<TokioObject<H>>, StoreError> {
        let slot = {
            let mut objects = self.objects.lock().await;
            objects
                .entry(key.clone())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        // The cell coalesces only this key. Errors and dropped initializer
        // futures release its permit so another acquisition can retry.
        let object = slot
            .get_or_try_init(|| async {
                let path = self.directory.join(format!("{}.db", key.as_str()));
                let mut connection = open_connection(&path, options).await?;
                #[cfg(test)]
                self.initialization_connections
                    .send_modify(|(opened, count)| {
                        *opened = Some(key.clone());
                        *count += 1;
                    });
                let mut transaction = connection
                    .begin_with("BEGIN IMMEDIATE")
                    .await
                    .map_err(StoreError::backend)?;
                migrate_journal(&mut transaction).await?;
                transaction.commit().await.map_err(StoreError::backend)?;
                Ok::<_, StoreError>(Arc::new(TokioObject {
                    key,
                    path,
                    options,
                    connection: Mutex::new(ObjectConnection::Open(Box::new(connection))),
                    host,
                    engine_work: Arc::new(Mutex::new(())),
                    committed: watch::channel(()).0,
                    #[cfg(feature = "test-fault")]
                    transaction_fault: 0.into(),
                }))
            })
            .await?;
        if object.options != options {
            return Err(StoreError::Aborted {
                reason: "object connection options changed".into(),
            });
        }
        Ok(object.clone())
    }
    pub async fn objects(&self) -> Vec<Arc<TokioObject<H>>> {
        self.objects
            .lock()
            .await
            .values()
            .filter_map(|slot| slot.get().cloned())
            .collect()
    }
    pub async fn close(&self) -> Result<(), StoreError> {
        for object in self.objects().await {
            object.close().await?;
        }
        Ok(())
    }
}

pub(crate) async fn open_connection(
    path: &Path,
    options: ObjectOptions,
) -> Result<TursoConnection, StoreError> {
    TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .foreign_keys(options.foreign_keys)
        .connect()
        .await
        .map_err(StoreError::backend)
}

pub(crate) async fn open_existing_connection(
    path: &Path,
    options: ObjectOptions,
) -> Result<TursoConnection, StoreError> {
    TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .foreign_keys(options.foreign_keys)
        .connect()
        .await
        .map_err(StoreError::backend)
}

impl<H: Host> TokioObject<H> {
    #[cfg(feature = "test-fault")]
    pub fn lose_transaction_reply_for_test(&self, fault: TransactionReplyFault) {
        self.transaction_fault.store(
            match fault {
                TransactionReplyFault::Commit => 1,
                TransactionReplyFault::Rollback => 2,
            },
            std::sync::atomic::Ordering::SeqCst,
        );
    }
    pub fn key(&self) -> &ObjectKey {
        &self.key
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn host(&self) -> Arc<H> {
        self.host.clone()
    }
    pub fn engine_lock(&self) -> Arc<Mutex<()>> {
        self.engine_work.clone()
    }
    /// Invalidation after an engine transaction commits. Readers subscribe
    /// before reading domain state, then re-read after the next invalidation.
    pub fn subscribe_commits(&self) -> watch::Receiver<()> {
        self.committed.subscribe()
    }
    pub(crate) fn notify_committed(&self) {
        self.committed.send_replace(());
    }
    pub async fn close(&self) -> Result<(), StoreError> {
        let _engine = self.engine_work.lock().await;
        let state = std::mem::replace(&mut *self.connection.lock().await, ObjectConnection::Closed);
        if let ObjectConnection::Open(connection) = state {
            (*connection).close().await.map_err(StoreError::backend)?;
        }
        Ok(())
    }
}
impl<H: Host> TokioObject<H> {
    /// A domain refusal rolls back through its own typed error. Effects returned
    /// by a successful rule become visible to the caller only after commit.
    pub async fn rule_transaction<T, E, F>(&self, run: F) -> Result<T, E>
    where
        T: Send + 'static,
        E: From<StoreError> + Send + 'static,
        F: for<'scope, 'transaction> FnOnce(
                &'scope mut TursoScope<'transaction, H>,
            ) -> BoxFuture<'scope, Result<T, E>>
            + Send
            + 'static,
    {
        self.rule_transaction_inner(run, true).await
    }
    async fn rule_transaction_inner<T, E, F>(&self, run: F, _inject_fault: bool) -> Result<T, E>
    where
        T: Send + 'static,
        E: From<StoreError> + Send + 'static,
        F: for<'scope, 'transaction> FnOnce(
                &'scope mut TursoScope<'transaction, H>,
            ) -> BoxFuture<'scope, Result<T, E>>
            + Send
            + 'static,
    {
        let mut guard = self.connection.lock().await;
        let ObjectConnection::Open(connection) = &mut *guard else {
            return Err(StoreError::Aborted {
                reason: "object is closed or publication needs replay".into(),
            }
            .into());
        };
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|error| E::from(StoreError::backend(error)))?;
        match run(&mut TursoScope::new(&mut transaction, self.host.as_ref())).await {
            Ok(value) => {
                transaction
                    .commit()
                    .await
                    .map_err(|error| E::from(StoreError::backend(error)))?;
                #[cfg(feature = "test-fault")]
                if _inject_fault
                    && self
                        .transaction_fault
                        .compare_exchange(
                            1,
                            0,
                            std::sync::atomic::Ordering::SeqCst,
                            std::sync::atomic::Ordering::SeqCst,
                        )
                        .is_ok()
                {
                    return Err(StoreError::backend(
                        "injected loss of committed transaction reply",
                    )
                    .into());
                }
                Ok(value)
            }
            Err(error) => {
                transaction
                    .rollback()
                    .await
                    .map_err(|error| E::from(StoreError::backend(error)))?;
                #[cfg(feature = "test-fault")]
                if _inject_fault
                    && self
                        .transaction_fault
                        .compare_exchange(
                            2,
                            0,
                            std::sync::atomic::Ordering::SeqCst,
                            std::sync::atomic::Ordering::SeqCst,
                        )
                        .is_ok()
                {
                    return Err(StoreError::backend(
                        "injected loss of rolled back transaction reply",
                    )
                    .into());
                }
                Err(error)
            }
        }
    }
}
impl<H: Host> AtomicStore for TokioObject<H> {
    type Scope<'transaction> = TursoScope<'transaction, H>;
    async fn transaction<T, F>(&self, run: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: for<'scope, 'transaction> FnOnce(
                &'scope mut Self::Scope<'transaction>,
            )
                -> BoxFuture<'scope, Result<T, StoreError>>
            + Send
            + 'static,
    {
        let result = self.rule_transaction_inner(run, false).await?;
        self.notify_committed();
        Ok(result)
    }
}

impl<H: Host> ReborrowScope for TokioObject<H> {
    fn reborrow<'s, 't: 's>(scope: &'s mut Self::Scope<'t>) -> Self::Scope<'s> {
        <TursoStore<H> as ReborrowScope>::reborrow(scope)
    }
}
