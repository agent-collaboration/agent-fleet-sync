use crate::ObjectKey;
use durable_steps::{
    AtomicStore, BoxFuture, Host, StoreError,
    turso_store::{TursoScope, migrate_journal},
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
use tokio::sync::Mutex;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectOptions {
    pub foreign_keys: bool,
}
impl Default for ObjectOptions {
    fn default() -> Self {
        Self { foreign_keys: true }
    }
}

pub struct ObjectStore<H: Host> {
    directory: PathBuf,
    objects: Mutex<BTreeMap<ObjectKey, Arc<TokioObject<H>>>>,
}
pub struct TokioObject<H: Host> {
    key: ObjectKey,
    path: PathBuf,
    pub(crate) options: ObjectOptions,
    pub(crate) connection: Mutex<ObjectConnection>,
    host: Arc<H>,
    /// A publish also acquires this lock before draining the file connection.
    pub(crate) engine_work: Arc<Mutex<()>>,
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
        })
    }
    pub async fn object(
        &self,
        key: ObjectKey,
        options: ObjectOptions,
        host: Arc<H>,
    ) -> Result<Arc<TokioObject<H>>, StoreError> {
        let mut objects = self.objects.lock().await;
        if let Some(object) = objects.get(&key) {
            if object.options != options {
                return Err(StoreError::Aborted {
                    reason: "object connection options changed".into(),
                });
            }
            return Ok(object.clone());
        }
        let path = self.directory.join(format!("{}.db", key.as_str()));
        let mut connection = open_connection(&path, options).await?;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(StoreError::backend)?;
        migrate_journal(&mut transaction).await?;
        transaction.commit().await.map_err(StoreError::backend)?;
        let object = Arc::new(TokioObject {
            key: key.clone(),
            path,
            options,
            connection: Mutex::new(ObjectConnection::Open(Box::new(connection))),
            host,
            engine_work: Arc::new(Mutex::new(())),
        });
        objects.insert(key, object.clone());
        Ok(object)
    }
    pub async fn objects(&self) -> Vec<Arc<TokioObject<H>>> {
        self.objects.lock().await.values().cloned().collect()
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
                Ok(value)
            }
            Err(error) => {
                transaction
                    .rollback()
                    .await
                    .map_err(|error| E::from(StoreError::backend(error)))?;
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
        self.rule_transaction(run).await
    }
}
