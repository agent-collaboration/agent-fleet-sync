use crate::{
    MaintenanceTransaction, OpenError, ReplicatedDatabase, ReplicatedSchema,
    writer_authority::restore_authority,
};
use durable_steps::{
    Admitted, AtomicStore, BoxFuture, Host, JournalScope, RequestKey, StartRefusal, StoreError,
    SyncTime, Task,
    storage::TransactionScope,
    turso_store::{TursoScope, migrate_journal},
};
use sqlx_core::connection::Connection;
use sqlx_turso::Turso;
use std::sync::Arc;

/// Durable local steps use the writer's existing capture-off connection and
/// writer guard. This wrapper owns no second connection to the replicated file.
pub struct WriterJournalStore<S: ReplicatedSchema, H: Host> {
    database: Arc<ReplicatedDatabase<S>>,
    host: Arc<H>,
}

pub struct WriterJournalScope<'transaction, H: Host> {
    pub(crate) maintenance: MaintenanceTransaction<'transaction>,
    pub(crate) host: &'transaction H,
}

impl<S: ReplicatedSchema> ReplicatedDatabase<S> {
    pub async fn journal_store<H: Host>(
        self: &Arc<Self>,
        host: Arc<H>,
    ) -> Result<WriterJournalStore<S, H>, OpenError> {
        let _control = self.control.lock().await;
        let mut connection = self.maintenance.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        migrate_journal(&mut transaction)
            .await
            .map_err(|error| OpenError::Storage {
                message: error.to_string(),
            })?;
        transaction.commit().await?;
        Ok(WriterJournalStore {
            database: self.clone(),
            host,
        })
    }
}

impl<S: ReplicatedSchema, H: Host> AtomicStore for WriterJournalStore<S, H> {
    type Scope<'transaction> = WriterJournalScope<'transaction, H>;
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
        let mut control = self.database.control.lock().await;
        let mut connection = self
            .database
            .maintenance
            .acquire()
            .await
            .map_err(StoreError::backend)?;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(StoreError::backend)?;
        restore_authority(&mut transaction, &mut control.holder)
            .await
            .map_err(StoreError::backend)?;
        let mut scope = WriterJournalScope {
            maintenance: MaintenanceTransaction::new(
                &mut transaction,
                control.statements.layouts().to_vec(),
            ),
            host: self.host.as_ref(),
        };
        match run(&mut scope).await {
            Ok(value) => {
                let mut next_holder = control.holder.clone();
                restore_authority(&mut transaction, &mut next_holder)
                    .await
                    .map_err(StoreError::backend)?;
                transaction.commit().await.map_err(StoreError::backend)?;
                control.holder = next_holder;
                Ok(value)
            }
            Err(error) => {
                transaction.rollback().await.map_err(StoreError::backend)?;
                Err(error)
            }
        }
    }
}

impl<'transaction, H: Host> TransactionScope for WriterJournalScope<'transaction, H> {
    type Database = Turso;
    type Executor<'query>
        = &'query mut MaintenanceTransaction<'transaction>
    where
        Self: 'query;
    fn executor(&mut self) -> Self::Executor<'_> {
        &mut self.maintenance
    }
}

impl<H: Host> WriterJournalScope<'_, H> {
    pub(crate) fn journal(&mut self) -> TursoScope<'_, H> {
        TursoScope::new(self.maintenance.connection, self.host)
    }
}

impl<H: Host> JournalScope for WriterJournalScope<'_, H> {
    async fn admit<T: Task>(
        &mut self,
        key: &RequestKey,
        input: &T::Input,
    ) -> Result<Admitted, StartRefusal> {
        self.journal().admit::<T>(key, input).await
    }
    async fn ensure_wake(&mut self, at: SyncTime) -> Result<(), StoreError> {
        self.journal().ensure_wake(at).await
    }
}
