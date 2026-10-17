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
    pub(crate) holder: fleet_sync_lease::LeaseHolder,
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
            holder: control.holder.clone(),
        };
        match run(&mut scope).await {
            Ok(value) => {
                let mut next_holder = control.holder.clone();
                restore_authority(&mut transaction, &mut next_holder)
                    .await
                    .map_err(StoreError::backend)?;
                transaction.commit().await.map_err(StoreError::backend)?;
                control.holder = next_holder;
                self.database.authority_changes.send_replace(());
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
    pub async fn begin_seeding(
        &mut self,
    ) -> Result<fleet_sync_protocol::ReplicationPosition, OpenError> {
        crate::writer_registration_state::begin_seed(self.maintenance.connection, &mut self.holder)
            .await
    }
    pub async fn observe_registration(
        &mut self,
        lease: fleet_sync_protocol::LeaseStatus,
        started: SyncTime,
    ) -> Result<(), OpenError> {
        crate::writer_registration_state::record_grant(
            self.maintenance.connection,
            &mut self.holder,
            lease,
            started,
        )
        .await
    }
    pub async fn complete_seed(&mut self) -> Result<(), OpenError> {
        crate::writer_registration_state::ready_seed(self.maintenance.connection, &mut self.holder)
            .await
    }
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
