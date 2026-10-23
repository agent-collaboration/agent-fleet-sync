//! A journal and its application's writes share one real Turso transaction.

use crate::{
    AtomicStore, BoxFuture, Host, ReborrowScope, StoreError, SyncTime,
    sql_journal::JournalQueries,
    sql_rows::{
        StoredCountRow, StoredIdRow, StoredOperationRow, StoredRequestKeyRow, StoredStepRow,
        StoredTimeRow,
    },
    storage::TransactionScope,
};
use sqlx_turso::{
    Turso, TursoConnectOptions, TursoConnection,
    sqlx::{ConnectOptions, Connection},
};
use std::{path::Path, sync::Arc};
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct TursoStore<H: Host> {
    connection: Mutex<TursoConnection>,
    host: Arc<H>,
}

/// Connection settings applied before the store begins its migration transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TursoStoreOptions {
    pub foreign_keys: bool,
}

impl Default for TursoStoreOptions {
    fn default() -> Self {
        Self { foreign_keys: true }
    }
}

impl<H: Host> TursoStore<H> {
    pub async fn open_and_migrate(
        path: impl AsRef<Path>,
        host: Arc<H>,
    ) -> Result<Self, StoreError> {
        Self::open_with(path, TursoStoreOptions::default(), host).await
    }

    /// Open and migrate with connection settings chosen by the store's owner.
    pub async fn open_with(
        path: impl AsRef<Path>,
        options: TursoStoreOptions,
        host: Arc<H>,
    ) -> Result<Self, StoreError> {
        let mut connection = TursoConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(options.foreign_keys)
            .connect()
            .await
            .map_err(StoreError::backend)?;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(StoreError::backend)?;
        migrate_journal(&mut transaction).await?;
        transaction.commit().await.map_err(StoreError::backend)?;
        Ok(Self {
            connection: Mutex::new(connection),
            host,
        })
    }
}

/// Migrate the journal on the caller's connection. The caller owns the
/// surrounding transaction and its commit or rollback, just as for business SQL.
pub async fn migrate_journal(connection: &mut TursoConnection) -> Result<(), StoreError> {
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.dangerous_set_table_name("_fleet_sync_migrations");
    migrator
        .run_direct(None, connection, false)
        .await
        .map_err(StoreError::backend)
}

pub struct TursoScope<'t, H: Host> {
    connection: &'t mut TursoConnection,
    host: &'t H,
}

impl<H: Host> ReborrowScope for TursoStore<H> {
    fn reborrow<'s, 't: 's>(scope: &'s mut Self::Scope<'t>) -> Self::Scope<'s> {
        TursoScope::new(&mut *scope.connection, scope.host)
    }
}

impl<'t, H: Host> TursoScope<'t, H> {
    /// Borrow the caller's transaction connection and host. This scope never
    /// begins, commits or rolls back; the caller must provide the atomic boundary.
    pub fn new(connection: &'t mut TursoConnection, host: &'t H) -> Self {
        Self { connection, host }
    }
}

impl<H: Host> TransactionScope for TursoScope<'_, H> {
    type Database = Turso;
    type Executor<'query>
        = &'query mut TursoConnection
    where
        Self: 'query;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.connection
    }
}

impl<H: Host> AtomicStore for TursoStore<H> {
    type Scope<'t> = TursoScope<'t, H>;
    async fn transaction<T, F>(&self, run: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: for<'s, 't> FnOnce(&'s mut Self::Scope<'t>) -> BoxFuture<'s, Result<T, StoreError>>
            + Send
            + 'static,
    {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(StoreError::backend)?;
        let mut scope = TursoScope::new(&mut transaction, &*self.host);
        match run(&mut scope).await {
            Ok(output) => {
                transaction.commit().await.map_err(StoreError::backend)?;
                Ok(output)
            }
            Err(error) => {
                transaction.rollback().await.map_err(StoreError::backend)?;
                Err(error)
            }
        }
    }
}

impl<H: Host> JournalQueries for TursoScope<'_, H> {
    async fn fetch_running_request_keys(&mut self) -> Result<Vec<StoredRequestKeyRow>, StoreError> {
        sqlx_turso::query_file_as!(StoredRequestKeyRow, "queries/running_request_keys.sql")
            .fetch_all(&mut *self.connection)
            .await
            .map_err(StoreError::backend)
    }
    async fn delete_steps(&mut self, id: String) -> Result<(), StoreError> {
        sqlx_turso::query_file!("queries/delete_steps.sql", id)
            .execute(&mut *self.connection)
            .await?;
        Ok(())
    }
    async fn delete_operation(&mut self, id: String, token: String) -> Result<bool, StoreError> {
        Ok(
            sqlx_turso::query_file!("queries/delete_operation.sql", id, token)
                .execute(&mut *self.connection)
                .await?
                .rows_affected()
                == 1,
        )
    }
    fn now(&self) -> SyncTime {
        self.host.now()
    }
    fn new_id(&self) -> Uuid {
        self.host.new_id()
    }

    async fn fetch_operation(
        &mut self,
        id: String,
    ) -> Result<Option<StoredOperationRow>, StoreError> {
        sqlx_turso::query_file_as!(StoredOperationRow, "queries/operation.sql", id)
            .fetch_optional(&mut *self.connection)
            .await
            .map_err(StoreError::backend)
    }
    async fn fetch_request(
        &mut self,
        kind: String,
        key: String,
    ) -> Result<Option<StoredOperationRow>, StoreError> {
        sqlx_turso::query_file_as!(StoredOperationRow, "queries/request.sql", kind, key)
            .fetch_optional(&mut *self.connection)
            .await
            .map_err(StoreError::backend)
    }
    async fn insert_operation(&mut self, row: StoredOperationRow) -> Result<(), StoreError> {
        sqlx_turso::query_file!(
            "queries/insert_operation.sql",
            row.operation_id,
            row.kind,
            row.kind_version,
            row.request_key,
            row.input,
            row.input_digest,
            row.state,
            row.result,
            row.failure,
            row.finished_at,
            row.next_attempt_at,
            row.attempt,
            row.drive_token,
            row.drive_until
        )
        .execute(&mut *self.connection)
        .await
        .map_err(StoreError::backend)?;
        Ok(())
    }
    async fn update_operation(
        &mut self,
        row: StoredOperationRow,
        token: Option<String>,
    ) -> Result<bool, StoreError> {
        let result = sqlx_turso::query_file!(
            "queries/update_operation.sql",
            row.state,
            row.result,
            row.failure,
            row.finished_at,
            row.input,
            row.next_attempt_at,
            row.attempt,
            row.drive_token,
            row.drive_until,
            row.operation_id,
            token
        )
        .execute(&mut *self.connection)
        .await
        .map_err(StoreError::backend)?;
        Ok(result.rows_affected() == 1)
    }
    async fn fetch_due(&mut self, now: i64) -> Result<Vec<String>, StoreError> {
        Ok(
            sqlx_turso::query_file_as!(StoredIdRow, "queries/due.sql", now, now)
                .fetch_all(&mut *self.connection)
                .await
                .map_err(StoreError::backend)?
                .into_iter()
                .map(|row| row.operation_id)
                .collect(),
        )
    }
    async fn fetch_step(
        &mut self,
        id: String,
        position: i64,
    ) -> Result<Option<StoredStepRow>, StoreError> {
        sqlx_turso::query_file_as!(StoredStepRow, "queries/step.sql", id, position)
            .fetch_optional(&mut *self.connection)
            .await
            .map_err(StoreError::backend)
    }
    async fn upsert_step(&mut self, row: StoredStepRow) -> Result<(), StoreError> {
        sqlx_turso::query_file!(
            "queries/upsert_step.sql",
            row.operation_id,
            row.position,
            row.step_name,
            row.kind,
            row.intent,
            row.output,
            row.first_attempt_at,
            row.may_have_reached
        )
        .execute(&mut *self.connection)
        .await
        .map_err(StoreError::backend)?;
        Ok(())
    }
    async fn count_steps(&mut self, id: String) -> Result<i64, StoreError> {
        Ok(
            sqlx_turso::query_file_as!(StoredCountRow, "queries/step_count.sql", id)
                .fetch_one(&mut *self.connection)
                .await
                .map_err(StoreError::backend)?
                .count,
        )
    }
    async fn earliest_deadline(&mut self) -> Result<Option<i64>, StoreError> {
        Ok(
            sqlx_turso::query_file_as!(StoredTimeRow, "queries/earliest.sql")
                .fetch_one(&mut *self.connection)
                .await
                .map_err(StoreError::backend)?
                .at,
        )
    }
    async fn ensure_alarm(&mut self, _at: SyncTime) -> Result<(), StoreError> {
        // The durable journal is the native wake source; the host owns its timer.
        Ok(())
    }
}
