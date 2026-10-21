//! A Durable Object's SQL journal, business writes and alarm share its transaction.

use crate::{
    AtomicStore, BoxFuture, Host, ReborrowScope, StoreError, SyncTime,
    sql_journal::JournalQueries,
    sql_rows::{
        StoredCountRow, StoredIdRow, StoredOperationRow, StoredRequestKeyRow, StoredStepRow,
        StoredTimeRow,
    },
    storage::TransactionScope,
};
use runtime_primitives::{AlarmOwner, WakeScheduler};
use sqlx_core::{executor::Executor, query::query, query_as::query_as};
use sqlx_durable_object::{Do, DoConnection, DoTransaction};
use std::sync::Arc;
use uuid::Uuid;

#[derive(sqlx::FromRow)]
struct JournalTable {
    name: String,
}
#[derive(sqlx::FromRow)]
struct JournalVersion {
    version: i64,
}

pub struct DurableObjectStore<H: Host> {
    connection: Arc<DoConnection>,
    host: Arc<H>,
    alarm: AlarmOwner,
}

impl<H: Host> DurableObjectStore<H> {
    pub fn new(connection: DoConnection, host: Arc<H>, alarm: AlarmOwner) -> Self {
        Self {
            connection: Arc::new(connection),
            host,
            alarm,
        }
    }

    /// The DO driver cannot implement sqlx::migrate!, so bootstrap is an owned
    /// storage transaction with the same migration SQL and an explicit version.
    /// Refuse unregistered pre-existing journal tables instead of adopting them.
    pub async fn migrate(&self) -> Result<(), StoreError> {
        self.connection.transaction(move |transaction| async move {
            let tables: Vec<JournalTable> = query_as::<Do, _>(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name GLOB '_fleet_sync_*' ORDER BY name")
                .fetch_all(&transaction).await?;
            if tables.is_empty() {
                (&transaction).execute(include_str!("../migrations/0001_durable_steps.sql")).await?;
            } else if tables.iter().any(|row| row.name == "_fleet_sync_schema") {
                let versions: Vec<JournalVersion> = query_as::<Do, _>("SELECT version FROM _fleet_sync_schema")
                    .fetch_all(&transaction).await?;
                if versions.len() != 1 || versions.first().is_none_or(|row| row.version != 1) {
                    return Err(StoreError::Corrupt { field: "schema_version".into(), message: "unsupported journal schema".into() });
                }
            } else {
                return Err(StoreError::Corrupt { field: "schema_version".into(), message: "unregistered existing journal tables".into() });
            }
            Ok(())
        }).await
    }
}

pub struct DurableObjectScope<'t, H: Host> {
    transaction: &'t DoTransaction,
    host: &'t H,
    alarm: &'t AlarmOwner,
}

impl<H: Host> ReborrowScope for DurableObjectStore<H> {
    fn reborrow<'s, 't: 's>(scope: &'s mut Self::Scope<'t>) -> Self::Scope<'s> {
        DurableObjectScope {
            transaction: scope.transaction,
            host: scope.host,
            alarm: scope.alarm,
        }
    }
}

#[cfg(feature = "test-support")]
impl<H: Host> DurableObjectScope<'_, H> {
    /// Identify shared alarm arbitration without extending the parent's loan.
    pub fn alarm_arbitration_identity_for_test(&self) -> usize {
        self.alarm.arbitration_identity_for_test()
    }
}

impl<H: Host> TransactionScope for DurableObjectScope<'_, H> {
    type Database = Do;
    type Executor<'query>
        = &'query DoTransaction
    where
        Self: 'query;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.transaction
    }
}

impl<H: Host> AtomicStore for DurableObjectStore<H> {
    type Scope<'t> = DurableObjectScope<'t, H>;
    async fn transaction<T, F>(&self, run: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: for<'s, 't> FnOnce(&'s mut Self::Scope<'t>) -> BoxFuture<'s, Result<T, StoreError>>
            + Send
            + 'static,
    {
        let host = Arc::clone(&self.host);
        let alarm = self.alarm.clone();
        self.connection
            .transaction(move |transaction| async move {
                let mut scope = DurableObjectScope {
                    transaction: &transaction,
                    host: &*host,
                    alarm: &alarm,
                };
                run(&mut scope).await
            })
            .await
    }
}

// The DO driver has no describe/query! support. These are the identical SQL
// files checked through native Turso metadata; FromRow maps their named fields
// into private storage rows, then the shared decoder validates domain states.
impl<H: Host> JournalQueries for DurableObjectScope<'_, H> {
    async fn fetch_running_request_keys(&mut self) -> Result<Vec<StoredRequestKeyRow>, StoreError> {
        query_as::<Do, StoredRequestKeyRow>(include_str!("../queries/running_request_keys.sql"))
            .fetch_all(self.transaction)
            .await
            .map_err(StoreError::from)
    }
    async fn delete_steps(&mut self, id: String) -> Result<(), StoreError> {
        query::<Do>(include_str!("../queries/delete_steps.sql"))
            .bind(id)
            .execute(self.transaction)
            .await?;
        Ok(())
    }
    async fn delete_operation(&mut self, id: String, token: String) -> Result<bool, StoreError> {
        Ok(query::<Do>(include_str!("../queries/delete_operation.sql"))
            .bind(id)
            .bind(token)
            .execute(self.transaction)
            .await?
            .rows_affected()
            == 1)
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
        Ok(
            query_as::<Do, StoredOperationRow>(include_str!("../queries/operation.sql"))
                .bind(id)
                .fetch_optional(self.transaction)
                .await?,
        )
    }
    async fn fetch_request(
        &mut self,
        kind: String,
        key: String,
    ) -> Result<Option<StoredOperationRow>, StoreError> {
        Ok(
            query_as::<Do, StoredOperationRow>(include_str!("../queries/request.sql"))
                .bind(kind)
                .bind(key)
                .fetch_optional(self.transaction)
                .await?,
        )
    }
    async fn insert_operation(&mut self, row: StoredOperationRow) -> Result<(), StoreError> {
        query::<Do>(include_str!("../queries/insert_operation.sql"))
            .bind(row.operation_id)
            .bind(row.kind)
            .bind(row.kind_version)
            .bind(row.request_key)
            .bind(row.input)
            .bind(row.input_digest)
            .bind(row.state)
            .bind(row.result)
            .bind(row.failure)
            .bind(row.finished_at)
            .bind(row.next_attempt_at)
            .bind(row.attempt)
            .bind(row.drive_token)
            .bind(row.drive_until)
            .execute(self.transaction)
            .await?;
        Ok(())
    }
    async fn update_operation(
        &mut self,
        row: StoredOperationRow,
        token: Option<String>,
    ) -> Result<bool, StoreError> {
        let result = query::<Do>(include_str!("../queries/update_operation.sql"))
            .bind(row.state)
            .bind(row.result)
            .bind(row.failure)
            .bind(row.finished_at)
            .bind(row.input)
            .bind(row.next_attempt_at)
            .bind(row.attempt)
            .bind(row.drive_token)
            .bind(row.drive_until)
            .bind(row.operation_id)
            .bind(token)
            .execute(self.transaction)
            .await?;
        Ok(result.rows_affected() == 1)
    }
    async fn fetch_due(&mut self, now: i64) -> Result<Vec<String>, StoreError> {
        Ok(
            query_as::<Do, StoredIdRow>(include_str!("../queries/due.sql"))
                .bind(now)
                .bind(now)
                .fetch_all(self.transaction)
                .await?
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
        Ok(
            query_as::<Do, StoredStepRow>(include_str!("../queries/step.sql"))
                .bind(id)
                .bind(position)
                .fetch_optional(self.transaction)
                .await?,
        )
    }
    async fn upsert_step(&mut self, row: StoredStepRow) -> Result<(), StoreError> {
        query::<Do>(include_str!("../queries/upsert_step.sql"))
            .bind(row.operation_id)
            .bind(row.position)
            .bind(row.step_name)
            .bind(row.kind)
            .bind(row.intent)
            .bind(row.output)
            .bind(row.first_attempt_at)
            .bind(row.may_have_reached)
            .execute(self.transaction)
            .await?;
        Ok(())
    }
    async fn count_steps(&mut self, id: String) -> Result<i64, StoreError> {
        Ok(
            query_as::<Do, StoredCountRow>(include_str!("../queries/step_count.sql"))
                .bind(id)
                .fetch_one(self.transaction)
                .await?
                .count,
        )
    }
    async fn earliest_deadline(&mut self) -> Result<Option<i64>, StoreError> {
        Ok(
            query_as::<Do, StoredTimeRow>(include_str!("../queries/earliest.sql"))
                .fetch_one(self.transaction)
                .await?
                .at,
        )
    }
    async fn ensure_alarm(&mut self, at: SyncTime) -> Result<(), StoreError> {
        self.alarm.wake_at(at).await.map_err(StoreError::backend)
    }
}
