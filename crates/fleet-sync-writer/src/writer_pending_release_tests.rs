//! A failed later migration remains unapplied; release reports Pending, never Failed.
#![allow(clippy::panic_in_result_fn)]
use crate::*;
use durable_steps::{CallId, OperationId, StepName, Uuid};
use fleet_sync_lease::{LeasePolicy, WriterState};
use fleet_sync_protocol::{
    Epoch, MigrationState, PeerId, ReleaseAnswer, ReleaseRefusal, ReleaseRequest, TableName,
    TransferId,
};
use sqlx_core::{
    migrate::{MigrationType, Migrator},
    query::query,
    row::Row,
    sql_str::SqlSafeStr,
};
use sqlx_turso::Turso;
use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static REGULAR: Migrator = sqlx::migrate!("./tests/fixtures/regular");
static OFFERED: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    let mut migrations = REGULAR.iter().cloned().collect::<Vec<_>>();
    migrations.push(Migration::new(3, "rollback duplicate key".into(), MigrationType::Simple,
        "UPDATE text_records SET label='must roll back'; INSERT INTO text_records VALUES('019eef00-0000-7000-8000-000000000001',42,'duplicate')".into_sql_str(), false));
    migrations
});
static TABLES: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
    LazyLock::new(|| {
        ["text_records", "composite_records", "real_records"]
            .into_iter()
            .map(TableName::new)
            .collect()
    });
struct PendingSchema {
    offered_count: Arc<AtomicUsize>,
    tables: &'static [TableName],
}
impl ReplicatedSchema for PendingSchema {
    fn migrations(&self) -> &'static [Migration] {
        OFFERED
            .get(..self.offered_count.load(Ordering::SeqCst))
            .unwrap_or_default()
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}

#[tokio::test]
async fn later_migration_failure_rolls_back_and_release_replays_the_real_pending_answer()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let offered_count = Arc::new(AtomicUsize::new(2));
    let database = ReplicatedDatabase::create(
        &path,
        PendingSchema {
            offered_count: offered_count.clone(),
            tables: TABLES.as_ref().map_err(Clone::clone)?,
        },
        PeerId::new(Uuid::from_u128(0x019eef00000070008000000000000a01))?,
        LeasePolicy {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_secs(1),
        },
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    let before = database.snapshot().await?.position();
    offered_count.store(3, Ordering::SeqCst);
    assert!(matches!(
        database.migrate().await,
        Err(OpenError::Storage { .. })
    ));
    assert_eq!(database.snapshot().await?.position(), before);
    let row = query::<Turso>(
        "SELECT label, (SELECT count(*) FROM _sqlx_migrations) AS applied FROM text_records",
    )
    .fetch_one(database.reader())
    .await?;
    assert_eq!(row.try_get::<String, _>("label")?, "backfilled");
    assert_eq!(row.try_get::<i64, _>("applied")?, 2);
    let request = ReleaseRequest {
        database: database.database(),
        epoch: Epoch::new(1)?,
        transfer: TransferId::new(Uuid::from_u128(0x019eef00000070008000000000000a02))?,
    };
    let call = CallId {
        operation: OperationId::new(Uuid::from_u128(0x019eef00000070008000000000000a03))?,
        step: StepName::new("pending_release")?,
    };
    let expected = ReleaseAnswer::ReleaseRefused(ReleaseRefusal::MigrationPending {
        migration: 3,
        state: MigrationState::Pending,
    });
    assert_eq!(
        database.request_release(&request, call.clone()).await?,
        expected
    );
    assert!(matches!(
        database.writer_state().await?,
        WriterState::Provisional
    ));
    offered_count.store(2, Ordering::SeqCst);
    assert_eq!(database.request_release(&request, call).await?, expected);
    let fresh_call = CallId {
        operation: OperationId::new(Uuid::from_u128(0x019eef00000070008000000000000a04))?,
        step: StepName::new("ready_release")?,
    };
    assert_eq!(
        database.request_release(&request, fresh_call).await?,
        ReleaseAnswer::Released {
            final_position: before
        }
    );
    database.close().await;
    Ok(())
}
