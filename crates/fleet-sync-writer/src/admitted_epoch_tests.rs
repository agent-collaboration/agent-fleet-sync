//! Admission metadata stays usable while the real writer guard is held.
#![allow(clippy::panic_in_result_fn)]
use crate::*;
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::*;
use runtime_primitives::Clock;
use sqlx_core::{executor::Executor, migrate::Migrator, query::query, row::Row};
use sqlx_turso::Turso;
use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
    time::Duration,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static MIGRATIONS: Migrator = sqlx::migrate!("./tests/fixtures/regular");
static TABLES: LazyLock<Result<Vec<TableName>, TextValueError>> = LazyLock::new(|| {
    ["text_records", "composite_records", "real_records"]
        .into_iter()
        .map(TableName::new)
        .collect()
});
struct EpochSchema {
    tables: &'static [TableName],
}
impl ReplicatedSchema for EpochSchema {
    fn migrations(&self) -> &'static [Migration] {
        MIGRATIONS.iter().as_slice()
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}
struct CountingClock {
    now: AtomicI64,
    reads: AtomicUsize,
}
impl Clock for CountingClock {
    fn now(&self) -> SyncTime {
        self.reads.fetch_add(1, Ordering::SeqCst);
        SyncTime::from_unix_millis(self.now.load(Ordering::SeqCst))
    }
}
async fn open(
    path: &std::path::Path,
    epoch: Option<Epoch>,
    clock: Arc<CountingClock>,
) -> TestResult<ReplicatedDatabase<EpochSchema>> {
    let database = DatabaseId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000b01,
    ))?;
    let peer = PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000b02,
    ))?;
    let mut holder = LeaseHolder::provisional(
        database,
        peer,
        LeasePolicy {
            lease_duration: Duration::from_secs(1),
            renew_interval: Duration::from_millis(100),
        },
    );
    if let Some(epoch) = epoch {
        holder.observe_registration(
            LeaseStatus {
                database,
                holder: peer,
                epoch,
                state: LeaseState::Held,
                expires_at: SyncTime::from_unix_millis(1100),
                remaining_ms: 1000,
            },
            SyncTime::from_unix_millis(100),
        )?;
    }
    Ok(ReplicatedDatabase::open_with(
        path,
        database,
        EpochSchema {
            tables: TABLES.as_ref().map_err(Clone::clone)?,
        },
        holder,
        clock,
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?)
}
fn clock() -> Arc<CountingClock> {
    Arc::new(CountingClock {
        now: AtomicI64::new(100),
        reads: AtomicUsize::new(0),
    })
}

#[tokio::test]
async fn provisional_admission_exposes_epoch_one_without_clock_or_database_work() -> TestResult {
    let directory = tempfile::tempdir()?;
    let clock = clock();
    let database = open(&directory.path().join("writer.db"), None, clock.clone()).await?;
    let mut write = database.begin_write().await?;
    let reads = clock.reads.load(Ordering::SeqCst);
    for _ in 0..1000 {
        assert_eq!(write.admitted_epoch(), Epoch::new(1)?);
    }
    assert_eq!(clock.reads.load(Ordering::SeqCst), reads);
    write
        .execute("UPDATE text_records SET label='provisional epoch one'")
        .await?;
    write.commit().await?;
    assert_eq!(
        query::<Turso>("SELECT label FROM text_records")
            .fetch_one(database.reader())
            .await?
            .try_get::<String, _>("label")?,
        "provisional epoch one"
    );
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn admission_epoch_is_frozen_after_expiry_but_commit_and_later_admission_still_refuse()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let clock = clock();
    let database = open(
        &directory.path().join("writer.db"),
        Some(Epoch::new(7)?),
        clock.clone(),
    )
    .await?;
    let mut write = database.begin_write().await?;
    assert_eq!(write.admitted_epoch(), Epoch::new(7)?);
    write
        .execute("UPDATE text_records SET label='committed at seven'")
        .await?;
    write.commit().await?;
    let before = database.snapshot().await?.position();
    let mut write = database.begin_write().await?;
    write
        .execute("UPDATE text_records SET label='must roll back'")
        .await?;
    clock.now.store(1100, Ordering::SeqCst);
    let reads = clock.reads.load(Ordering::SeqCst);
    for _ in 0..1000 {
        assert_eq!(write.admitted_epoch(), Epoch::new(7)?);
    }
    assert_eq!(clock.reads.load(Ordering::SeqCst), reads);
    assert_eq!(
        write.commit().await,
        Err(WriteRefusal::LeaseExpired {
            epoch: Epoch::new(7)?,
            expired_at: SyncTime::from_unix_millis(1100)
        })
    );
    assert_eq!(database.snapshot().await?.position(), before);
    assert_eq!(
        query::<Turso>("SELECT label FROM text_records")
            .fetch_one(database.reader())
            .await?
            .try_get::<String, _>("label")?,
        "committed at seven"
    );
    assert!(matches!(
        database.begin_write().await,
        Err(WriteRefusal::LeaseExpired { .. })
    ));
    database
        .observe_renewal(
            RenewAnswer::Refused(LeaseRefusal::StaleEpoch {
                current_epoch: Epoch::new(8)?,
                epoch_ended_at: before,
            }),
            SyncTime::from_unix_millis(1100),
        )
        .await?;
    assert!(
        matches!(database.begin_write().await, Err(WriteRefusal::Superseded { current }) if current == Epoch::new(8)?)
    );
    database.close().await;
    Ok(())
}
