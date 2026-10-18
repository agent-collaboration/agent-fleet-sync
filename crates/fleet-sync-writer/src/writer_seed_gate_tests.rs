#![allow(clippy::panic_in_result_fn)]
use crate::*;
use fleet_sync_lease::{LeaseHolder, LeasePolicy, WriterState};
use fleet_sync_protocol::*;
use sqlx_core::{executor::Executor, migrate::Migrator};
use std::{sync::LazyLock, time::Duration};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static MIGRATIONS: Migrator = sqlx::migrate!("./tests/fixtures/regular");
static TABLES: LazyLock<Result<Vec<TableName>, TextValueError>> = LazyLock::new(|| {
    ["text_records", "composite_records", "real_records"]
        .into_iter()
        .map(TableName::new)
        .collect()
});
struct Schema {
    tables: &'static [TableName],
}
fn schema() -> TestResult<Schema> {
    Ok(Schema {
        tables: TABLES.as_ref().map_err(Clone::clone)?,
    })
}
impl ReplicatedSchema for Schema {
    fn migrations(&self) -> &'static [Migration] {
        MIGRATIONS.iter().as_slice()
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(60),
        renew_interval: Duration::from_secs(10),
    }
}

struct SeedAdmission;
impl durable_steps::Task for SeedAdmission {
    const KIND: &'static str = "test.seed-admission";
    const VERSION: u32 = 1;
    type Store = WriterJournalStore<Schema, crate::writer_journal_tests::LocalHost>;
    type Input = ReplicationPosition;
    type Output = ();
    async fn run<C: durable_steps::OperationContext<Store = Self::Store>>(
        &self,
        _: &mut C,
        _: &ReplicationPosition,
    ) -> Result<(), durable_steps::OperationFailure> {
        Ok(())
    }
}

#[tokio::test]
async fn seed_freeze_and_work_admission_share_the_same_commit_and_rollback() -> TestResult {
    use durable_steps::{AtomicStore, JournalScope, RequestKey, StoreError};
    use sqlx_core::{query::query, row::Row};
    let root = tempfile::tempdir()?;
    let peer = PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000002,
    ))?;
    let writer = std::sync::Arc::new(
        ReplicatedDatabase::create(
            &root.path().join("writer.db"),
            schema()?,
            peer,
            policy(),
            WriterOptions {
                allow_provisional_writes: true,
            },
        )
        .await?,
    );
    let store = writer
        .journal_store(std::sync::Arc::new(crate::writer_journal_tests::LocalHost))
        .await?;
    let failed = store
        .transaction(|scope| {
            Box::pin(async move {
                let position = scope.begin_seeding().await.map_err(StoreError::backend)?;
                scope
                    .admit::<SeedAdmission>(
                        &RequestKey::new("rolled-back-seed").map_err(StoreError::backend)?,
                        &position,
                    )
                    .await
                    .map_err(StoreError::backend)?;
                Err::<(), _>(StoreError::Aborted {
                    reason: "roll back freeze and admission".into(),
                })
            })
        })
        .await;
    assert!(matches!(failed, Err(StoreError::Aborted { .. })));
    assert_eq!(writer.writer_state().await?, WriterState::Provisional);
    assert_eq!(
        query::<sqlx_turso::Turso>("SELECT COUNT(*) AS count FROM _fleet_sync_operations")
            .fetch_one(writer.reader())
            .await?
            .try_get::<i64, _>("count")?,
        0
    );
    let position = store
        .transaction(|scope| {
            Box::pin(async move {
                let position = scope.begin_seeding().await.map_err(StoreError::backend)?;
                scope
                    .admit::<SeedAdmission>(
                        &RequestKey::new("committed-seed").map_err(StoreError::backend)?,
                        &position,
                    )
                    .await
                    .map_err(StoreError::backend)?;
                Ok(position)
            })
        })
        .await?;
    assert_eq!(
        writer.writer_state().await?,
        WriterState::Seeding {
            epoch: Epoch::new(1)?
        }
    );
    assert_eq!(writer.snapshot().await?.position(), position);
    assert_eq!(
        query::<sqlx_turso::Turso>("SELECT COUNT(*) AS count FROM _fleet_sync_operations")
            .fetch_one(writer.reader())
            .await?
            .try_get::<i64, _>("count")?,
        1
    );
    drop(store);
    writer.close().await;
    Ok(())
}
#[tokio::test]
async fn beginning_registration_seed_freezes_provisional_capture_and_reopens_at_the_same_position()
-> TestResult {
    let root = tempfile::tempdir()?;
    let path = root.path().join("writer.db");
    let peer = PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000002,
    ))?;
    let writer = ReplicatedDatabase::create(
        &path,
        schema()?,
        peer,
        policy(),
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    let database = writer.database();
    let mut write = writer.begin_write().await?;
    (&mut write)
        .execute("UPDATE text_records SET label='provisional before seed'")
        .await?;
    write.commit().await?;
    let position = writer.begin_seeding().await?;
    assert!(matches!(
        writer.begin_write().await,
        Err(WriteRefusal::Seeding)
    ));
    assert_eq!(writer.snapshot().await?.position(), position);
    writer.close().await;
    drop(writer);
    let reopened = ReplicatedDatabase::open_with(
        &path,
        database,
        schema()?,
        LeaseHolder::provisional(database, peer, policy()),
        std::sync::Arc::new(runtime_primitives::SystemClock),
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    assert_eq!(
        reopened.writer_state().await?,
        WriterState::Seeding {
            epoch: Epoch::new(1)?
        }
    );
    assert_eq!(reopened.snapshot().await?.position(), position);
    assert_eq!(reopened.begin_seeding().await?, position);
    assert!(matches!(
        reopened.begin_write().await,
        Err(WriteRefusal::Seeding)
    ));
    let reason = GateReason {
        code: GateReasonCode::new("denied")?,
        detail: "registration refused while seed is pending".into(),
    };
    reopened.reject_registration(reason.clone()).await?;
    assert!(
        matches!(reopened.begin_write().await, Err(WriteRefusal::Rejected { reason: actual }) if actual == reason)
    );
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn seed_grant_survives_reopen_and_late_gate_refusal_cannot_replace_it() -> TestResult {
    use runtime_primitives::Clock;
    let root = tempfile::tempdir()?;
    let path = root.path().join("writer.db");
    let peer = PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000002,
    ))?;
    let writer = ReplicatedDatabase::create(
        &path,
        schema()?,
        peer,
        policy(),
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    let database = writer.database();
    let position = writer.begin_seeding().await?;
    let started = runtime_primitives::SystemClock.now();
    let mut offered = LeaseStatus {
        database,
        epoch: Epoch::new(2)?,
        holder: peer,
        state: LeaseState::Held,
        expires_at: started.after(Duration::from_secs(60)),
        remaining_ms: 60_000,
    };
    assert!(matches!(
        writer.observe_registration(offered.clone(), started).await,
        Err(OpenError::Holder {
            error: fleet_sync_lease::HolderError::LeaseMismatch
        })
    ));
    assert_eq!(writer.snapshot().await?.position(), position);
    offered.epoch = Epoch::new(1)?;
    // This grant fixture tests durable gate restoration. Registry and seed
    // integration needs its own process journey.
    writer.observe_registration(offered, started).await?;
    writer.close().await;
    drop(writer);
    let reopened = ReplicatedDatabase::open_with(
        &path,
        database,
        schema()?,
        LeaseHolder::provisional(database, peer, policy()),
        std::sync::Arc::new(runtime_primitives::SystemClock),
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    let reason = GateReason {
        code: GateReasonCode::new("late")?,
        detail: "late gate refusal".into(),
    };
    assert!(matches!(
        reopened.reject_registration(reason).await,
        Err(OpenError::Holder {
            error: fleet_sync_lease::HolderError::AlreadyGranted
        })
    ));
    assert_eq!(reopened.snapshot().await?.position(), position);
    assert!(matches!(
        reopened.begin_write().await,
        Err(WriteRefusal::Seeding)
    ));
    reopened.complete_seed().await?;
    assert_eq!(
        reopened.writer_state().await?,
        WriterState::Writing {
            epoch: Epoch::new(1)?
        }
    );
    assert!(reopened.begin_seeding().await.is_err());
    reopened.close().await;
    Ok(())
}
