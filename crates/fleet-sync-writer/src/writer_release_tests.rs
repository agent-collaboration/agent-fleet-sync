#![allow(clippy::panic_in_result_fn)]
use crate::{
    writer_release_record::{ReleaseState, WriterReleaseRecord, save_release},
    *,
};
use durable_steps::{CallId, OperationId, StepName, Uuid};
use fleet_sync_lease::{LeaseHolder, LeasePolicy, WriterState};
use fleet_sync_protocol::{
    Epoch, LeaseRefusal, LeaseState, LeaseStatus, PeerId, ReleaseAnswer, ReleaseRequest,
    RenewAnswer, RenewResult, SyncTime, TableName, TransferId,
};
use runtime_primitives::Clock;
use sqlx_core::{
    connection::Connection, executor::Executor, migrate::Migrator, query::query, row::Row,
};
use sqlx_turso::Turso;
use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static MIGRATIONS: Migrator = sqlx::migrate!("./tests/fixtures/regular");
static TABLES: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
    LazyLock::new(|| {
        ["text_records", "composite_records", "real_records"]
            .into_iter()
            .map(TableName::new)
            .collect()
    });
pub(super) struct ReleaseSchema {
    tables: &'static [TableName],
}
impl ReplicatedSchema for ReleaseSchema {
    fn migrations(&self) -> &'static [Migration] {
        MIGRATIONS.iter().as_slice()
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}
pub(super) struct TestClock(pub(super) AtomicI64);
impl Clock for TestClock {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(self.0.load(Ordering::SeqCst))
    }
}
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(10),
        renew_interval: Duration::from_secs(1),
    }
}
fn peer() -> TestResult<PeerId> {
    Ok(PeerId::new(Uuid::from_u128(2))?)
}
fn call() -> TestResult<CallId> {
    Ok(CallId {
        operation: OperationId(Uuid::from_u128(3)),
        step: StepName::new("request_release")?,
    })
}
fn request(database: fleet_sync_protocol::DatabaseId) -> TestResult<ReleaseRequest> {
    Ok(ReleaseRequest {
        database,
        epoch: Epoch::new(1)?,
        transfer: TransferId::new(Uuid::from_u128(4))?,
    })
}
fn renewal(
    database: fleet_sync_protocol::DatabaseId,
    epoch: u64,
    remaining_ms: u64,
) -> TestResult<RenewAnswer> {
    Ok(RenewAnswer::Renewed {
        recorded: RenewResult {
            epoch: Epoch::new(epoch)?,
            expires_at: SyncTime::from_unix_millis(i64::MAX),
        },
        current: LeaseStatus {
            database,
            epoch: Epoch::new(epoch)?,
            holder: peer()?,
            state: LeaseState::Held,
            expires_at: SyncTime::from_unix_millis(i64::MAX),
            remaining_ms,
        },
        transfer_in_progress: None,
        replayed: true,
    })
}
pub(super) async fn create(
    path: &std::path::Path,
    clock: Arc<TestClock>,
) -> TestResult<Arc<ReplicatedDatabase<ReleaseSchema>>> {
    let id = fleet_sync_protocol::DatabaseId::new(Uuid::from_u128(1))?;
    let database = Arc::new(
        ReplicatedDatabase::open_with(
            path,
            id,
            ReleaseSchema {
                tables: TABLES.as_ref().map_err(Clone::clone)?,
            },
            LeaseHolder::provisional(id, peer()?, policy()),
            clock,
            WriterOptions {
                allow_provisional_writes: true,
            },
        )
        .await?,
    );
    Ok(database)
}

#[tokio::test]
async fn release_drains_accepted_transaction_and_replays_after_reopen_without_reopening_gate()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let clock = Arc::new(TestClock(AtomicI64::new(100)));
    let database = create(&path, clock.clone()).await?;
    database
        .observe_renewal(renewal(database.database(), 1, 1000)?, clock.now())
        .await?;
    let mut write = database.begin_write().await?;
    (&mut write)
        .execute("INSERT INTO text_records VALUES('019eef00-0000-7000-8000-00000000000a',42,'before release')")
        .await?;
    let release_request = request(database.database())?;
    let release_call = call()?;
    let task = {
        let database = database.clone();
        let request = release_request.clone();
        let call = release_call.clone();
        tokio::spawn(async move { database.request_release(&request, call).await })
    };
    write.commit().await?;
    let answer = task.await??;
    let ReleaseAnswer::Released { final_position } = answer else {
        return Err("release refused".into());
    };
    assert_eq!(database.snapshot().await?.position(), final_position);
    assert_eq!(
        query::<Turso>(
            "SELECT amount FROM text_records WHERE id='019eef00-0000-7000-8000-00000000000a'"
        )
        .fetch_one(database.reader())
        .await?
        .try_get::<i64, _>("amount")?,
        42
    );
    assert!(matches!(
        database.begin_write().await,
        Err(WriteRefusal::Released)
    ));
    assert_eq!(
        database
            .request_release(&release_request, release_call.clone())
            .await?,
        answer
    );
    let mut changed = release_request.clone();
    changed.transfer = TransferId::new(Uuid::from_u128(5))?;
    assert_eq!(
        database
            .request_release(&changed, release_call.clone())
            .await?,
        ReleaseAnswer::RequestConflict
    );
    database.close().await;
    let reopened = create(&path, clock).await?;
    assert!(
        matches!(reopened.writer_state().await?,WriterState::Released {final_position:position,..} if position==final_position)
    );
    assert!(matches!(
        reopened.begin_write().await,
        Err(WriteRefusal::Released)
    ));
    assert_eq!(
        reopened
            .request_release(&release_request, release_call)
            .await?,
        answer
    );
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn committed_drain_blocks_admission_before_cache_update_and_finishes_after_reopen()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let clock = Arc::new(TestClock(AtomicI64::new(100)));
    let database = create(&path, clock.clone()).await?;
    let request = request(database.database())?;
    let call = call()?;
    // Save only the durable half, leaving the in-memory holder provisional.
    // This is the cancellation boundary between SQL commit and cache update.
    {
        let mut connection = database.maintenance.acquire().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        save_release(
            &mut transaction,
            &WriterReleaseRecord {
                request: request.clone(),
                call: call.clone(),
                state: ReleaseState::Draining,
            },
        )
        .await?;
        transaction.commit().await?;
    }
    assert!(
        matches!(database.begin_write().await,Err(WriteRefusal::Draining {transfer}) if transfer==request.transfer)
    );
    database.close().await;
    let reopened = create(&path, clock).await?;
    assert!(matches!(
        reopened.begin_write().await,
        Err(WriteRefusal::Draining { .. })
    ));
    assert!(matches!(
        reopened.request_release(&request, call).await?,
        ReleaseAnswer::Released { .. }
    ));
    assert!(matches!(
        reopened.begin_write().await,
        Err(WriteRefusal::Released)
    ));
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn renewal_restore_keeps_original_deadline_and_does_not_erase_supersession() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let clock = Arc::new(TestClock(AtomicI64::new(100)));
    let database = create(&path, clock.clone()).await?;
    database
        .observe_renewal(renewal(database.database(), 1, 100)?, clock.now())
        .await?;
    database.close().await;
    clock.0.store(200, Ordering::SeqCst);
    let reopened = create(&path, clock.clone()).await?;
    assert!(
        matches!(reopened.begin_write().await,Err(WriteRefusal::LeaseExpired {expired_at,..}) if expired_at==SyncTime::from_unix_millis(200))
    );
    let ended_at = reopened.snapshot().await?.position();
    reopened
        .observe_renewal(
            RenewAnswer::Refused(LeaseRefusal::StaleEpoch {
                current_epoch: Epoch::new(2)?,
                epoch_ended_at: ended_at,
            }),
            clock.now(),
        )
        .await?;
    reopened
        .observe_renewal(renewal(reopened.database(), 1, 10_000)?, clock.now())
        .await?;
    reopened.close().await;
    let reopened = create(&path, clock).await?;
    assert!(
        matches!(reopened.begin_write().await,Err(WriteRefusal::Superseded {current}) if current==Epoch::new(2)?)
    );
    reopened.close().await;
    Ok(())
}

fn withdrawal(request: &ReleaseRequest) -> fleet_sync_protocol::WithdrawRelease {
    fleet_sync_protocol::WithdrawRelease {
        database: request.database,
        epoch: request.epoch,
        transfer: request.transfer,
    }
}
fn named_call(name: &str) -> TestResult<CallId> {
    Ok(CallId {
        step: StepName::new(name)?,
        ..call()?
    })
}

#[tokio::test]
async fn withdrawal_survives_reopen_refuses_late_release_and_keeps_original_deadline() -> TestResult
{
    use fleet_sync_protocol::{ReleaseRefusal, WithdrawAnswer};
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let clock = Arc::new(TestClock(AtomicI64::new(100)));
    let database = create(&path, clock.clone()).await?;
    database
        .observe_renewal(renewal(database.database(), 1, 1000)?, clock.now())
        .await?;
    let request = request(database.database())?;
    let release = database.request_release(&request, call()?).await?;
    let ReleaseAnswer::Released { final_position } = release else {
        return Err("release refused".into());
    };
    // A real renewal while released stays authoritative, without reopening admission.
    database
        .observe_renewal(
            renewal(database.database(), 1, 500)?,
            SyncTime::from_unix_millis(200),
        )
        .await?;
    assert!(matches!(
        database.begin_write().await,
        Err(WriteRefusal::Released)
    ));
    let withdraw_call = named_call("withdraw")?;
    assert_eq!(
        database
            .withdraw_release(&withdrawal(&request), withdraw_call.clone())
            .await?,
        WithdrawAnswer::Withdrawn
    );
    assert_eq!(database.snapshot().await?.position(), final_position);
    assert_eq!(
        database.request_release(&request, call()?).await?,
        ReleaseAnswer::ReleaseRefused(ReleaseRefusal::Withdrawn)
    );
    database.close().await;
    clock.0.store(699, Ordering::SeqCst);
    let database = create(&path, clock.clone()).await?;
    assert_eq!(
        database
            .withdraw_release(&withdrawal(&request), withdraw_call)
            .await?,
        WithdrawAnswer::Withdrawn
    );
    assert_eq!(
        database
            .request_release(&request, named_call("late_release")?)
            .await?,
        ReleaseAnswer::ReleaseRefused(ReleaseRefusal::Withdrawn)
    );
    let mut write = database.begin_write().await?;
    (&mut write).execute("INSERT INTO text_records VALUES('019eef00-0000-7000-8000-00000000000a',42,'after withdrawal')").await?;
    write.commit().await?;
    assert!(database.snapshot().await?.position() > final_position);
    clock.0.store(700, Ordering::SeqCst);
    assert!(
        matches!(database.begin_write().await, Err(WriteRefusal::LeaseExpired { expired_at, .. }) if expired_at == SyncTime::from_unix_millis(700))
    );
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn withdrawal_before_release_is_a_tombstone_and_does_not_erase_another_transfer() -> TestResult
{
    use fleet_sync_protocol::{ReleaseRefusal, WithdrawAnswer};
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let clock = Arc::new(TestClock(AtomicI64::new(100)));
    let database = create(&path, clock.clone()).await?;
    database
        .observe_renewal(renewal(database.database(), 1, 1000)?, clock.now())
        .await?;
    let first = request(database.database())?;
    assert_eq!(
        database
            .withdraw_release(&withdrawal(&first), named_call("withdraw_first")?)
            .await?,
        WithdrawAnswer::Withdrawn
    );
    let mut second = first.clone();
    second.transfer = TransferId::new(Uuid::from_u128(5))?;
    assert!(
        database
            .withdraw_release(&withdrawal(&second), named_call("withdraw_first")?)
            .await
            .is_err()
    );
    assert_eq!(
        database
            .request_release(&first, named_call("withdraw_first")?)
            .await?,
        ReleaseAnswer::RequestConflict
    );
    database
        .request_release(&second, named_call("release_second")?)
        .await?;
    database
        .withdraw_release(&withdrawal(&first), named_call("withdraw_first_again")?)
        .await?;
    assert!(matches!(
        database.begin_write().await,
        Err(WriteRefusal::Released)
    ));
    database.close().await;
    let database = create(&path, clock).await?;
    assert_eq!(
        database.request_release(&first, call()?).await?,
        ReleaseAnswer::ReleaseRefused(ReleaseRefusal::Withdrawn)
    );
    assert!(matches!(
        database.begin_write().await,
        Err(WriteRefusal::Released)
    ));
    assert_eq!(
        database
            .withdraw_release(&withdrawal(&second), named_call("withdraw_second")?)
            .await?,
        WithdrawAnswer::Withdrawn
    );
    assert!(matches!(
        database.writer_state().await?,
        WriterState::Writing { .. }
    ));
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn committed_withdrawal_tombstone_reopens_the_gate_before_the_cache_has_changed() -> TestResult
{
    use fleet_sync_protocol::WithdrawAnswer;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let clock = Arc::new(TestClock(AtomicI64::new(100)));
    let database = create(&path, clock.clone()).await?;
    database
        .observe_renewal(renewal(database.database(), 1, 1000)?, clock.now())
        .await?;
    let request = request(database.database())?;
    database.request_release(&request, call()?).await?;
    // A committed tombstone is sufficient even if the cache update was cancelled.
    {
        let mut connection = database.maintenance.acquire().await?;
        save_release(
            &mut connection,
            &WriterReleaseRecord {
                request,
                call: named_call("withdraw")?,
                state: ReleaseState::WithdrawalAnswered {
                    answer: WithdrawAnswer::Withdrawn,
                },
            },
        )
        .await?;
    }
    assert!(matches!(
        database.writer_state().await?,
        WriterState::Writing { .. }
    ));
    database.close().await;
    let database = create(&path, clock).await?;
    assert!(matches!(
        database.writer_state().await?,
        WriterState::Writing { .. }
    ));
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn withdrawal_after_committed_drain_never_resurrects_superseded_or_retired_authority()
-> TestResult {
    use fleet_sync_protocol::WithdrawAnswer;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let clock = Arc::new(TestClock(AtomicI64::new(100)));
    let database = create(&path, clock.clone()).await?;
    database
        .observe_renewal(renewal(database.database(), 1, 1000)?, clock.now())
        .await?;
    let request = request(database.database())?;
    // Commit only Draining, as a crash could leave it before the cache changes.
    {
        let mut connection = database.maintenance.acquire().await?;
        save_release(
            &mut connection,
            &WriterReleaseRecord {
                request: request.clone(),
                call: call()?,
                state: ReleaseState::Draining,
            },
        )
        .await?;
    }
    assert_eq!(
        database
            .withdraw_release(&withdrawal(&request), named_call("withdraw")?)
            .await?,
        WithdrawAnswer::Withdrawn
    );
    let ended_at = database.snapshot().await?.position();
    database
        .observe_renewal(
            RenewAnswer::Refused(LeaseRefusal::StaleEpoch {
                current_epoch: Epoch::new(2)?,
                epoch_ended_at: ended_at,
            }),
            clock.now(),
        )
        .await?;
    assert_eq!(
        database
            .withdraw_release(&withdrawal(&request), named_call("after_supersession")?)
            .await?,
        WithdrawAnswer::NotHolder
    );
    // Repeating the original answer must leave the newer authority intact.
    assert_eq!(
        database
            .withdraw_release(&withdrawal(&request), named_call("withdraw")?)
            .await?,
        WithdrawAnswer::Withdrawn
    );
    assert!(matches!(
        database.begin_write().await,
        Err(WriteRefusal::Superseded { .. })
    ));
    database.close().await;
    let database = create(&path, clock.clone()).await?;
    assert!(matches!(
        database.begin_write().await,
        Err(WriteRefusal::Superseded { .. })
    ));
    database.close().await;
    let retired_path = directory.path().join("retired.db");
    let database = create(&retired_path, clock.clone()).await?;
    database
        .observe_renewal(renewal(database.database(), 1, 1000)?, clock.now())
        .await?;
    database.request_release(&request, call()?).await?;
    database
        .observe_renewal(RenewAnswer::Refused(LeaseRefusal::Retired), clock.now())
        .await?;
    assert_eq!(
        database
            .withdraw_release(&withdrawal(&request), named_call("retired")?)
            .await?,
        WithdrawAnswer::NotHolder
    );
    database.close().await;
    let database = create(&retired_path, clock).await?;
    assert!(matches!(
        database.writer_state().await,
        Err(OpenError::Holder {
            error: fleet_sync_lease::HolderError::Refused(LeaseRefusal::Retired)
        })
    ));
    database.close().await;
    Ok(())
}
