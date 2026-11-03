#![allow(clippy::panic_in_result_fn)]
use super::*;
use crate::{
    WriteRefusal,
    writer_journal_tests::LocalHost,
    writer_release_tests::{TestClock, create},
};
use durable_steps::{AtomicStore, StoreError};
use fleet_sync_lease::{HolderError, WriterState};
use fleet_sync_protocol::{GateReasonCode, LeaseState, LeaseStatus, RenewResult};
use runtime_primitives::Clock;
use std::sync::{Arc, atomic::AtomicI64};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub(super) fn clock() -> Arc<TestClock> {
    Arc::new(TestClock(AtomicI64::new(100)))
}
pub(super) fn renewal(database: DatabaseId, holder: PeerId, epoch: u64) -> TestResult<RenewAnswer> {
    let epoch = fleet_sync_protocol::Epoch::new(epoch)?;
    Ok(RenewAnswer::Renewed {
        recorded: RenewResult {
            epoch,
            expires_at: SyncTime::from_unix_millis(10_100),
        },
        current: LeaseStatus {
            database,
            holder,
            epoch,
            state: LeaseState::Held,
            expires_at: SyncTime::from_unix_millis(10_100),
            remaining_ms: 10_000,
        },
        transfer_in_progress: None,
        replayed: false,
    })
}

#[tokio::test]
async fn direct_epochless_refusals_replace_a_renewal_on_disk_and_after_reopen() -> TestResult {
    epochless_refusals(false).await
}
#[tokio::test]
async fn task_epochless_refusals_match_memory_disk_and_reopened_write_gate() -> TestResult {
    epochless_refusals(true).await
}
async fn epochless_refusals(task_path: bool) -> TestResult {
    for refusal in [
        LeaseRefusal::NotRegistered,
        LeaseRefusal::NoAuthoritativeCopy,
        LeaseRefusal::EpochUnprovable,
        LeaseRefusal::NotWriter { current: None },
    ] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("writer.db");
        let clock = clock();
        let database = create(&path, clock.clone()).await?;
        let peer = database.control.lock().await.holder.peer();
        database
            .observe_renewal(renewal(database.database(), peer, 1)?, clock.now())
            .await?;
        assert_eq!(
            database.writer_state().await?,
            WriterState::Writing {
                epoch: Epoch::new(1)?
            }
        );
        if task_path {
            let journal = database.journal_store(Arc::new(LocalHost)).await?;
            let answer = RenewAnswer::Refused(refusal.clone());
            let expected = refusal.clone();
            let started = clock.now();
            journal.transaction(move |scope|Box::pin(async move {
                scope.observe_task_renewal(answer,started).await.map_err(StoreError::backend)?;
                assert!(matches!(scope.holder.writer_state(started),Err(HolderError::Refused(reason)) if reason==expected));
                Ok(())
            })).await?;
        } else {
            database
                .observe_renewal(RenewAnswer::Refused(refusal.clone()), clock.now())
                .await?;
        }
        let record = {
            let mut connection = database.maintenance.acquire().await?;
            read_file_record(&mut connection)
                .await?
                .ok_or("writer record missing")?
        };
        assert!(
            matches!(record.authority,Some(AuthorityObservation::Renewal {answer:RenewAnswer::Refused(reason),..}) if reason==refusal),
            "the committed observation must be the current refusal: {refusal:?}"
        );
        assert!(
            matches!(database.writer_state().await,Err(OpenError::Holder {error:HolderError::Refused(reason)}) if reason==refusal)
        );
        assert!(matches!(
            database.begin_write().await,
            Err(WriteRefusal::NotWriter)
        ));
        database.close().await;
        drop(database);
        let reopened = create(&path, clock).await?;
        assert!(
            matches!(reopened.writer_state().await,Err(OpenError::Holder {error:HolderError::Refused(reason)}) if reason==refusal)
        );
        assert!(matches!(
            reopened.begin_write().await,
            Err(WriteRefusal::NotWriter)
        ));
        reopened.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn lower_epoch_refusal_cannot_replace_a_newer_persisted_observation() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let clock = clock();
    let database = create(&path, clock.clone()).await?;
    let peer = database.control.lock().await.holder.peer();
    database
        .observe_renewal(renewal(database.database(), peer, 1)?, clock.now())
        .await?;
    database
        .observe_renewal(renewal(database.database(), peer, 2)?, clock.now())
        .await?;
    database
        .observe_renewal(
            RenewAnswer::Refused(LeaseRefusal::NotWriter {
                current: Some(Epoch::new(1)?),
            }),
            clock.now(),
        )
        .await?;
    database.close().await;
    drop(database);
    let reopened = create(&path, clock).await?;
    assert_eq!(
        reopened.writer_state().await?,
        WriterState::Superseded {
            current: Epoch::new(2)?
        }
    );
    assert!(
        matches!(reopened.begin_write().await,Err(WriteRefusal::Superseded {current}) if current==Epoch::new(2)?)
    );
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn epochless_refusals_do_not_replace_existing_terminal_observations() -> TestResult {
    for retired in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("writer.db");
        let clock = clock();
        let database = create(&path, clock.clone()).await?;
        let reason = GateReason {
            code: GateReasonCode::new("authority_denied")?,
            detail: "registration refused".into(),
        };
        if retired {
            let peer = database.control.lock().await.holder.peer();
            database
                .observe_renewal(renewal(database.database(), peer, 1)?, clock.now())
                .await?;
            database
                .observe_renewal(RenewAnswer::Refused(LeaseRefusal::Retired), clock.now())
                .await?;
        } else {
            database.reject_registration(reason.clone()).await?;
        }
        database
            .observe_renewal(
                RenewAnswer::Refused(LeaseRefusal::NotRegistered),
                clock.now(),
            )
            .await?;
        database.close().await;
        drop(database);
        let reopened = create(&path, clock).await?;
        if retired {
            assert!(matches!(
                reopened.writer_state().await,
                Err(OpenError::Holder {
                    error: HolderError::Refused(LeaseRefusal::Retired)
                })
            ));
        } else {
            assert!(
                matches!(reopened.writer_state().await,Err(OpenError::Holder {error:HolderError::RegistrationRejected {reason:stored}}) if stored==reason)
            );
        }
        reopened.close().await;
    }
    Ok(())
}
