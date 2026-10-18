//! Force both orders of an already-dispatched Move fence and a superseding Takeover fence.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_change_tests::register_first, turso_registry_copy::FenceGate, turso_registry_fixture::*,
    *,
};
use durable_steps::{AtomicStore, OperationState, RequestKey, Uuid};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::sync::atomic::Ordering;
use std::{sync::Arc, time::Duration};

async fn saved_answer(
    engine: &TestEngine,
    id: durable_steps::OperationId,
) -> TestResult<HolderChangeAnswer> {
    let OperationState::Completed { result } = engine.status(&id).await?.state else {
        return Err("holder change did not finish".into());
    };
    Ok(serde_json::from_slice(&result)?)
}
fn transfer(value: u128) -> TestResult<TransferId> {
    Ok(TransferId::new(Uuid::from_u128(
        (value) | 0x019eef00000070008000000000000000,
    ))?)
}

#[tokio::test]
async fn superseded_move_finishes_after_reopen_without_resending_its_unanswered_call() -> TestResult
{
    for waiting_for_release in [true, false] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("registry.db");
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(&path, Arc::clone(&host), true).await?;
        let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
        register_first(&engine).await?;
        let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
            &engine,
            &RequestKey::new("pending-call-preview")?,
            PreviewTakeover {
                database: database()?,
            },
        )
        .await?;
        let move_id = transfer(801)?;
        let takeover_id = transfer(802)?;
        let request = HolderChangeRequest::Move {
            attempt: move_id,
            database: database()?,
            from_epoch: Epoch::new(1)?,
            to: peer(2)?,
        };
        let admitted = store
            .transaction(move |scope| {
                Box::pin(async move {
                    admit_holder_change::<TestStore, Turso>(
                        scope,
                        &request,
                        &ActorRef::new("race-admin").map_err(storage)?,
                        SyncTime::from_unix_millis(1000),
                    )
                    .await
                    .map_err(storage)
                })
            })
            .await?;
        host.hold_release
            .store(waiting_for_release, Ordering::SeqCst);
        host.hold_acked
            .store(!waiting_for_release, Ordering::SeqCst);
        let observed = if waiting_for_release {
            &host.release_observed
        } else {
            &host.acked_observed
        };
        let mut drive = Box::pin(engine.drive_due());
        tokio::select! {
            result = &mut drive => return Err(format!("pending call returned unexpectedly: {result:?}").into()),
            result = tokio::time::timeout(Duration::from_secs(5), observed.notified()) => result?,
        }
        drop(drive);
        let answer = task_answer::<HolderChangeTask<TestStore, Turso>>(
            &engine,
            &holder_change_request_key(takeover_id)?,
            HolderChangeRequest::Takeover {
                attempt: takeover_id,
                database: database()?,
                preview,
                to: peer(3)?,
            },
        )
        .await?;
        assert!(
            matches!(answer, HolderChangeAnswer::Granted { epoch, .. } if epoch == Epoch::new(3)?)
        );
        let kind = if waiting_for_release {
            "fleet-sync.holder-release"
        } else {
            "fleet-sync.copy-acknowledged"
        };
        let calls_before = host
            .calls
            .lock()
            .await
            .iter()
            .filter(|(actual, _)| actual == kind)
            .count();
        assert_eq!(calls_before, 1);
        drop(engine);
        drop(store);
        host.advance();
        host.hold_release.store(false, Ordering::SeqCst);
        host.release_unreachable.store(true, Ordering::SeqCst);
        host.hold_acked.store(false, Ordering::SeqCst);
        let store = open_store(&path, Arc::clone(&host), false).await?;
        let resumed = crate::turso_registry_fixture::engine(Arc::clone(&store), Arc::clone(&host))?;
        let report = tokio::time::timeout(Duration::from_secs(5), resumed.drive_due()).await??;
        assert_eq!(report.completed, vec![admitted.id()]);
        assert_eq!(
            saved_answer(&resumed, admitted.id()).await?,
            HolderChangeAnswer::Refused(HolderChangeRefusal::Superseded { by: takeover_id })
        );
        assert_eq!(
            host.calls
                .lock()
                .await
                .iter()
                .filter(|(actual, _)| actual == kind)
                .count(),
            calls_before
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_history"
            )
            .await?,
            "2"
        );
    }
    Ok(())
}

#[tokio::test]
async fn dispatched_old_fence_and_takeover_interleave_without_two_grants_or_epoch_reuse()
-> TestResult {
    for old_fence_first in [true, false] {
        let directory = tempfile::tempdir()?;
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(
            &directory.path().join("registry.db"),
            Arc::clone(&host),
            true,
        )
        .await?;
        let engine = Arc::new(engine(Arc::clone(&store), Arc::clone(&host))?);
        register_first(&engine).await?;
        let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
            &engine,
            &RequestKey::new("preview-before-race")?,
            PreviewTakeover {
                database: database()?,
            },
        )
        .await?;
        let move_id = transfer(701)?;
        let takeover_id = transfer(702)?;
        let old_gate = FenceGate::new();
        let new_gate = FenceGate::new();
        host.fence_gates
            .lock()
            .await
            .insert(move_id, Arc::clone(&old_gate));
        host.fence_gates
            .lock()
            .await
            .insert(takeover_id, Arc::clone(&new_gate));
        let old_request = HolderChangeRequest::Move {
            attempt: move_id,
            database: database()?,
            from_epoch: Epoch::new(1)?,
            to: peer(2)?,
        };
        let old_key = holder_change_request_key(move_id)?;
        let new_request = HolderChangeRequest::Takeover {
            attempt: takeover_id,
            database: database()?,
            preview,
            to: peer(3)?,
        };
        let new_key = holder_change_request_key(takeover_id)?;
        let old_finished = tokio::sync::Notify::new();
        let new_finished = tokio::sync::Notify::new();
        let old_drive = async {
            let result = engine
                .start::<HolderChangeTask<TestStore, Turso>>(&old_key, old_request)
                .await;
            old_finished.notify_one();
            result
        };
        let new_drive = async {
            old_gate.started.notified().await;
            let result = engine
                .start::<HolderChangeTask<TestStore, Turso>>(&new_key, new_request)
                .await;
            new_finished.notify_one();
            result
        };
        let control = async {
            new_gate.started.notified().await;
            if old_fence_first {
                old_gate.proceed.notify_one();
                old_finished.notified().await;
                new_gate.proceed.notify_one();
            } else {
                new_gate.proceed.notify_one();
                new_finished.notified().await;
                old_gate.proceed.notify_one();
            }
        };
        let (old_operation, new_operation, ()) =
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(old_drive, new_drive, control)
            })
            .await?;
        let old_operation = old_operation?;
        let new_operation = new_operation?;
        assert_eq!(
            saved_answer(&engine, old_operation).await?,
            HolderChangeAnswer::Refused(HolderChangeRefusal::Superseded { by: takeover_id })
        );
        if old_fence_first {
            assert_eq!(
                saved_answer(&engine, new_operation).await?,
                HolderChangeAnswer::Refused(HolderChangeRefusal::PreviewStale)
            );
            assert_eq!(
                scalar(
                    &store,
                    "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_history"
                )
                .await?,
                "1"
            );
            let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
                &engine,
                &RequestKey::new("preview-after-old-fence")?,
                PreviewTakeover {
                    database: database()?,
                },
            )
            .await?;
            assert_eq!(preview.fence, Epoch::new(2)?);
            let request = HolderChangeRequest::Takeover {
                attempt: transfer(703)?,
                database: database()?,
                preview,
                to: peer(3)?,
            };
            let answer = task_answer::<HolderChangeTask<TestStore, Turso>>(
                &engine,
                &holder_change_request_key(transfer(703)?)?,
                request,
            )
            .await?;
            assert!(
                matches!(answer, HolderChangeAnswer::Granted { epoch, .. } if epoch == Epoch::new(4)?)
            );
        } else {
            assert!(
                matches!(saved_answer(&engine, new_operation).await?, HolderChangeAnswer::Granted { epoch, .. } if epoch == Epoch::new(3)?)
            );
        }
        let expected = if old_fence_first {
            Epoch::new(4)?
        } else {
            Epoch::new(3)?
        };
        assert_eq!(
            scalar(
                &store,
                "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_history"
            )
            .await?,
            "2"
        );
        assert_eq!(
            scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
            expected.value().to_string()
        );
        let mut copies = host.copies.lock().await;
        let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
        assert!(
            matches!(fleet_sync_receiver::copy_status(&mut CopyScope(copy)).await?, CopyStatusAnswer::Status(CopyStatus { fence, holder: Some(holder), .. }) if fence == expected && holder == peer(3)?)
        );
    }
    Ok(())
}
