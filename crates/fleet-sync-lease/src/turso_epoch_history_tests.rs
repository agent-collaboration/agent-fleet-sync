//! Expectations come from actual grants and literal lineage, never the projection itself.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_epoch_history_fixture::*, turso_registry_fixture::*, turso_workflow_tests::attempt, *,
};
use durable_steps::{AtomicStore, RequestKey, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

fn start() -> TestResult<ReplicationPosition> {
    Ok(ReplicationPosition::new(
        Epoch::new(1)?,
        CommitSequence::new(0),
    ))
}

#[tokio::test]
async fn history_keeps_zero_write_grants_and_distinguishes_release_from_fence_after_reopen()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let (host, store, engine) = registered(directory.path()).await?;
    host.now.store(1500, Ordering::SeqCst);
    assert!(
        matches!(move_to(&engine, attempt(3001)?, 1, 2).await?, HolderChangeAnswer::Granted { epoch, start: position, .. } if epoch == Epoch::new(2)? && position == start()?)
    );
    host.advance();
    assert!(
        matches!(takeover(&engine, attempt(3002)?, 3).await?, HolderChangeAnswer::Granted { epoch, start: position, .. } if epoch == Epoch::new(3)? && position == start()?)
    );
    let expected = EpochHistoryPage {
        entries: vec![
            EpochHistoryEntry {
                grant: EpochGrant {
                    epoch: Epoch::new(1)?,
                    holder: peer(1)?,
                    granted_at: SyncTime::from_unix_millis(1000),
                    ended_at: Some(start()?),
                },
                end: EpochEnd::Released {
                    final_position: start()?,
                },
            },
            EpochHistoryEntry {
                grant: EpochGrant {
                    epoch: Epoch::new(2)?,
                    holder: peer(2)?,
                    granted_at: SyncTime::from_unix_millis(1500),
                    ended_at: Some(start()?),
                },
                end: EpochEnd::Fenced { ended_at: start()? },
            },
        ],
        next_after: None,
    };
    let before = records(&store).await?;
    let calls = host.calls.lock().await.len();
    assert_eq!(page(&store, None, 3, 10).await??, expected);
    assert_eq!(page(&store, None, 3, 10).await??, expected);
    assert_eq!(records(&store).await?, before);
    assert_eq!(host.calls.lock().await.len(), calls);
    drop(engine);
    drop(store);
    let reopened = open_store(&directory.path().join("registry.db"), host, false).await?;
    assert_eq!(page(&reopened, None, 3, 10).await??, expected);
    Ok(())
}

#[tokio::test]
async fn history_skips_a_real_repaired_reservation_and_preserves_frozen_paging() -> TestResult {
    use crate::{turso_floor_repair_tests::advance_copy, turso_registry_copy::FenceGate};
    let directory = tempfile::tempdir()?;
    let (host, store, engine) = registered(directory.path()).await?;
    let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
        &engine,
        &RequestKey::new("history-floor-repair")?,
        PreviewTakeover {
            database: database()?,
        },
    )
    .await?;
    let id = attempt(3003)?;
    let gate = FenceGate::new();
    host.fence_gates.lock().await.insert(id, Arc::clone(&gate));
    let request = HolderChangeRequest::Takeover {
        reason: fleet_sync_protocol::HolderChangeReason::new("exercise holder change")?,
        attempt: id,
        database: database()?,
        preview,
        to: peer(2)?,
    };
    let key = holder_change_request_key(id)?;
    let drive = task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, request);
    let control = async {
        gate.started.notified().await;
        advance_copy(&host, 1, 9).await?;
        host.fence_gates.lock().await.remove(&id);
        gate.proceed.notify_one();
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let (answer, control) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(drive, control)
    })
    .await?;
    control?;
    assert!(
        matches!(answer?, HolderChangeAnswer::Granted { epoch, .. } if epoch == Epoch::new(10)?)
    );
    assert!(
        matches!(move_to(&engine, attempt(3004)?, 10, 3).await?, HolderChangeAnswer::Granted { epoch, .. } if epoch == Epoch::new(11)?)
    );
    let first = page(&store, None, 11, 1).await??;
    assert_eq!(
        first
            .entries
            .iter()
            .map(|entry| entry.grant.epoch.value())
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(first.next_after, Some(Epoch::new(1)?));
    assert_eq!(
        first.entries[0].end,
        EpochEnd::Fenced { ended_at: start()? }
    );
    // A new grant cannot enter the caller's frozen interval.
    assert!(
        matches!(move_to(&engine, attempt(3005)?, 11, 4).await?, HolderChangeAnswer::Granted { epoch, .. } if epoch == Epoch::new(12)?)
    );
    let second = page(&store, Some(1), 11, 1).await??;
    assert_eq!(
        second
            .entries
            .iter()
            .map(|entry| entry.grant.epoch.value())
            .collect::<Vec<_>>(),
        vec![10]
    );
    assert_eq!(
        second.entries[0].end,
        EpochEnd::Released {
            final_position: start()?
        }
    );
    assert_eq!(second.next_after, None);
    assert_eq!(page(&store, None, 11, 1).await??, first);
    assert!(page(&store, Some(10), 11, 1).await??.entries.is_empty());
    Ok(())
}

#[tokio::test]
async fn history_never_invents_imported_unheld_epochs_and_can_read_an_open_grant() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    let request = import_request(3006, "a")?;
    assert_eq!(
        task_answer::<ImportTask<TestStore, Turso>>(
            &engine,
            &lifecycle_request_key(request.attempt)?,
            request
        )
        .await?,
        ImportAnswer::Imported
    );
    assert_eq!(page(&store, None, 100, 4).await??.entries, vec![]);
    let position = crate::turso_copy_fixture::seed_import(&host).await?;
    let id = attempt(3007)?;
    assert!(
        matches!(task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &holder_change_request_key(id)?, HolderChangeRequest::Claim { attempt: id, database: database()?, claimant: peer(2)? }).await?, HolderChangeAnswer::Granted { epoch, start, .. } if epoch == Epoch::new(2)? && start == position)
    );
    let history = page(&store, None, 3, 4).await??;
    assert_eq!(
        history.entries,
        vec![EpochHistoryEntry {
            grant: EpochGrant {
                epoch: Epoch::new(2)?,
                holder: peer(2)?,
                granted_at: SyncTime::from_unix_millis(1000),
                ended_at: None
            },
            end: EpochEnd::Open
        }]
    );
    assert_eq!(history.next_after, None);
    Ok(())
}

#[tokio::test]
async fn history_cancelled_release_is_not_a_permanent_end_or_an_actual_grant() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (host, store, engine) = registered(directory.path()).await?;
    *host.release_answer.lock().await = Some(ReleaseAnswer::Released {
        final_position: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(9)),
    });
    let id = attempt(3008)?;
    let operation = engine
        .start::<HolderChangeTask<TestStore, Turso>>(
            &holder_change_request_key(id)?,
            crate::turso_workflow_tests::move_request(id)?,
        )
        .await?;
    assert!(matches!(
        crate::turso_workflow_tests::view(&store, id).await?.phase,
        HolderChangePhase::Released { .. }
    ));
    assert!(matches!(
        crate::turso_workflow_tests::cancel(&store, id).await?,
        CancelAnswer::Accepted { .. }
    ));
    host.advance();
    assert!(engine.drive_due().await?.completed.contains(&operation));
    engine.drive_due().await?;
    assert!(matches!(
        crate::turso_workflow_tests::view(&store, id).await?.outcome,
        Some(HolderChangeAnswer::Cancelled { .. })
    ));
    let history = page(&store, None, 3, 10).await??;
    assert_eq!(history.entries.len(), 1);
    assert_eq!(history.entries[0].grant.epoch, Epoch::new(1)?);
    assert_eq!(history.entries[0].grant.ended_at, None);
    assert_eq!(history.entries[0].end, EpochEnd::Open);
    Ok(())
}

#[tokio::test]
async fn history_refuses_corrupt_rows_or_successor_evidence_without_changing_them() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (_, store, engine) = registered(directory.path()).await?;
    move_to(&engine, attempt(3009)?, 1, 2).await?;
    for sql in [
        "UPDATE _fleet_sync_epoch_history SET record='not JSON' WHERE epoch='1'",
        "UPDATE _fleet_sync_epoch_history SET epoch='01' WHERE epoch='1'",
        "UPDATE _fleet_sync_epoch_history SET epoch='18446744073709551616' WHERE epoch='1'",
        "UPDATE _fleet_sync_epoch_history SET epoch='2x' WHERE epoch='1'",
        "DELETE FROM _fleet_sync_epoch_history WHERE epoch='2'",
        "UPDATE _fleet_sync_transfers SET outcome='null'",
        "UPDATE _fleet_sync_transfers SET phase_history='[]'",
    ] {
        let error = store
            .transaction(move |scope| {
                Box::pin(async move {
                    sqlx_core::query::query::<Turso>(sql)
                        .execute(scope.executor())
                        .await?;
                    let before = records_in_scope(scope).await?;
                    assert!(
                        matches!(
                            epoch_history(
                                scope,
                                database().map_err(storage)?,
                                None,
                                Epoch::new(2).map_err(storage)?,
                                10
                            )
                            .await,
                            Err(LeaseError::Corrupt { .. })
                        ),
                        "accepted {sql}"
                    );
                    assert_eq!(records_in_scope(scope).await?, before);
                    // Roll back deliberately to retain the same valid baseline for the next corruption.
                    Err::<(), _>(storage("rollback corruption probe"))
                })
            })
            .await;
        assert!(error.is_err());
    }
    assert_eq!(page(&store, None, 2, 10).await??.entries.len(), 1);
    Ok(())
}

#[tokio::test]
async fn history_validates_limits_and_does_not_truncate_unsigned_epoch_order() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (host, store, engine) = registered(directory.path()).await?;
    assert!(matches!(
        page(&store, None, 2, 0).await?,
        Err(LeaseError::InvalidInput { .. })
    ));
    assert!(page(&store, Some(2), 2, 1).await??.entries.is_empty());
    assert!(
        matches!(move_to(&engine, attempt(3010)?, 1, 2).await?, HolderChangeAnswer::Granted { epoch, .. } if epoch == Epoch::new(2)?)
    );
    crate::turso_floor_repair_tests::advance_copy(&host, 2, u64::MAX - 2).await?;
    assert!(
        matches!(takeover(&engine, attempt(3011)?, 3).await?, HolderChangeAnswer::Granted { epoch, .. } if epoch == Epoch::new(u64::MAX - 1)?)
    );
    // Integer walking must not turn three real grants into an unbounded search.
    let first =
        tokio::time::timeout(Duration::from_secs(5), page(&store, None, u64::MAX, 1)).await???;
    assert_eq!(first.entries[0].grant.epoch, Epoch::new(1)?);
    assert_eq!(first.next_after, Some(Epoch::new(1)?));
    let second = tokio::time::timeout(Duration::from_secs(5), page(&store, Some(1), u64::MAX, 1))
        .await???;
    assert_eq!(second.entries[0].grant.epoch, Epoch::new(2)?);
    assert_eq!(
        second.entries[0].end,
        EpochEnd::Fenced { ended_at: start()? }
    );
    assert_eq!(second.next_after, Some(Epoch::new(2)?));
    let last = page(&store, Some(2), u64::MAX, 1).await??;
    assert_eq!(last.entries[0].grant.epoch, Epoch::new(u64::MAX - 1)?);
    assert_eq!(last.entries[0].end, EpochEnd::Open);
    assert_eq!(last.next_after, None);
    Ok(())
}
