//! Public workflow admission, inspection and cancellation over actual journal/store transactions.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_registry_fixture::*, *};
use durable_steps::{AtomicStore, Uuid};
use fleet_sync_protocol::{CommitSequence, ReleaseAnswer};
use sqlx_turso::Turso;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub(super) fn actor(value: &str) -> TestResult<ActorRef> {
    Ok(ActorRef::new(value)?)
}
pub(super) fn attempt(value: u128) -> TestResult<TransferId> {
    Ok(TransferId::new(Uuid::from_u128(
        0x019f0000000070008000000000000000 | value,
    ))?)
}
pub(super) fn move_request(id: TransferId) -> TestResult<HolderChangeRequest> {
    Ok(HolderChangeRequest::Move {
        attempt: id,
        database: database()?,
        from_epoch: Epoch::new(1)?,
        to: peer(2)?,
    })
}
pub(super) async fn view(store: &TestStore, id: TransferId) -> TestResult<HolderChangeView> {
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move {
                holder_change_view(scope, id)
                    .await
                    .map_err(storage)?
                    .ok_or_else(|| storage("missing workflow view"))
            })
        })
        .await?)
}
pub(super) async fn cancel(store: &TestStore, id: TransferId) -> TestResult<CancelAnswer> {
    let cancel = CancelHolderChange {
        attempt: id,
        by: actor("admin-canceller")?,
        reason: CancelReason::new("operator changed course")?,
    };
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move {
                cancel_holder_change(scope, &cancel, SyncTime::from_unix_millis(1001))
                    .await
                    .map_err(storage)
            })
        })
        .await?)
}

#[tokio::test]
async fn precheck_admission_binds_distinct_ids_and_preserves_first_requester_and_cancel()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let id = attempt(2601)?;
    let request = move_request(id)?;
    let offered = request.clone();
    let admitted = store
        .transaction(move |scope| {
            Box::pin(async move {
                admit_holder_change::<TestStore, Turso>(
                    scope,
                    &offered,
                    &ActorRef::new("first-admin").map_err(storage)?,
                    SyncTime::from_unix_millis(1000),
                )
                .await
                .map_err(storage)
            })
        })
        .await?;
    assert_ne!(admitted.id().as_uuid(), id.as_uuid());
    let before = view(&store, id).await?;
    assert_eq!(before.request, request);
    assert_eq!(before.requested_by, actor("first-admin")?);
    assert_eq!(before.phase, HolderChangePhase::Requested);
    assert_eq!(
        before.history,
        vec![PhaseEntry {
            phase: HolderChangePhase::Requested,
            entered_at: SyncTime::from_unix_millis(1000)
        }]
    );
    assert!(before.waiting.is_none() && before.outcome.is_none());
    let again = store
        .transaction(move |scope| {
            Box::pin(async move {
                admit_holder_change::<TestStore, Turso>(
                    scope,
                    &request,
                    &ActorRef::new("second-admin").map_err(storage)?,
                    SyncTime::from_unix_millis(2000),
                )
                .await
                .map_err(storage)
            })
        })
        .await?;
    assert_eq!(again.id(), admitted.id());
    assert_eq!(view(&store, id).await?.requested_by, actor("first-admin")?);
    let first = cancel(&store, id).await?;
    assert!(
        matches!(first,CancelAnswer::Accepted{ref recorded,phase:HolderChangePhase::Requested} if recorded.by==actor("admin-canceller")?)
    );
    let repeated = store
        .transaction(move |scope| {
            Box::pin(async move {
                cancel_holder_change(
                    scope,
                    &CancelHolderChange {
                        attempt: id,
                        by: ActorRef::new("another-admin").map_err(storage)?,
                        reason: CancelReason::new("different repeated reason").map_err(storage)?,
                    },
                    SyncTime::from_unix_millis(2000),
                )
                .await
                .map_err(storage)
            })
        })
        .await?;
    assert_eq!(repeated, first);
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    assert_eq!(engine.drive_due().await?.completed, vec![admitted.id()]);
    let ended = view(&store, id).await?;
    assert_eq!(ended.phase, HolderChangePhase::Ended);
    assert_eq!(
        ended.outcome,
        Some(HolderChangeAnswer::Cancelled {
            by: actor("admin-canceller")?
        })
    );
    assert!(host.calls.lock().await.is_empty());
    assert!(matches!(
        cancel(&store, attempt(2699)?).await?,
        CancelAnswer::NotFound
    ));
    Ok(())
}

#[tokio::test]
async fn move_cancellation_withdraws_release_from_both_pending_holder_and_pending_copy()
-> TestResult {
    for released in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("registry.db");
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(&path, host.clone(), true).await?;
        let engine = engine(store.clone(), host.clone())?;
        crate::turso_change_tests::register_first(&engine).await?;
        let id = attempt(2701)?;
        if released {
            *host.release_answer.lock().await = Some(ReleaseAnswer::Released {
                final_position: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(9)),
            });
        } else {
            host.lose_release.store(true, Ordering::SeqCst);
        }
        let operation = engine
            .start::<HolderChangeTask<TestStore, Turso>>(
                &holder_change_request_key(id)?,
                move_request(id)?,
            )
            .await?;
        let before = view(&store, id).await?;
        assert_eq!(
            before.phase,
            if released {
                HolderChangePhase::Released {
                    final_position: ReplicationPosition::new(
                        Epoch::new(1)?,
                        CommitSequence::new(9),
                    ),
                }
            } else {
                HolderChangePhase::Releasing
            }
        );
        let waiting = before.waiting.ok_or("pending call missing")?;
        assert_eq!(
            waiting.on,
            if released {
                WaitingOn::Copy(config()?.authoritative_receiver)
            } else {
                WaitingOn::Holder(peer(1)?)
            }
        );
        assert_eq!(waiting.since, SyncTime::from_unix_millis(1000));
        assert_eq!(
            waiting.reach,
            durable_steps::storage::CallReach::MayHaveReached
        );
        assert!(matches!(
            cancel(&store, id).await?,
            CancelAnswer::Accepted { .. }
        ));
        host.withdrawal_unreachable.store(true, Ordering::SeqCst);
        let calls = host.calls.lock().await.len();
        host.advance();
        assert!(engine.drive_due().await?.completed.contains(&operation));
        let cancelling = view(&store, id).await?;
        assert_eq!(cancelling.phase, HolderChangePhase::Cancelling);
        assert!(cancelling.outcome.is_none());
        assert!(
            host.calls.lock().await[calls..]
                .iter()
                .all(|(kind, _)| kind != "fleet-sync.holder-release"
                    && kind != "fleet-sync.copy-acknowledged"
                    && kind != "fleet-sync.copy-fence")
        );
        assert_eq!(
            store
                .transaction(move |scope| Box::pin(async move {
                    holder_change_answer(scope, &move_request(id).map_err(storage)?)
                        .await
                        .map_err(storage)
                }))
                .await?,
            Some(HolderChangeAnswer::Pending { transfer: id })
        );
        engine.drive_due().await?;
        let follow = view(&store, id).await?;
        assert_eq!(
            follow
                .waiting
                .ok_or("pending withdrawal not visible after parent compacted")?
                .on,
            WaitingOn::Holder(peer(1)?)
        );
        drop(engine);
        drop(store);
        let store = open_store(&path, host.clone(), false).await?;
        assert_eq!(view(&store, id).await?.phase, HolderChangePhase::Cancelling);
        host.withdrawal_unreachable.store(false, Ordering::SeqCst);
        host.advance();
        let resumed = crate::turso_registry_fixture::engine(store.clone(), host.clone())?;
        resumed.drive_due().await?;
        let ended = view(&store, id).await?;
        assert_eq!(ended.phase, HolderChangePhase::Ended);
        assert_eq!(
            ended.outcome,
            Some(HolderChangeAnswer::Cancelled {
                by: actor("admin-canceller")?
            })
        );
        assert!(ended.waiting.is_none());
        assert_eq!(
            scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
            "2"
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_history"
            )
            .await?,
            "1"
        );
        let history = ended.history.clone();
        resumed
            .start::<HolderChangeTask<TestStore, Turso>>(
                &holder_change_request_key(id)?,
                move_request(id)?,
            )
            .await?;
        assert_eq!(view(&store, id).await?.history, history);
    }
    Ok(())
}

#[tokio::test]
async fn claim_cancellation_decides_outcome_while_its_gate_settlement_stays_owed() -> TestResult {
    for reserved in [false, true] {
        let directory = tempfile::tempdir()?;
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(&directory.path().join("registry.db"), host.clone(), true).await?;
        let engine = engine(store.clone(), host.clone())?;
        let import = import_request(201, "a")?;
        task_answer::<ImportTask<TestStore, Turso>>(
            &engine,
            &lifecycle_request_key(import.attempt)?,
            import,
        )
        .await?;
        crate::turso_copy_fixture::seed_import(&host).await?;
        let id = attempt(2702)?;
        host.lose_reservation.store(!reserved, Ordering::SeqCst);
        host.status_unreachable.store(reserved, Ordering::SeqCst);
        host.settlement_unreachable.store(true, Ordering::SeqCst);
        let request = HolderChangeRequest::Claim {
            attempt: id,
            database: database()?,
            claimant: peer(2)?,
        };
        engine
            .start::<HolderChangeTask<TestStore, Turso>>(&holder_change_request_key(id)?, request)
            .await?;
        assert_eq!(
            view(&store, id).await?.phase,
            if reserved {
                HolderChangePhase::Reserved
            } else {
                HolderChangePhase::Requested
            }
        );
        cancel(&store, id).await?;
        host.advance();
        engine.drive_due().await?;
        engine.drive_due().await?;
        let ended = view(&store, id).await?;
        assert_eq!(ended.phase, HolderChangePhase::Ended);
        assert_eq!(
            ended.outcome,
            Some(HolderChangeAnswer::Cancelled {
                by: actor("admin-canceller")?
            })
        );
        assert!(ended.settlement_owed);
        assert_eq!(
            ended.waiting.ok_or("owed settlement not visible")?.on,
            WaitingOn::Gate
        );
        assert!(
            ended
                .history
                .iter()
                .all(|entry| entry.phase != HolderChangePhase::Cancelling)
        );
        host.settlement_unreachable.store(false, Ordering::SeqCst);
        host.advance();
        engine.drive_due().await?;
        assert!(!view(&store, id).await?.settlement_owed);
        let mut gate = host.gate.connection.lock().await;
        use sqlx_core::row::Row;
        let row = sqlx_core::query::query::<Turso>("SELECT outcome FROM settlements")
            .fetch_one(&mut *gate)
            .await?;
        assert_eq!(
            serde_json::from_str::<SettleOutcome>(&row.try_get::<String, _>("outcome")?)?,
            SettleOutcome::Cancelled
        );
    }
    Ok(())
}
