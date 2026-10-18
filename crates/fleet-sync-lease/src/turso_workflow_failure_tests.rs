//! Acknowledgement is not inferred from a failed child or a misdecoded saved answer.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_registry_fixture::*,
    turso_workflow_tests::{attempt, cancel, move_request, view},
    *,
};
use durable_steps::{AtomicStore, RequestKey, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::sync::atomic::Ordering;

async fn import_copy(engine: &TestEngine) -> TestResult {
    let import = import_request(201, "a")?;
    task_answer::<ImportTask<TestStore, Turso>>(
        engine,
        &lifecycle_request_key(import.attempt)?,
        import,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn refused_claim_with_unanswered_settle_reports_owed_from_the_wrapped_reserve_answer()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&directory.path().join("registry.db"), host.clone(), true).await?;
    let engine = engine(store.clone(), host.clone())?;
    import_copy(&engine).await?;
    crate::turso_copy_fixture::seed_import(&host).await?;
    crate::turso_claim_refusal_tests::retire_copy(&host).await?;
    host.settlement_unreachable.store(true, Ordering::SeqCst);
    let id = attempt(3001)?;
    let request = HolderChangeRequest::Claim {
        attempt: id,
        database: database()?,
        claimant: peer(2)?,
    };
    engine
        .start::<HolderChangeTask<TestStore, Turso>>(&holder_change_request_key(id)?, request)
        .await?;
    let record = view(&store, id).await?;
    assert_eq!(
        record.outcome,
        Some(HolderChangeAnswer::Refused(HolderChangeRefusal::Retired))
    );
    assert!(record.settlement_owed);
    assert_eq!(
        record.waiting.ok_or("missing settlement wait")?.on,
        WaitingOn::Gate
    );
    assert!(record.follow_up_failure.is_none());
    Ok(())
}

#[tokio::test]
async fn version_failed_withdrawal_stays_cancelling_records_failure_and_can_be_superseded()
-> TestResult {
    for unanswered in [false, true] {
        let directory = tempfile::tempdir()?;
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(&directory.path().join("registry.db"), host.clone(), true).await?;
        let engine = engine(store.clone(), host.clone())?;
        crate::turso_change_tests::register_first(&engine).await?;
        let old = attempt(3002)?;
        host.lose_release.store(true, Ordering::SeqCst);
        engine
            .start::<HolderChangeTask<TestStore, Turso>>(
                &holder_change_request_key(old)?,
                move_request(old)?,
            )
            .await?;
        cancel(&store, old).await?;
        host.advance();
        engine.drive_due().await?;
        if unanswered {
            host.withdrawal_unreachable.store(true, Ordering::SeqCst);
            engine.drive_due().await?;
        }
        let lease = scalar(&store, "SELECT record AS value FROM _fleet_sync_leases").await?;
        let mut mismatch = TestEngine::new(store.clone(), host.clone());
        mismatch.register(VersionedTask::<_, 2>(HolderChangeCancelTask::<
            TestStore,
            Turso,
        >::new(
            config()?, host.clone()
        )))?;
        host.advance();
        assert_eq!(mismatch.drive_due().await?.failed.len(), 1);
        let failed = view(&store, old).await?;
        assert_eq!(failed.phase, HolderChangePhase::Cancelling);
        assert_eq!(
            scalar(&store, "SELECT record AS value FROM _fleet_sync_leases").await?,
            lease
        );
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
        assert!(failed.outcome.is_none());
        assert_eq!(
            failed.follow_up_failure,
            Some(FailureCode::new("versionMismatch")?)
        );
        let cleanup = scalar(
            &store,
            "SELECT cleanup_failed AS value FROM _fleet_sync_transfers",
        )
        .await?;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&cleanup)?,
            serde_json::json!({"reason":"versionMismatch","at":2_001_000})
        );
        let new = attempt(3003)?;
        let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
            &engine,
            &RequestKey::new("cleanup-failure-preview")?,
            PreviewTakeover {
                database: database()?,
            },
        )
        .await?;
        let request = HolderChangeRequest::Takeover {
            attempt: new,
            database: database()?,
            preview,
            to: peer(3)?,
        };
        assert!(matches!(
            task_answer::<HolderChangeTask<TestStore, Turso>>(
                &engine,
                &holder_change_request_key(new)?,
                request
            )
            .await?,
            HolderChangeAnswer::Granted { .. }
        ));
        assert_eq!(
            view(&store, old).await?.outcome,
            Some(HolderChangeAnswer::Refused(
                HolderChangeRefusal::Superseded { by: new }
            ))
        );
        assert_eq!(scalar(&store,"SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_transfers WHERE outcome IS NULL").await?,"0");
    }
    Ok(())
}

#[tokio::test]
async fn version_failed_settlement_keeps_granted_and_owed_with_the_child_failure_visible()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("registry.db");
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&path, host.clone(), true).await?;
    let engine = engine(store.clone(), host.clone())?;
    import_copy(&engine).await?;
    crate::turso_copy_fixture::seed_import(&host).await?;
    host.settlement_unreachable.store(true, Ordering::SeqCst);
    let id = attempt(3004)?;
    engine
        .start::<HolderChangeTask<TestStore, Turso>>(
            &holder_change_request_key(id)?,
            HolderChangeRequest::Claim {
                attempt: id,
                database: database()?,
                claimant: peer(2)?,
            },
        )
        .await?;
    let granted = view(&store, id).await?.outcome;
    let mut mismatch = TestEngine::new(store.clone(), host.clone());
    mismatch.register(VersionedTask::<_, 2>(
        HolderChangeTask::<TestStore, Turso>::new(config()?, host.clone()),
    ))?;
    host.advance();
    assert_eq!(mismatch.drive_due().await?.failed.len(), 1);
    let mut child_mismatch = TestEngine::new(store.clone(), host.clone());
    child_mismatch.register(VersionedTask::<_, 2>(
        SettleReservationTask::<TestStore>::new(),
    ))?;
    assert_eq!(child_mismatch.drive_due().await?.failed.len(), 1);
    let record = view(&store, id).await?;
    assert_eq!(record.outcome, granted);
    assert!(
        matches!(record.outcome,Some(HolderChangeAnswer::Granted{epoch,..}) if epoch==Epoch::new(2)?)
    );
    assert!(record.settlement_owed);
    assert_eq!(
        record.follow_up_failure,
        Some(FailureCode::new("versionMismatch")?)
    );
    assert!(record.waiting.is_none());
    drop(engine);
    drop(mismatch);
    drop(child_mismatch);
    drop(store);
    let reopened = open_store(&path, host.clone(), false).await?;
    assert_eq!(view(&reopened, id).await?, record);
    Ok(())
}

#[tokio::test]
async fn admin_view_rejects_unwrapped_or_malformed_saved_call_answers() -> TestResult {
    for output in [br#"{"kind":"reserved"}"#.to_vec(), b"invalid-json".to_vec()] {
        let directory = tempfile::tempdir()?;
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(&directory.path().join("registry.db"), host.clone(), true).await?;
        let engine = engine(store.clone(), host.clone())?;
        import_copy(&engine).await?;
        host.settlement_unreachable.store(true, Ordering::SeqCst);
        let id = attempt(3005)?;
        engine
            .start::<HolderChangeTask<TestStore, Turso>>(
                &holder_change_request_key(id)?,
                HolderChangeRequest::Claim {
                    attempt: id,
                    database: database()?,
                    claimant: peer(2)?,
                },
            )
            .await?;
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    sqlx_core::query::query::<Turso>(
                        "UPDATE _fleet_sync_steps SET output=? WHERE step_name='reserve'",
                    )
                    .bind(output)
                    .execute(scope.executor())
                    .await?;
                    Ok(())
                })
            })
            .await?;
        let error = store
            .transaction(move |scope| {
                Box::pin(async move { Ok(holder_change_view(scope, id).await.err()) })
            })
            .await?;
        assert!(
            matches!(error,Some(LeaseError::Corrupt{field}) if field=="holder-change saved call answer")
        );
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_claim_failure_survives_reopen_while_a_same_version_settlement_clears_debt()
-> TestResult {
    for fail_child in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("registry.db");
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(&path, host.clone(), true).await?;
        let engine = engine(store.clone(), host.clone())?;
        import_copy(&engine).await?;
        host.lose_reservation.store(true, Ordering::SeqCst);
        host.settlement_unreachable.store(true, Ordering::SeqCst);
        let id = attempt(3006)?;
        engine
            .start::<HolderChangeTask<TestStore, Turso>>(
                &holder_change_request_key(id)?,
                HolderChangeRequest::Claim {
                    attempt: id,
                    database: database()?,
                    claimant: peer(2)?,
                },
            )
            .await?;
        cancel(&store, id).await?;
        host.advance();
        engine.drive_due().await?;
        engine.drive_due().await?;
        assert!(view(&store, id).await?.settlement_owed);
        let mut child = TestEngine::new(store.clone(), host.clone());
        if fail_child {
            child.register(VersionedTask::<_, 2>(
                SettleReservationTask::<TestStore>::new(),
            ))?;
        } else {
            child.register(SettleReservationTask::<TestStore>::new())?;
        }
        host.settlement_unreachable.store(false, Ordering::SeqCst);
        host.advance();
        let result = child.drive_due().await?;
        assert_eq!(result.failed.len(), usize::from(fail_child));
        assert_eq!(result.completed.len(), usize::from(!fail_child));
        let record = view(&store, id).await?;
        assert_eq!(
            record.outcome,
            Some(HolderChangeAnswer::Cancelled {
                by: ActorRef::new("admin-canceller")?
            })
        );
        assert_eq!(record.settlement_owed, fail_child);
        assert_eq!(
            record.follow_up_failure,
            if fail_child {
                Some(FailureCode::new("versionMismatch")?)
            } else {
                None
            }
        );
        assert_eq!(
            host.gate.settlement_count().await?,
            if fail_child { "0" } else { "1" }
        );
        drop(engine);
        drop(child);
        drop(store);
        let reopened = open_store(&path, host.clone(), false).await?;
        assert_eq!(view(&reopened, id).await?, record);
    }
    Ok(())
}
