//! Failure recording and settlement debt over the actual journal and consumer-gate files.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_copy_fixture::*, turso_registry_fixture::*, *};
use durable_steps::{AtomicStore, OperationState, Uuid};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

fn transfer() -> TestResult<TransferId> {
    Ok(TransferId::new(Uuid::from_u128(301))?)
}
async fn prepare(store: &Arc<TestStore>, host: &Arc<TestHost>) -> TestResult {
    let engine = engine(Arc::clone(store), Arc::clone(host))?;
    let offered = import_request(201, "a")?;
    if task_answer::<ImportTask<TestStore, Turso>>(
        &engine,
        &lifecycle_request_key(offered.attempt)?,
        offered,
    )
    .await?
        != ImportAnswer::Imported
    {
        return Err("import failed".into());
    }
    seed_import(host).await?;
    Ok(())
}
async fn changed_version(store: Arc<TestStore>, host: Arc<TestHost>) -> TestResult<TestEngine> {
    let mut engine = TestEngine::new(store, Arc::clone(&host));
    engine.register(VersionedTask::<_, 2>(
        HolderChangeTask::<TestStore, Turso>::new(config()?, host),
    ))?;
    engine.register(SettleReservationTask::<TestStore>::new())?;
    Ok(engine)
}
#[tokio::test]
async fn version_failure_before_grant_records_failed_and_settles_reserved_claim_refused()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("registry.db");
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&path, Arc::clone(&host), true).await?;
    prepare(&store, &host).await?;
    host.status_unreachable.store(true, Ordering::SeqCst);
    let running = engine(Arc::clone(&store), Arc::clone(&host))?;
    let request = HolderChangeRequest::Claim {
        attempt: transfer()?,
        database: database()?,
        claimant: peer(2)?,
    };
    let id = running
        .start::<HolderChangeTask<TestStore, Turso>>(
            &holder_change_request_key(transfer()?)?,
            request.clone(),
        )
        .await?;
    assert!(matches!(
        running.status(&id).await?.state,
        OperationState::Running { .. }
    ));
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_leases"
        )
        .await?,
        "0"
    );
    drop(running);
    drop(store);
    host.advance();
    let store = open_store(&path, Arc::clone(&host), false).await?;
    let changed = changed_version(Arc::clone(&store), Arc::clone(&host)).await?;
    assert_eq!(changed.drive_due().await?.failed.len(), 1);
    assert_eq!(changed.drive_due().await?.completed.len(), 1);
    assert_eq!(host.gate.settlement_count().await?, "1");
    assert_eq!(
        host.gate.settlement_outcome(transfer()?).await?,
        Some(SettleOutcome::Refused {
            reason: HolderChangeRefusal::Unavailable
        })
    );
    let answer = store
        .transaction(move |scope| {
            Box::pin(async move { holder_change_answer(scope, &request).await.map_err(storage) })
        })
        .await?;
    assert!(matches!(answer, Some(HolderChangeAnswer::Failed { .. })));
    assert_eq!(scalar(&store,"SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_transfers WHERE outcome IS NULL").await?,"0");
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_leases"
        )
        .await?,
        "0"
    );
    Ok(())
}

#[tokio::test]
async fn version_failure_after_committed_grant_preserves_granted_and_settles_it_once() -> TestResult
{
    for lose_answer_after_effect in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("registry.db");
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(&path, Arc::clone(&host), true).await?;
        prepare(&store, &host).await?;
        host.settlement_unreachable
            .store(!lose_answer_after_effect, Ordering::SeqCst);
        host.gate
            .lose_settlement
            .store(lose_answer_after_effect, Ordering::SeqCst);
        let running = engine(Arc::clone(&store), Arc::clone(&host))?;
        let request = HolderChangeRequest::Claim {
            attempt: transfer()?,
            database: database()?,
            claimant: peer(2)?,
        };
        let id = running
            .start::<HolderChangeTask<TestStore, Turso>>(
                &holder_change_request_key(transfer()?)?,
                request.clone(),
            )
            .await?;
        assert!(matches!(
            running.status(&id).await?.state,
            OperationState::Running { .. }
        ));
        let offered = request.clone();
        let answer = store
            .transaction(move |scope| {
                Box::pin(
                    async move { holder_change_answer(scope, &offered).await.map_err(storage) },
                )
            })
            .await?;
        assert!(
            matches!(answer,Some(HolderChangeAnswer::Granted{epoch,..}) if epoch==Epoch::new(2)?)
        );
        let view = crate::turso_workflow_tests::view(&store, transfer()?).await?;
        assert_eq!(view.phase, HolderChangePhase::Ended);
        assert!(view.settlement_owed);

        drop(running);
        drop(store);
        host.advance();
        host.settlement_unreachable.store(true, Ordering::SeqCst);
        let store = open_store(&path, Arc::clone(&host), false).await?;
        let changed = changed_version(Arc::clone(&store), Arc::clone(&host)).await?;
        assert_eq!(changed.drive_due().await?.failed.len(), 1);
        changed.drive_due().await?;
        let pending = crate::turso_workflow_tests::view(&store, transfer()?).await?;
        assert_eq!(pending.phase, HolderChangePhase::Ended);
        assert!(
            matches!(pending.outcome,Some(HolderChangeAnswer::Granted{epoch,..}) if epoch==Epoch::new(2)?)
        );
        assert!(pending.settlement_owed);
        assert_eq!(
            pending.waiting.ok_or("follow-up settlement missing")?.on,
            WaitingOn::Gate
        );
        host.settlement_unreachable.store(false, Ordering::SeqCst);
        host.advance();
        assert_eq!(changed.drive_due().await?.completed.len(), 1);
        assert!(
            !crate::turso_workflow_tests::view(&store, transfer()?)
                .await?
                .settlement_owed
        );
        let answer = store
            .transaction(move |scope| {
                Box::pin(
                    async move { holder_change_answer(scope, &request).await.map_err(storage) },
                )
            })
            .await?;
        assert!(
            matches!(answer,Some(HolderChangeAnswer::Granted{epoch,..}) if epoch==Epoch::new(2)?)
        );
        assert_eq!(host.gate.settlement_count().await?, "1");
        assert_eq!(
            host.gate.settlement_outcome(transfer()?).await?,
            Some(SettleOutcome::Granted {
                epoch: Epoch::new(2)?
            })
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_history"
            )
            .await?,
            "1"
        );
    }
    Ok(())
}

#[tokio::test]
async fn schema_refusal_after_reservation_is_settled_and_a_gate_refusal_has_no_settlement()
-> TestResult {
    for reject_at_gate in [false, true] {
        let directory = tempfile::tempdir()?;
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(
            &directory.path().join("registry.db"),
            Arc::clone(&host),
            true,
        )
        .await?;
        prepare(&store, &host).await?;
        host.gate
            .reject_claim
            .store(reject_at_gate, Ordering::SeqCst);
        let mut engine = TestEngine::new(Arc::clone(&store), Arc::clone(&host));
        let mut policy = config()?;
        policy.target_schema_max = 0;
        engine.register(HolderChangeTask::<TestStore, Turso>::new(
            policy,
            host.clone(),
        ))?;
        let request = HolderChangeRequest::Claim {
            attempt: transfer()?,
            database: database()?,
            claimant: peer(2)?,
        };
        let answer = task_answer::<HolderChangeTask<TestStore, Turso>>(
            &engine,
            &holder_change_request_key(transfer()?)?,
            request,
        )
        .await?;
        if reject_at_gate {
            assert!(matches!(
                answer,
                HolderChangeAnswer::Refused(HolderChangeRefusal::GateRefused { .. })
            ));
            assert_eq!(host.gate.settlement_count().await?, "0");
        } else {
            assert!(matches!(
                answer,
                HolderChangeAnswer::Refused(HolderChangeRefusal::SchemaTooNew {
                    store: SchemaIdentity { version: 3, .. },
                    supported: Some(0)
                })
            ));
            assert_eq!(host.gate.settlement_count().await?, "1");
            assert!(matches!(
                host.gate.settlement_outcome(transfer()?).await?,
                Some(SettleOutcome::Refused {
                    reason: HolderChangeRefusal::SchemaTooNew { .. }
                })
            ));
        }
        assert_eq!(
            scalar(
                &store,
                "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_leases"
            )
            .await?,
            "0"
        );
    }
    Ok(())
}
