//! Copy availability is controlled; registry, copy rules, gate and journals are real Turso.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_registry_fixture::*,
    turso_workflow_tests::{attempt, move_request, view},
    *,
};
use durable_steps::{
    AtomicStore,
    storage::{CallReach, TransactionScope},
};
use fleet_sync_protocol::*;
use sqlx_core::query::query;
use sqlx_turso::Turso;

#[tokio::test]
async fn claim_ack_wait_and_fence_keep_pending_history_then_resume_the_same_attempt() -> TestResult
{
    for (index, target) in [
        "fleet-sync.copy-status",
        "fleet-sync.copy-acknowledged",
        "fleet-sync.copy-fence",
    ]
    .into_iter()
    .enumerate()
    {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("registry.db");
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(&path, host.clone(), true).await?;
        let running = engine(store.clone(), host.clone())?;
        let id = attempt(4000 + index as u128)?;
        let request = if index == 0 {
            let import = import_request(4010, "a")?;
            task_answer::<ImportTask<TestStore, Turso>>(
                &running,
                &lifecycle_request_key(import.attempt)?,
                import,
            )
            .await?;
            crate::turso_copy_fixture::seed_import(&host).await?;
            HolderChangeRequest::Claim {
                attempt: id,
                database: database()?,
                claimant: peer(2)?,
            }
        } else {
            crate::turso_change_tests::register_first(&running).await?;
            move_request(id)?
        };
        *host.copy_no_effect.lock().await = Some(target);
        let operation = running
            .start::<HolderChangeTask<TestStore, Turso>>(
                &holder_change_request_key(id)?,
                request.clone(),
            )
            .await?;
        let before = view(&store, id).await?;
        assert!(before.outcome.is_none());
        let waiting = before.waiting.as_ref().ok_or("copy wait missing")?;
        assert_eq!(waiting.reach, CallReach::NoEffect);
        assert_eq!(
            waiting.on,
            WaitingOn::Copy(config()?.authoritative_receiver)
        );
        assert!(matches!(
            waiting.copy_unavailable,
            Some(CopyUnavailableReason::Damaged { .. })
        ));
        let epoch_floor =
            scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?;
        let reservation =
            scalar(&store, "SELECT intent AS value FROM _fleet_sync_transfers").await?;
        assert_eq!(host.gate.settlement_count().await?, "0");
        drop(running);
        drop(store);
        let reopened = open_store(&path, host.clone(), false).await?;
        assert_eq!(view(&reopened, id).await?, before);
        // An unknown generic code must fail closed at the domain view.
        let corrupt_probe = reopened.transaction(move |scope| Box::pin(async move {
            query::<Turso>("UPDATE _fleet_sync_steps SET intent=json_set(intent,'$.retry_reason.code','unknown_copy_reason') WHERE operation_id=? AND kind='call_pending'")
                .bind(operation.as_uuid().to_string()).execute(scope.executor()).await?;
            let error = holder_change_view(scope, id).await;
            if !matches!(error, Err(LeaseError::Corrupt { field }) if field == "copy retry reason") {
                return Err(storage("unknown retry code was accepted"));
            }
            Err::<(), _>(durable_steps::StoreError::Aborted { reason: "restore valid original reason".into() })
        })).await;
        assert!(
            matches!(corrupt_probe, Err(durable_steps::StoreError::Aborted { reason }) if reason == "restore valid original reason")
        );
        *host.copy_no_effect.lock().await = None;
        host.advance();
        let resumed = engine(reopened.clone(), host.clone())?;
        resumed.drive_due().await?;
        resumed.drive_due().await?;
        let after = view(&reopened, id).await?;
        assert!(
            matches!(after.outcome, Some(HolderChangeAnswer::Granted { epoch, .. }) if epoch == Epoch::new(2)?)
        );
        assert_eq!(
            scalar(
                &reopened,
                "SELECT epoch AS value FROM _fleet_sync_epoch_floor"
            )
            .await?,
            epoch_floor
        );
        assert_eq!(
            scalar(
                &reopened,
                "SELECT intent AS value FROM _fleet_sync_transfers"
            )
            .await?,
            reservation
        );
        let replayed: HolderChangeAnswer = task_answer::<HolderChangeTask<TestStore, Turso>>(
            &resumed,
            &holder_change_request_key(id)?,
            request,
        )
        .await?;
        assert_eq!(Some(replayed), after.outcome);
        assert_eq!(
            host.gate.settlement_count().await?,
            if index == 0 { "1" } else { "0" }
        );
    }
    Ok(())
}
