//! Admission and completion survive actual transaction rollback and file reopen.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_registry_fixture::*,
    turso_workflow_tests::{actor, attempt, cancel, move_request, view},
    *,
};
use durable_steps::{
    AtomicStore, BoxFuture, Engine, OperationId,
    storage::{JournalStorage, OperationExecution},
};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct RollbackFinishStore {
    inner: Arc<TestStore>,
    operation: OperationId,
    fail: AtomicBool,
}
impl AtomicStore for RollbackFinishStore {
    type Scope<'t> = <TestStore as AtomicStore>::Scope<'t>;
    async fn transaction<T, F>(&self, run: F) -> Result<T, durable_steps::StoreError>
    where
        T: Send + 'static,
        F: for<'s, 't> FnOnce(
                &'s mut Self::Scope<'t>,
            ) -> BoxFuture<'s, Result<T, durable_steps::StoreError>>
            + Send
            + 'static,
    {
        let id = self.operation;
        let fail = self.fail.load(Ordering::SeqCst);
        self.inner
            .transaction(move |scope| {
                Box::pin(async move {
                    let result = run(scope).await?;
                    if fail
                        && matches!(
                            scope.operation(id).await?.map(|record| record.execution),
                            Some(OperationExecution::Completed { .. })
                        )
                    {
                        return Err(durable_steps::StoreError::Aborted {
                            reason: "injected before finish transaction commit".into(),
                        });
                    }
                    Ok(result)
                })
            })
            .await
    }
}

#[tokio::test]
async fn cancellation_follow_up_admission_and_parent_finish_roll_back_in_one_transaction()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&directory.path().join("registry.db"), host.clone(), true).await?;
    let engine = engine(store.clone(), host.clone())?;
    crate::turso_change_tests::register_first(&engine).await?;
    let id = attempt(2901)?;
    host.lose_release.store(true, Ordering::SeqCst);
    let operation = engine
        .start::<HolderChangeTask<TestStore, Turso>>(
            &holder_change_request_key(id)?,
            move_request(id)?,
        )
        .await?;
    cancel(&store, id).await?;
    let before = view(&store, id).await?;
    let fault = Arc::new(RollbackFinishStore {
        inner: store.clone(),
        operation,
        fail: AtomicBool::new(true),
    });
    let mut faulty = Engine::new(fault.clone(), host.clone());
    faulty.register(HolderChangeTask::<RollbackFinishStore, Turso>::new(
        config()?,
        host.clone(),
    ))?;
    host.advance();
    let _report = faulty.drive_due().await;
    let after = view(&store, id).await?;
    assert_eq!(after.phase, before.phase);
    assert_eq!(after.history, before.history);
    assert_eq!(after.outcome, before.outcome);
    assert_eq!(after.cancel, before.cancel);
    assert_eq!(
        after
            .waiting
            .as_ref()
            .map(|waiting| (&waiting.on, waiting.since, waiting.reach)),
        before
            .waiting
            .as_ref()
            .map(|waiting| (&waiting.on, waiting.since, waiting.reach))
    );
    assert_eq!(scalar(&store,"SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_transfer_admissions WHERE follow_up IS NOT NULL").await?,"0");
    assert_eq!(scalar(&store,"SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_operations WHERE kind='fleet-sync.holder-change-cancel'").await?,"0");
    fault.fail.store(false, Ordering::SeqCst);
    host.advance();
    faulty.drive_due().await?;
    assert_eq!(view(&store, id).await?.phase, HolderChangePhase::Cancelling);
    assert_eq!(scalar(&store,"SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_transfer_admissions WHERE follow_up IS NOT NULL").await?,"1");
    Ok(())
}

#[tokio::test]
async fn cancelling_each_kind_before_check_survives_restart_without_external_debt() -> TestResult {
    for kind in ["move", "takeover", "claim"] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("registry.db");
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(&path, host.clone(), true).await?;
        let id = attempt(2902)?;
        let request = match kind {
            "move" => move_request(id)?,
            "takeover" => HolderChangeRequest::Takeover {
                attempt: id,
                database: database()?,
                to: peer(3)?,
                preview: TakeoverPreview {
                    token: PreviewToken::new("019eef00-0000-7000-8000-000000000002")?,
                    fence: Epoch::new(1)?,
                    acked: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0)),
                    holder: peer(1)?,
                    last_seen: None,
                },
            },
            _ => HolderChangeRequest::Claim {
                attempt: id,
                database: database()?,
                claimant: peer(2)?,
            },
        };
        let admitted = store
            .transaction(move |scope| {
                Box::pin(async move {
                    admit_holder_change::<TestStore, Turso>(
                        scope,
                        &request,
                        &ActorRef::new("requester").map_err(storage)?,
                        SyncTime::from_unix_millis(1000),
                    )
                    .await
                    .map_err(storage)
                })
            })
            .await?;
        cancel(&store, id).await?;
        drop(store);
        let store = open_store(&path, host.clone(), false).await?;
        let pending = view(&store, id).await?;
        assert_eq!(pending.phase, HolderChangePhase::Requested);
        assert_eq!(
            pending.cancel.ok_or("cancel lost on reopen")?.by,
            actor("admin-canceller")?
        );
        host.advance();
        let engine = engine(store.clone(), host.clone())?;
        assert_eq!(engine.drive_due().await?.completed, vec![admitted.id()]);
        let ended = view(&store, id).await?;
        assert_eq!(ended.phase, HolderChangePhase::Ended);
        assert_eq!(
            ended.outcome,
            Some(HolderChangeAnswer::Cancelled {
                by: actor("admin-canceller")?
            })
        );
        assert_eq!(
            ended.history.last().ok_or("ended time")?.entered_at,
            SyncTime::from_unix_millis(1_001_000)
        );
        assert!(ended.waiting.is_none() && !ended.settlement_owed);
        assert!(host.calls.lock().await.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn stored_phase_history_rejects_regression_overflow_and_outcome_mismatch() -> TestResult {
    for corruption in ["regression", "overflow", "outcome"] {
        let directory = tempfile::tempdir()?;
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(&directory.path().join("registry.db"), host.clone(), true).await?;
        let engine = engine(store.clone(), host.clone())?;
        crate::turso_change_tests::register_first(&engine).await?;
        host.lose_release.store(true, Ordering::SeqCst);
        let id = attempt(2903)?;
        engine
            .start::<HolderChangeTask<TestStore, Turso>>(
                &holder_change_request_key(id)?,
                move_request(id)?,
            )
            .await?;
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    use durable_steps::storage::TransactionScope;
                    let mut record = crate::registry_storage::RegistryStorage(scope)
                        .transfer(id)
                        .await
                        .map_err(storage)?
                        .ok_or_else(|| storage("transfer missing"))?;
                    match corruption {
                        "regression" => record.phase_history.push(PhaseEntry {
                            phase: HolderChangePhase::Requested,
                            entered_at: SyncTime::from_unix_millis(1001),
                        }),
                        "overflow" => {
                            record.phase_history = vec![
                                PhaseEntry {
                                    phase: HolderChangePhase::Releasing,
                                    entered_at: SyncTime::from_unix_millis(1000)
                                };
                                9
                            ]
                        }
                        _ => {
                            record.outcome = Some(HolderChangeAnswer::Granted {
                                epoch: Epoch::new(2).map_err(storage)?,
                                expires_at: SyncTime::from_unix_millis(3000),
                                start: ReplicationPosition::new(
                                    Epoch::new(1).map_err(storage)?,
                                    CommitSequence::new(0),
                                ),
                            })
                        }
                    }
                    sqlx_core::query::query::<Turso>(
                        "UPDATE _fleet_sync_transfers SET phase_history=?,outcome=?",
                    )
                    .bind(serde_json::to_string(&record.phase_history).map_err(storage)?)
                    .bind(
                        record
                            .outcome
                            .map(|value| serde_json::to_string(&value))
                            .transpose()
                            .map_err(storage)?,
                    )
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
            matches!(error,Some(LeaseError::Corrupt{field}) if field=="holder-change phase history")
        );
    }
    Ok(())
}

#[tokio::test]
async fn admin_read_rejects_an_admission_bound_to_another_operation() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&directory.path().join("registry.db"), host.clone(), true).await?;
    let engine = engine(store.clone(), host.clone())?;
    crate::turso_change_tests::register_first(&engine).await?;
    let id = attempt(2904)?;
    let offered = move_request(id)?;
    store.transaction(move|scope|Box::pin(async move{
        admit_holder_change::<TestStore,Turso>(scope,&offered,&ActorRef::new("requester").map_err(storage)?,SyncTime::from_unix_millis(1000)).await.map_err(storage)?;
        use durable_steps::storage::TransactionScope;
        use sqlx_core::row::Row;
        let other=sqlx_core::query::query::<Turso>("SELECT operation_id FROM _fleet_sync_operations WHERE kind='fleet-sync.register'").fetch_one(scope.executor()).await?;
        let other=other.try_get::<String,_>("operation_id")?;
        sqlx_core::query::query::<Turso>("UPDATE _fleet_sync_transfer_admissions SET operation=?").bind(serde_json::to_string(&other).map_err(storage)?).execute(scope.executor()).await?;
        Ok(())
    })).await?;
    let error = store
        .transaction(move |scope| {
            Box::pin(async move { Ok(holder_change_view(scope, id).await.err()) })
        })
        .await?;
    assert!(
        matches!(error,Some(LeaseError::Corrupt{field}) if field=="holder-change operation binding")
    );
    Ok(())
}
