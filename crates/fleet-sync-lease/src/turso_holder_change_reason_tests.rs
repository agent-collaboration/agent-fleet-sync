//! Reasons survive real admission, execution, replay and registry reopen.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_epoch_history_fixture::{records, registered},
    turso_registry_fixture::*,
    turso_registry_snapshot_fixture::required_snapshot,
    turso_workflow_tests::{attempt, view},
    *,
};
use durable_steps::{Admitted, AtomicStore, RequestKey, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_core::row::Row;
use sqlx_turso::Turso;
use std::sync::Arc;

async fn admit(
    store: &TestStore,
    request: HolderChangeRequest,
) -> TestResult<Result<Admitted, LeaseError>> {
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move {
                // Commit even a refusal, so rollback cannot hide an unintended write.
                Ok(admit_holder_change::<TestStore, Turso>(
                    scope,
                    &request,
                    &ActorRef::new("reason-requester").map_err(storage)?,
                    SyncTime::from_unix_millis(1000),
                )
                .await)
            })
        })
        .await?)
}

async fn admission_and_journal_rows(store: &TestStore) -> TestResult<Vec<String>> {
    Ok(store.transaction(|scope| Box::pin(async move {
        sqlx_core::query::query::<Turso>(
            "SELECT 'admission:' || json_array(attempt,operation,request,requested_by,admitted_at,follow_up,target_schema) AS value FROM _fleet_sync_transfer_admissions
             UNION ALL SELECT 'operation:' || json_array(operation_id,kind,kind_version,request_key,hex(input),hex(input_digest),state,hex(result),failure,finished_at,next_attempt_at,attempt,drive_token,drive_until) FROM _fleet_sync_operations
             UNION ALL SELECT 'step:' || json_array(operation_id,position,step_name,kind,intent,hex(output),first_attempt_at,may_have_reached) FROM _fleet_sync_steps
             UNION ALL SELECT 'database:' || json_array(database,record) FROM _fleet_sync_databases
             UNION ALL SELECT 'cancel:' || json_array(attempt,record) FROM _fleet_sync_transfer_cancels
             UNION ALL SELECT 'lifecycle:' || json_array(attempt,database,intent,outcome) FROM _fleet_sync_lifecycle_attempts
             UNION ALL SELECT 'preview:' || json_array(token,database,record) FROM _fleet_sync_takeover_previews
             UNION ALL SELECT 'renewal:' || json_array(database,attempt,record) FROM _fleet_sync_renewal_attempts
             UNION ALL SELECT 'support:' || json_array(peer,record) FROM _fleet_sync_schema_support
             ORDER BY value"
        ).fetch_all(scope.executor()).await?.into_iter()
            .map(|row| row.try_get("value").map_err(storage)).collect()
    })).await?)
}

fn replace_reason(request: &mut HolderChangeRequest, value: &str) -> TestResult {
    match request {
        HolderChangeRequest::Move { reason, .. } | HolderChangeRequest::Takeover { reason, .. } => {
            *reason = HolderChangeReason::new(value)?;
        }
        HolderChangeRequest::Claim { .. } => return Err("claims have no reason".into()),
    }
    Ok(())
}

fn recorded_reason(request: &HolderChangeRequest) -> TestResult<&str> {
    match request {
        HolderChangeRequest::Move { reason, .. } | HolderChangeRequest::Takeover { reason, .. } => {
            Ok(reason.as_str())
        }
        HolderChangeRequest::Claim { .. } => Err("claims have no reason".into()),
    }
}

async fn changed_reason_has_no_effect(
    store: &TestStore,
    request: &HolderChangeRequest,
    host: &TestHost,
) -> TestResult {
    let before = admission_and_journal_rows(store).await?;
    let authority = records(store).await?;
    let snapshot = required_snapshot(store).await?;
    let calls = host.calls.lock().await.len();
    let mut changed = request.clone();
    replace_reason(&mut changed, "a different reason under the same attempt")?;
    assert!(matches!(
        admit(store, changed).await?,
        Err(LeaseError::InvalidInput { field }) if field.starts_with("request key has different content;")
    ));
    assert_eq!(admission_and_journal_rows(store).await?, before);
    assert_eq!(records(store).await?, authority);
    assert_eq!(required_snapshot(store).await?, snapshot);
    assert_eq!(host.calls.lock().await.len(), calls);
    Ok(())
}

#[tokio::test]
async fn move_and_takeover_reasons_survive_reopen_and_bind_replay_content() -> TestResult {
    for takeover in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("registry.db");
        let (host, store, running) = registered(directory.path()).await?;
        let id = attempt(4301)?;
        let literal_reason = if takeover {
            "  recover the offline holder: déplacer 🐒\n"
        } else {
            "  planned maintenance: déplacer 🐒\n"
        };
        let request = if takeover {
            let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
                &running,
                &RequestKey::new("reason-preview")?,
                PreviewTakeover {
                    database: database()?,
                },
            )
            .await?;
            HolderChangeRequest::Takeover {
                attempt: id,
                database: database()?,
                preview,
                to: peer(2)?,
                reason: HolderChangeReason::new(literal_reason)?,
            }
        } else {
            HolderChangeRequest::Move {
                attempt: id,
                database: database()?,
                from_epoch: Epoch::new(1)?,
                to: peer(2)?,
                reason: HolderChangeReason::new(literal_reason)?,
            }
        };
        let snapshot = required_snapshot(&store).await?;
        let operation = admit(&store, request.clone()).await??;
        assert_eq!(required_snapshot(&store).await?, snapshot);
        assert_eq!(
            recorded_reason(&view(&store, id).await?.request)?,
            literal_reason
        );
        changed_reason_has_no_effect(&store, &request, &host).await?;
        drop(running);
        drop(store);

        let reopened = open_store(&path, Arc::clone(&host), false).await?;
        let resumed = engine(Arc::clone(&reopened), Arc::clone(&host))?;
        assert_eq!(
            recorded_reason(&view(&reopened, id).await?.request)?,
            literal_reason
        );
        assert!(
            resumed
                .drive_due()
                .await?
                .completed
                .contains(&operation.id())
        );
        let completed = view(&reopened, id).await?;
        let answer = completed.outcome.ok_or("holder change did not end")?;
        assert!(matches!(answer, HolderChangeAnswer::Granted { epoch, .. } if epoch.value() == 2));
        assert_eq!(recorded_reason(&completed.request)?, literal_reason);
        changed_reason_has_no_effect(&reopened, &request, &host).await?;
        let calls = host.calls.lock().await.len();
        assert_eq!(
            task_answer::<HolderChangeTask<TestStore, Turso>>(
                &resumed,
                &holder_change_request_key(id)?,
                request,
            )
            .await?,
            answer
        );
        assert_eq!(host.calls.lock().await.len(), calls);
        drop(resumed);
        drop(reopened);

        let reopened = open_store(&path, host, false).await?;
        let recorded = view(&reopened, id).await?;
        assert_eq!(recorded_reason(&recorded.request)?, literal_reason);
        assert_eq!(recorded.outcome, Some(answer));
    }
    Ok(())
}

#[tokio::test]
async fn invalid_reason_wire_never_reaches_admission_or_changes_registry_state() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (host, store, running) = registered(directory.path()).await?;
    let request = HolderChangeRequest::Move {
        attempt: attempt(4302)?,
        database: database()?,
        from_epoch: Epoch::new(1)?,
        to: peer(2)?,
        reason: HolderChangeReason::new("valid before wire corruption")?,
    };
    let wire = serde_json::to_value(request)?;
    let before = admission_and_journal_rows(&store).await?;
    let authority = records(&store).await?;
    let calls = host.calls.lock().await.len();
    for invalid in [String::new(), "x".repeat(16 * 1024 + 1), "é".repeat(8193)] {
        let mut invalid_wire = wire.clone();
        invalid_wire["value"]["reason"] = serde_json::Value::String(invalid);
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    let decoded = serde_json::from_value::<HolderChangeRequest>(invalid_wire);
                    assert!(
                        decoded.is_err(),
                        "invalid reason passed the typed admission boundary"
                    );
                    if let Ok(request) = decoded {
                        admit_holder_change::<TestStore, Turso>(
                            scope,
                            &request,
                            &ActorRef::new("invalid-wire-requester").map_err(storage)?,
                            SyncTime::from_unix_millis(1000),
                        )
                        .await
                        .map_err(storage)?;
                    }
                    Ok(())
                })
            })
            .await?;
        assert_eq!(admission_and_journal_rows(&store).await?, before);
        assert_eq!(records(&store).await?, authority);
        assert_eq!(host.calls.lock().await.len(), calls);
    }
    drop(running);
    Ok(())
}

#[tokio::test]
async fn machine_claim_record_has_no_reason_after_real_grant_and_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("registry.db");
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&path, Arc::clone(&host), true).await?;
    let running = engine(Arc::clone(&store), Arc::clone(&host))?;
    let import = import_request(4303, "a")?;
    assert_eq!(
        task_answer::<ImportTask<TestStore, Turso>>(
            &running,
            &lifecycle_request_key(import.attempt)?,
            import,
        )
        .await?,
        ImportAnswer::Imported
    );
    crate::turso_copy_fixture::seed_import(&host).await?;
    let id = attempt(4304)?;
    let answer = task_answer::<HolderChangeTask<TestStore, Turso>>(
        &running,
        &holder_change_request_key(id)?,
        HolderChangeRequest::Claim {
            attempt: id,
            database: database()?,
            claimant: peer(2)?,
        },
    )
    .await?;
    assert!(matches!(answer, HolderChangeAnswer::Granted { epoch, .. } if epoch.value() == 2));
    drop(running);
    drop(store);
    let reopened = open_store(&path, host, false).await?;
    let recorded = view(&reopened, id).await?;
    assert!(matches!(
        recorded.request,
        HolderChangeRequest::Claim { .. }
    ));
    let wire = serde_json::to_value(recorded.request)?;
    assert_eq!(wire["kind"], "claim");
    assert!(wire["value"].get("reason").is_none());
    assert_eq!(recorded.outcome, Some(answer));
    Ok(())
}
