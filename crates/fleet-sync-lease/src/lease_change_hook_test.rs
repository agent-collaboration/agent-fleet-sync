#![allow(clippy::panic_in_result_fn)]
use super::*;
use crate::{turso_registry_fixture::*, *};
use durable_steps::{AtomicStore, storage::TransactionScope};
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct ProjectionHook {
    fail_after_write: AtomicBool,
    identities: Arc<TestHost>,
}
impl<S: TransactionScope<Database = Turso>> LeaseChangeHook<S> for ProjectionHook {
    async fn on_lease_change(&self, scope: &mut S, change: &LeaseChange) -> Result<(), LeaseError> {
        if registry_snapshot(scope, change.after.database)
            .await?
            .as_ref()
            != Some(&change.after)
        {
            return Err(LeaseError::Corrupt {
                field: "hook ran before consistent lease rows".into(),
            });
        }
        let before = change
            .before
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| LeaseError::Storage {
                message: error.to_string(),
            })?;
        let after = serde_json::to_string(&change.after).map_err(|error| LeaseError::Storage {
            message: error.to_string(),
        })?;
        query::<Turso>("INSERT INTO test_lease_projection(id,created_at,updated_at,project_id,epoch,revision,before_snapshot,after_snapshot) VALUES(?1,CAST(?2 AS INTEGER),CAST(?2 AS INTEGER),?3,CAST(?4 AS INTEGER),CAST(?5 AS INTEGER),?6,?7)")
            .bind(runtime_primitives::IdSource::new_id(self.identities.as_ref()).to_string()).bind(change.after.changed_at.unix_millis().to_string()).bind(change.after.database.as_uuid().to_string()).bind(change.after.revision.epoch.value().to_string())
            .bind(change.after.revision.change.get().to_string()).bind(before).bind(after).execute(scope.executor()).await?;
        if self.fail_after_write.load(Ordering::SeqCst) {
            return Err(LeaseError::Storage {
                message: "injected projection failure".into(),
            });
        }
        Ok(())
    }
}

async fn fixture() -> TestResult<(
    tempfile::TempDir,
    Arc<TestHost>,
    Arc<TestStore>,
    Arc<ProjectionHook>,
)> {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), true).await?;
    let store = open_store(&directory.path().join("registry.db"), host.clone(), true).await?;
    store.transaction(|scope| Box::pin(async move {
        query::<Turso>("CREATE TABLE test_lease_projection(id TEXT NOT NULL PRIMARY KEY,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,project_id TEXT NOT NULL,epoch INTEGER NOT NULL,revision INTEGER NOT NULL,before_snapshot TEXT,after_snapshot TEXT NOT NULL,UNIQUE(project_id,epoch,revision))")
            .execute(scope.executor()).await?;
        Ok(())
    })).await?;
    Ok((
        directory,
        host.clone(),
        store,
        Arc::new(ProjectionHook {
            fail_after_write: AtomicBool::new(false),
            identities: host.clone(),
        }),
    ))
}

fn projection_engine(
    store: Arc<TestStore>,
    host: Arc<TestHost>,
    hook: Arc<ProjectionHook>,
) -> TestResult<TestEngine> {
    let mut engine = TestEngine::new(store, host.clone());
    engine.register(RegistrationTask::<TestStore, Turso, ProjectionHook>::new(
        config()?,
        host.clone(),
        hook.clone(),
    ))?;
    engine.register(ImportTask::<TestStore, Turso, ProjectionHook>::new(
        config()?,
        host.clone(),
        hook.clone(),
    ))?;
    engine.register(RetirementTask::<TestStore, Turso, ProjectionHook>::new(
        config()?,
        host.clone(),
        hook.clone(),
    ))?;
    engine.register(HolderChangeTask::<TestStore, Turso, ProjectionHook>::new(
        config()?,
        host,
        hook,
    ))?;
    engine.register(SettleReservationTask::<TestStore>::new())?;
    Ok(engine)
}

async fn changes(store: &TestStore) -> TestResult<Vec<LeaseChange>> {
    Ok(store.transaction(|scope| Box::pin(async move {
        let rows = query::<Turso>("SELECT before_snapshot,after_snapshot FROM test_lease_projection ORDER BY epoch,revision")
            .fetch_all(scope.executor()).await?;
        rows.into_iter().map(|row| {
            let before:Option<String> = row.try_get("before_snapshot")?;
            let after:String = row.try_get("after_snapshot")?;
            Ok(LeaseChange {before:before.map(|value| serde_json::from_str(&value)).transpose().map_err(storage)?,after:serde_json::from_str(&after).map_err(storage)?})
        }).collect::<Result<Vec<_>,durable_steps::StoreError>>()
    })).await?)
}

#[tokio::test]
async fn registration_projects_a_consistent_snapshot_once_and_replays_write_nothing() -> TestResult
{
    let (_directory, host, store, hook) = fixture().await?;
    let engine = projection_engine(store.clone(), host, hook)?;
    let request = registration(201, 1)?;
    let key = lifecycle_request_key(request.attempt)?;
    let first = task_answer::<RegistrationTask<TestStore, Turso, ProjectionHook>>(
        &engine,
        &key,
        request.clone(),
    )
    .await?;
    let observed = changes(&store).await?;
    assert_eq!(
        observed.len(),
        1,
        "registration did not invoke its transactional projection"
    );
    let change = observed.first().ok_or("no projection")?;
    assert!(change.before.is_none());
    assert_eq!(change.after.database, database()?);
    assert_eq!(change.after.revision.epoch, Epoch::new(1)?);
    assert_eq!(change.after.revision.change.get(), 1);
    assert_eq!(change.after.changed_at, SyncTime::from_unix_millis(1000));
    let grant = change.after.grant.as_ref().ok_or("missing grant")?;
    assert_eq!(grant.holder, peer(1)?);
    assert_eq!(grant.expires_at, SyncTime::from_unix_millis(3000));
    assert_eq!(
        task_answer::<RegistrationTask<TestStore, Turso, ProjectionHook>>(&engine, &key, request)
            .await?,
        first
    );
    let fresh_request = registration(202, 1)?;
    task_answer::<RegistrationTask<TestStore, Turso, ProjectionHook>>(
        &engine,
        &lifecycle_request_key(fresh_request.attempt)?,
        fresh_request,
    )
    .await?;
    assert_eq!(changes(&store).await?, observed);
    Ok(())
}

async fn snapshot(store: &TestStore) -> TestResult<Option<RegistrySnapshot>> {
    Ok(store
        .transaction(|scope| {
            Box::pin(async move {
                registry_snapshot(scope, database().map_err(storage)?)
                    .await
                    .map_err(storage)
            })
        })
        .await?)
}
async fn register_once(engine: &TestEngine) -> TestResult {
    let request = registration(201, 1)?;
    task_answer::<RegistrationTask<TestStore, Turso, ProjectionHook>>(
        engine,
        &lifecycle_request_key(request.attempt)?,
        request,
    )
    .await?;
    Ok(())
}
async fn renew_at(
    store: &TestStore,
    hook: Arc<ProjectionHook>,
    attempt: u128,
    milliseconds: i64,
) -> TestResult<RenewAnswer> {
    let request = fleet_sync_protocol::RenewRequest {
        attempt: import_request(attempt, "a")?.attempt,
        database: database()?,
        epoch: Epoch::new(1)?,
    };
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move {
                renew(
                    scope,
                    &request,
                    &peer(1).map_err(storage)?,
                    SyncTime::from_unix_millis(milliseconds),
                    &config().map_err(storage)?.lease,
                    hook.as_ref(),
                )
                .await
                .map_err(storage)
            })
        })
        .await?)
}

#[tokio::test]
async fn failing_registration_projection_rolls_back_grant_history_floor_and_hook_writes()
-> TestResult {
    let (_directory, host, store, hook) = fixture().await?;
    hook.fail_after_write.store(true, Ordering::SeqCst);
    let engine = projection_engine(store.clone(), host.clone(), hook.clone())?;
    let request = registration(201, 1)?;
    let key = lifecycle_request_key(request.attempt)?;
    let operation = engine
        .start::<RegistrationTask<TestStore, Turso, ProjectionHook>>(&key, request.clone())
        .await?;
    assert!(matches!(
        engine.status(&operation).await?.state,
        durable_steps::OperationState::Running { .. }
    ));
    assert_eq!(snapshot(&store).await?, None);
    assert!(changes(&store).await?.is_empty());
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_floor"
        )
        .await?,
        "0"
    );
    hook.fail_after_write.store(false, Ordering::SeqCst);
    host.advance();
    assert_eq!(engine.drive_due().await?.completed.len(), 1);
    assert_eq!(changes(&store).await?.len(), 1);
    let current = snapshot(&store).await?.ok_or("missing lease after retry")?;
    assert_eq!(current.revision.change.get(), 1);
    assert_eq!(
        current.grant.ok_or("missing grant after retry")?.expires_at,
        SyncTime::from_unix_millis(1_003_000)
    );
    task_answer::<RegistrationTask<TestStore, Turso, ProjectionHook>>(&engine, &key, request)
        .await?;
    assert_eq!(changes(&store).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn renewal_reports_same_holder_revision_and_expiry_with_replay_noop_and_atomic_failure()
-> TestResult {
    let (_directory, host, store, hook) = fixture().await?;
    let engine = projection_engine(store.clone(), host, hook.clone())?;
    register_once(&engine).await?;
    assert!(matches!(
        renew_at(&store, hook.clone(), 301, 1500).await?,
        RenewAnswer::Renewed {
            replayed: false,
            ..
        }
    ));
    let observed = changes(&store).await?;
    assert_eq!(observed.len(), 2);
    let changed = observed.last().ok_or("no renewal projection")?;
    let before = changed.before.as_ref().ok_or("renewal has no before")?;
    assert_eq!(before.revision.change.get(), 1);
    assert_eq!(
        before.grant.as_ref().ok_or("before grant")?.expires_at,
        SyncTime::from_unix_millis(3000)
    );
    assert_eq!(changed.after.revision.epoch, Epoch::new(1)?);
    assert_eq!(changed.after.revision.change.get(), 2);
    assert_eq!(changed.after.changed_at, SyncTime::from_unix_millis(1500));
    let grant = changed.after.grant.as_ref().ok_or("after grant")?;
    assert_eq!(grant.holder, peer(1)?);
    assert_eq!(grant.expires_at, SyncTime::from_unix_millis(3500));
    renew_at(&store, hook.clone(), 302, 1500).await?;
    assert!(matches!(
        renew_at(&store, hook.clone(), 301, 1700).await?,
        RenewAnswer::Renewed { replayed: true, .. }
    ));
    assert_eq!(changes(&store).await?, observed);
    hook.fail_after_write.store(true, Ordering::SeqCst);
    let failed = renew_at(&store, hook.clone(), 303, 1800).await;
    assert!(
        failed
            .as_ref()
            .is_err_and(|error| error.to_string().contains("injected projection failure"))
    );
    assert_eq!(snapshot(&store).await?, Some(changed.after.clone()));
    assert_eq!(changes(&store).await?, observed);
    hook.fail_after_write.store(false, Ordering::SeqCst);
    assert!(matches!(
        renew_at(&store, hook, 303, 1800).await?,
        RenewAnswer::Renewed {
            replayed: false,
            ..
        }
    ));
    let final_changes = changes(&store).await?;
    assert_eq!(final_changes.len(), 3);
    let final_change = final_changes.last().ok_or("final renewal")?;
    assert_eq!(final_change.after.revision.change.get(), 3);
    assert_eq!(
        final_change
            .after
            .grant
            .as_ref()
            .ok_or("final grant")?
            .expires_at,
        SyncTime::from_unix_millis(3800)
    );
    Ok(())
}

fn retirement_request() -> TestResult<fleet_sync_protocol::Retirement> {
    Ok(fleet_sync_protocol::Retirement {
        attempt: import_request(501, "a")?.attempt,
        database: database()?,
        expect_fence: Epoch::new(1)?,
        expect_acked: fleet_sync_protocol::ReplicationPosition::new(
            Epoch::new(1)?,
            fleet_sync_protocol::CommitSequence::new(0),
        ),
    })
}
async fn import_once(engine: &TestEngine) -> TestResult {
    let request = import_request(401, "a")?;
    assert_eq!(
        task_answer::<ImportTask<TestStore, Turso, ProjectionHook>>(
            engine,
            &lifecycle_request_key(request.attempt)?,
            request
        )
        .await?,
        fleet_sync_protocol::ImportAnswer::Imported
    );
    Ok(())
}

#[tokio::test]
async fn import_and_both_retirement_lifecycle_commits_are_projected_without_replay_writes()
-> TestResult {
    let (_directory, host, store, hook) = fixture().await?;
    let engine = projection_engine(store.clone(), host.clone(), hook)?;
    import_once(&engine).await?;
    let imported = changes(&store).await?;
    assert_eq!(imported.len(), 1);
    let first = imported.first().ok_or("import projection")?;
    assert!(first.before.is_none());
    assert!(matches!(
        first.after.lifecycle,
        DatabaseLifecycle::Unheld { .. }
    ));
    assert!(first.after.grant.is_none());
    assert_eq!(first.after.revision.change.get(), 1);
    import_once(&engine).await?;
    let repeated = import_request(402, "a")?;
    assert_eq!(
        task_answer::<ImportTask<TestStore, Turso, ProjectionHook>>(
            &engine,
            &lifecycle_request_key(repeated.attempt)?,
            repeated
        )
        .await?,
        fleet_sync_protocol::ImportAnswer::AlreadyImported
    );
    assert_eq!(changes(&store).await?, imported);
    host.now.store(1200, Ordering::SeqCst);
    let request = retirement_request()?;
    let key = lifecycle_request_key(request.attempt)?;
    assert!(
        matches!(task_answer::<RetirementTask<TestStore,Turso,ProjectionHook>>(&engine,&key,request.clone()).await?,fleet_sync_protocol::RetirementAnswer::Retired {at} if at==SyncTime::from_unix_millis(1200))
    );
    let observed = changes(&store).await?;
    assert_eq!(observed.len(), 3);
    let retiring = observed.get(1).ok_or("retiring projection")?;
    let retired = observed.last().ok_or("retired projection")?;
    assert_eq!(retiring.before, Some(first.after.clone()));
    assert!(matches!(
        retiring.after.lifecycle,
        DatabaseLifecycle::Retiring { .. }
    ));
    assert_eq!(retiring.after.revision.change.get(), 2);
    assert_eq!(retired.before, Some(retiring.after.clone()));
    assert!(
        matches!(retired.after.lifecycle,DatabaseLifecycle::Retired {at} if at==SyncTime::from_unix_millis(1200))
    );
    assert_eq!(retired.after.revision.change.get(), 3);
    task_answer::<RetirementTask<TestStore, Turso, ProjectionHook>>(&engine, &key, request).await?;
    assert_eq!(changes(&store).await?, observed);
    Ok(())
}

#[tokio::test]
async fn lifecycle_projection_failure_preserves_unheld_before_copy_retirement_and_recovers_once()
-> TestResult {
    let (_directory, host, store, hook) = fixture().await?;
    let engine = projection_engine(store.clone(), host.clone(), hook.clone())?;
    import_once(&engine).await?;
    let before = snapshot(&store).await?;
    let before_changes = changes(&store).await?;
    hook.fail_after_write.store(true, Ordering::SeqCst);
    let request = retirement_request()?;
    let operation = engine
        .start::<RetirementTask<TestStore, Turso, ProjectionHook>>(
            &lifecycle_request_key(request.attempt)?,
            request,
        )
        .await?;
    assert!(matches!(
        engine.status(&operation).await?.state,
        durable_steps::OperationState::Running { .. }
    ));
    assert_eq!(snapshot(&store).await?, before);
    assert_eq!(changes(&store).await?, before_changes);
    assert!(
        !host
            .calls
            .lock()
            .await
            .iter()
            .any(|(kind, _)| kind == "fleet-sync.copy-retire")
    );
    hook.fail_after_write.store(false, Ordering::SeqCst);
    host.advance();
    assert_eq!(engine.drive_due().await?.completed.len(), 1);
    assert_eq!(changes(&store).await?.len(), 3);
    Ok(())
}

#[tokio::test]
async fn import_projection_failure_rolls_back_unheld_identity_and_floor_then_retries() -> TestResult
{
    let (_directory, host, store, hook) = fixture().await?;
    hook.fail_after_write.store(true, Ordering::SeqCst);
    let engine = projection_engine(store.clone(), host.clone(), hook.clone())?;
    let request = import_request(401, "a")?;
    let operation = engine
        .start::<ImportTask<TestStore, Turso, ProjectionHook>>(
            &lifecycle_request_key(request.attempt)?,
            request,
        )
        .await?;
    assert!(matches!(
        engine.status(&operation).await?.state,
        durable_steps::OperationState::Running { .. }
    ));
    assert_eq!(snapshot(&store).await?, None);
    assert!(changes(&store).await?.is_empty());
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_floor"
        )
        .await?,
        "0"
    );
    hook.fail_after_write.store(false, Ordering::SeqCst);
    host.advance();
    assert_eq!(engine.drive_due().await?.completed.len(), 1);
    assert_eq!(changes(&store).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn grant_projection_failure_rolls_back_holder_epoch_revision_and_history_until_retry()
-> TestResult {
    let (_directory, host, store, hook) = fixture().await?;
    let engine = projection_engine(store.clone(), host.clone(), hook.clone())?;
    register_once(&engine).await?;
    let before = snapshot(&store).await?;
    let before_changes = changes(&store).await?;
    hook.fail_after_write.store(true, Ordering::SeqCst);
    host.now.store(1500, Ordering::SeqCst);
    let attempt =
        fleet_sync_protocol::TransferId::new(import_request(601, "a")?.attempt.as_uuid())?;
    let request = fleet_sync_protocol::HolderChangeRequest::Move {
        attempt,
        database: database()?,
        from_epoch: Epoch::new(1)?,
        to: peer(2)?,
        reason: fleet_sync_protocol::HolderChangeReason::new("projection transaction")?,
    };
    let key = holder_change_request_key(attempt)?;
    let operation = engine
        .start::<HolderChangeTask<TestStore, Turso, ProjectionHook>>(&key, request.clone())
        .await?;
    assert!(matches!(
        engine.status(&operation).await?.state,
        durable_steps::OperationState::Running { .. }
    ));
    assert_eq!(snapshot(&store).await?, before);
    assert_eq!(changes(&store).await?, before_changes);
    hook.fail_after_write.store(false, Ordering::SeqCst);
    host.advance();
    assert_eq!(engine.drive_due().await?.completed.len(), 1);
    let observed = changes(&store).await?;
    assert_eq!(observed.len(), 2);
    let granted = observed.last().ok_or("grant projection")?;
    assert_eq!(granted.before, before);
    assert_eq!(granted.after.revision.epoch, Epoch::new(2)?);
    assert_eq!(granted.after.revision.change.get(), 1);
    let grant = granted.after.grant.as_ref().ok_or("new grant")?;
    assert_eq!(grant.holder, peer(2)?);
    assert_eq!(grant.epoch, Epoch::new(2)?);
    assert_eq!(grant.expires_at, SyncTime::from_unix_millis(1_003_500));
    task_answer::<HolderChangeTask<TestStore, Turso, ProjectionHook>>(&engine, &key, request)
        .await?;
    assert_eq!(changes(&store).await?, observed);
    Ok(())
}
