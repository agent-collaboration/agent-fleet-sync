//! Public registry tasks over the actual engine, journal, copy rules and consumer gates.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_registry_fixture::*, *};
use durable_steps::{AtomicStore, OperationState, Uuid};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

async fn import_entry(
    store: &TestStore,
    engine: &TestEngine,
    request: Import,
) -> TestResult<ImportAnswer> {
    let database = request.database;
    let lifecycle = store
        .transaction(move |scope| {
            Box::pin(async move { database_lifecycle(scope, database).await.map_err(storage) })
        })
        .await?;
    if matches!(
        lifecycle,
        Some(DatabaseLifecycle::Retiring { .. } | DatabaseLifecycle::Retired { .. })
    ) {
        return Ok(ImportAnswer::Retired);
    }
    task_answer::<ImportTask<TestStore, Turso>>(
        engine,
        &lifecycle_request_key(request.attempt)?,
        request,
    )
    .await
}

#[tokio::test]
async fn register_creates_copy_before_grant_and_replay_never_issues_another_epoch() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), true).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    let request = registration(201, 1)?;
    let key = lifecycle_request_key(request.attempt)?;
    let first =
        task_answer::<RegistrationTask<TestStore, Turso>>(&engine, &key, request.clone()).await?;
    assert!(
        matches!(first,RegistrationAnswer::Registered{lease:LeaseStatus{epoch,holder,..},..} if epoch==Epoch::new(1)?&&holder==peer(1)?)
    );
    let calls = host.calls.lock().await.len();
    assert_eq!(
        task_answer::<RegistrationTask<TestStore, Turso>>(&engine, &key, request).await?,
        first
    );
    assert_eq!(host.calls.lock().await.len(), calls);
    assert_eq!(
        scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        "1"
    );
    let mut copies = host.copies.lock().await;
    let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
    assert!(
        matches!(fleet_sync_receiver::copy_status(&mut CopyScope(copy)).await?,CopyStatusAnswer::Status(CopyStatus{holder:Some(current),fence,..}) if current==peer(1)?&&fence==Epoch::new(1)?)
    );
    Ok(())
}

#[tokio::test]
async fn failed_registration_leaves_no_grant_and_original_creation_prevents_a_different_writer()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("registry.db");
    let host = TestHost::open(directory.path(), true).await?;
    host.lose_create.store(true, Ordering::SeqCst);
    let store = open_store(&path, Arc::clone(&host), true).await?;
    let first_engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    let offered = registration(201, 1)?;
    let id = first_engine
        .start::<RegistrationTask<TestStore, Turso>>(
            &lifecycle_request_key(offered.attempt)?,
            offered.clone(),
        )
        .await?;
    assert!(matches!(
        first_engine.status(&id).await?.state,
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
    assert_eq!(host.copies.lock().await.len(), 1);
    drop(first_engine);
    drop(store);
    host.advance();
    let store = open_store(&path, Arc::clone(&host), false).await?;
    let mut mismatch = TestEngine::new(Arc::clone(&store), Arc::clone(&host));
    mismatch.register(VersionedTask::<_, 2>(
        RegistrationTask::<TestStore, Turso>::new(config()?),
    ))?;
    assert_eq!(mismatch.drive_due().await?.failed.len(), 1);
    assert!(matches!(
        mismatch.status(&id).await?.state,
        OperationState::Failed { .. }
    ));
    assert_eq!(scalar(&store,"SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_lifecycle_attempts WHERE outcome IS NULL").await?,"0");
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_leases"
        )
        .await?,
        "0"
    );
    let normal = engine(Arc::clone(&store), Arc::clone(&host))?;
    let changed = registration(202, 2)?;
    assert_eq!(
        task_answer::<RegistrationTask<TestStore, Turso>>(
            &normal,
            &lifecycle_request_key(changed.attempt)?,
            changed
        )
        .await?,
        RegistrationAnswer::Refused(RegistrationRefusal::NotWriter {
            current: Some(Epoch::new(1)?)
        })
    );
    let same = registration(203, 1)?;
    assert!(
        matches!(task_answer::<RegistrationTask<TestStore,Turso>>(&normal,&lifecycle_request_key(same.attempt)?,same).await?,RegistrationAnswer::Registered{lease:LeaseStatus{epoch,..},..} if epoch==Epoch::new(1)?)
    );
    Ok(())
}

#[tokio::test]
async fn refused_registration_changes_no_copy_or_authority_and_a_new_attempt_can_succeed()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), true).await?;
    host.gate.reject_registration.store(true, Ordering::SeqCst);
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    let offered = registration(201, 1)?;
    assert!(matches!(
        task_answer::<RegistrationTask<TestStore, Turso>>(
            &engine,
            &lifecycle_request_key(offered.attempt)?,
            offered
        )
        .await?,
        RegistrationAnswer::Refused(RegistrationRefusal::DatabaseRejected { .. })
    ));
    assert!(host.copies.lock().await.is_empty());
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_leases"
        )
        .await?,
        "0"
    );
    host.gate.reject_registration.store(false, Ordering::SeqCst);
    let offered = registration(202, 1)?;
    assert!(matches!(
        task_answer::<RegistrationTask<TestStore, Turso>>(
            &engine,
            &lifecycle_request_key(offered.attempt)?,
            offered
        )
        .await?,
        RegistrationAnswer::Registered { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn import_repeats_and_conflicts_then_retirement_precedes_cached_engine_answers() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    let original = import_request(201, "a")?;
    assert_eq!(
        import_entry(&store, &engine, original.clone()).await?,
        ImportAnswer::Imported
    );
    assert_eq!(
        import_entry(&store, &engine, import_request(202, "a")?).await?,
        ImportAnswer::AlreadyImported
    );
    assert_eq!(
        import_entry(&store, &engine, import_request(203, "b")?).await?,
        ImportAnswer::ImportConflict {
            existing: Some(original.digest.clone()),
            offered: ImportDigest::new("b".repeat(64))?
        }
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_leases"
        )
        .await?,
        "0"
    );
    host.gate.reject_retirement.store(true, Ordering::SeqCst);
    let expected = ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0));
    let request = Retirement {
        attempt: RequestId::new(Uuid::from_u128(0x019eef000000700080000000000000cc))?,
        database: database()?,
        expect_fence: Epoch::new(1)?,
        expect_acked: expected,
    };
    assert!(matches!(
        task_answer::<RetirementTask<TestStore, Turso>>(
            &engine,
            &lifecycle_request_key(request.attempt)?,
            request
        )
        .await?,
        RetirementAnswer::Refused(RetirementRefusal::GateRefused { .. })
    ));
    host.gate.reject_retirement.store(false, Ordering::SeqCst);
    let request = Retirement {
        attempt: RequestId::new(Uuid::from_u128(0x019eef000000700080000000000000cd))?,
        database: database()?,
        expect_fence: Epoch::new(1)?,
        expect_acked: expected,
    };
    assert!(matches!(
        task_answer::<RetirementTask<TestStore, Turso>>(
            &engine,
            &lifecycle_request_key(request.attempt)?,
            request
        )
        .await?,
        RetirementAnswer::Retired { .. }
    ));
    assert_eq!(
        import_entry(&store, &engine, original).await?,
        ImportAnswer::Retired
    );
    let mut copies = host.copies.lock().await;
    let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
    assert_eq!(
        fleet_sync_receiver::copy_status(&mut CopyScope(copy)).await?,
        CopyStatusAnswer::Retired
    );
    Ok(())
}
