//! Retirement's seal and copy marker survive failed tasks and lost acknowledgements.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_change_tests::register_first, turso_copy_fixture::seed_import, turso_registry_fixture::*,
    *,
};
use durable_steps::{AtomicStore, OperationState};
use fleet_sync_protocol::*;
use sqlx_core::row::Row;
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

async fn imported(engine: &TestEngine, host: &Arc<TestHost>) -> TestResult<Retirement> {
    let import = import_request(201, "a")?;
    assert_eq!(
        task_answer::<ImportTask<TestStore, Turso>>(
            engine,
            &lifecycle_request_key(import.attempt)?,
            import
        )
        .await?,
        ImportAnswer::Imported
    );
    let position = seed_import(host).await?;
    Ok(Retirement {
        attempt: import_request(202, "a")?.attempt,
        database: database()?,
        expect_fence: Epoch::new(1)?,
        expect_acked: position,
    })
}
async fn lifecycle(store: &TestStore) -> TestResult<DatabaseLifecycle> {
    let database = database()?;
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move { database_lifecycle(scope, database).await.map_err(storage) })
        })
        .await?
        .ok_or("missing lifecycle")?)
}
async fn marker(host: &TestHost) -> TestResult<String> {
    let mut copies = host.copies.lock().await;
    let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
    Ok(
        sqlx_core::query::query::<Turso>("SELECT record FROM _fleet_sync_copy")
            .fetch_one(&mut *copy)
            .await?
            .try_get("record")?,
    )
}

#[tokio::test]
async fn failed_retirement_keeps_its_seal_and_a_new_attempt_finishes_the_real_copy() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("registry.db");
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&path, Arc::clone(&host), true).await?;
    let running = engine(Arc::clone(&store), Arc::clone(&host))?;
    let request = imported(&running, &host).await?;
    host.retire_unreachable.store(true, Ordering::SeqCst);
    let id = running
        .start::<RetirementTask<TestStore, Turso>>(
            &lifecycle_request_key(request.attempt)?,
            request.clone(),
        )
        .await?;
    assert!(matches!(
        running.status(&id).await?.state,
        OperationState::Running { .. }
    ));
    assert_eq!(
        lifecycle(&store).await?,
        DatabaseLifecycle::Retiring {
            attempt: request.attempt
        }
    );
    drop(running);
    drop(store);
    host.advance();
    let store = open_store(&path, Arc::clone(&host), false).await?;
    let mut changed = TestEngine::new(Arc::clone(&store), Arc::clone(&host));
    changed.register(VersionedTask::<_, 2>(
        RetirementTask::<TestStore, Turso>::new(config()?),
    ))?;
    assert_eq!(changed.drive_due().await?.failed.len(), 1);
    assert_eq!(
        lifecycle(&store).await?,
        DatabaseLifecycle::Retiring {
            attempt: request.attempt
        }
    );
    host.retire_unreachable.store(false, Ordering::SeqCst);
    let resumed = engine(Arc::clone(&store), Arc::clone(&host))?;
    let mut next = request;
    next.attempt = import_request(203, "a")?.attempt;
    assert!(matches!(
        task_answer::<RetirementTask<TestStore, Turso>>(
            &resumed,
            &lifecycle_request_key(next.attempt)?,
            next
        )
        .await?,
        RetirementAnswer::Retired { .. }
    ));
    assert!(matches!(
        lifecycle(&store).await?,
        DatabaseLifecycle::Retired { .. }
    ));
    let mut copies = host.copies.lock().await;
    let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
    assert_eq!(
        fleet_sync_receiver::copy_status(&mut CopyScope(copy)).await?,
        CopyStatusAnswer::Retired
    );
    let row =
        sqlx_core::query::query::<Turso>("SELECT CAST(count(*) AS TEXT) AS rows FROM records")
            .fetch_one(&mut *copy)
            .await?;
    assert_eq!(row.try_get::<String, _>("rows")?, "0");
    Ok(())
}

#[tokio::test]
async fn lost_copy_retirement_answer_replays_the_same_call_without_rewriting_its_marker()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("registry.db");
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&path, Arc::clone(&host), true).await?;
    let running = engine(Arc::clone(&store), Arc::clone(&host))?;
    let request = imported(&running, &host).await?;
    host.lose_retire.store(true, Ordering::SeqCst);
    let key = lifecycle_request_key(request.attempt)?;
    let id = running
        .start::<RetirementTask<TestStore, Turso>>(&key, request.clone())
        .await?;
    assert!(matches!(
        running.status(&id).await?.state,
        OperationState::Running { .. }
    ));
    let before = marker(&host).await?;
    drop(running);
    drop(store);
    host.advance();
    let store = open_store(&path, Arc::clone(&host), false).await?;
    let resumed = engine(Arc::clone(&store), Arc::clone(&host))?;
    assert_eq!(resumed.drive_due().await?.completed, vec![id]);
    assert!(matches!(
        task_answer::<RetirementTask<TestStore, Turso>>(&resumed, &key, request).await?,
        RetirementAnswer::Retired { .. }
    ));
    assert_eq!(before, marker(&host).await?);
    let calls = host
        .calls
        .lock()
        .await
        .iter()
        .filter(|(kind, _)| kind == "fleet-sync.copy-retire")
        .map(|(_, call)| call.clone())
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls.first(), calls.last());
    Ok(())
}

#[tokio::test]
async fn an_expired_lease_still_prevents_retirement() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    register_first(&engine).await?;
    host.advance();
    let request = Retirement {
        attempt: import_request(204, "a")?.attempt,
        database: database()?,
        expect_fence: Epoch::new(1)?,
        expect_acked: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0)),
    };
    assert_eq!(
        task_answer::<RetirementTask<TestStore, Turso>>(
            &engine,
            &lifecycle_request_key(request.attempt)?,
            request
        )
        .await?,
        RetirementAnswer::Refused(RetirementRefusal::LeaseHeld)
    );
    assert!(matches!(
        lifecycle(&store).await?,
        DatabaseLifecycle::Registered { .. }
    ));
    assert!(
        host.calls
            .lock()
            .await
            .iter()
            .all(|(kind, _)| kind != "fleet-sync.copy-retire"
                && kind != "fleet-sync.retirement-admission")
    );
    Ok(())
}
