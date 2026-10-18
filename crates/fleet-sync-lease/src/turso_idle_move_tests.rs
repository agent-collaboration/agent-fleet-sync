//! An idle holder releases its inherited position without inventing a commit in its epoch.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_change_tests::register_first,
    turso_copy_fixture::{push_rows, seed_import},
    turso_registry_fixture::*,
    *,
};
use durable_steps::{AtomicStore, OperationState, RequestKey, Uuid};
use fleet_sync_protocol::*;
use runtime_primitives::Clock;
use sqlx_core::row::Row;
use sqlx_turso::Turso;
use std::sync::Arc;

fn transfer(value: u128) -> TestResult<TransferId> {
    Ok(TransferId::new(Uuid::from_u128(
        (value) | 0x019eef00000070008000000000000000,
    ))?)
}
fn movement(id: u128, from: u64, to: u128) -> TestResult<HolderChangeRequest> {
    Ok(HolderChangeRequest::Move {
        attempt: transfer(id)?,
        database: database()?,
        from_epoch: Epoch::new(from)?,
        to: peer(to)?,
    })
}
async fn grant_and_replay(
    engine: &TestEngine,
    store: &TestStore,
    host: &TestHost,
    request: HolderChangeRequest,
    epoch: u64,
    start: ReplicationPosition,
    history: &str,
) -> TestResult {
    let (attempt, expected_holder) = match &request {
        HolderChangeRequest::Move { attempt, to, .. }
        | HolderChangeRequest::Takeover { attempt, to, .. } => (*attempt, *to),
        HolderChangeRequest::Claim {
            attempt, claimant, ..
        } => (*attempt, *claimant),
    };
    let key = holder_change_request_key(attempt)?;
    let answer =
        task_answer::<HolderChangeTask<TestStore, Turso>>(engine, &key, request.clone()).await?;
    assert!(
        matches!(answer,HolderChangeAnswer::Granted { epoch: actual, start: position, .. } if actual==Epoch::new(epoch)? && position==start)
    );
    let calls = host.calls.lock().await.len();
    host.advance();
    assert_eq!(
        task_answer::<HolderChangeTask<TestStore, Turso>>(engine, &key, request).await?,
        answer
    );
    assert_eq!(host.calls.lock().await.len(), calls);
    assert_eq!(
        scalar(store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        epoch.to_string()
    );
    assert_eq!(
        scalar(
            store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_history"
        )
        .await?,
        history
    );
    let database_id = database()?;
    let now = host.now();
    let lease = store
        .transaction(move |scope| {
            Box::pin(async move { lease_status(scope, database_id, now).await.map_err(storage) })
        })
        .await?
        .ok_or("lease missing")?;
    assert_eq!(lease.holder, expected_holder);
    assert_eq!(lease.epoch, Epoch::new(epoch)?);
    let mut copies = host.copies.lock().await;
    let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
    assert!(
        matches!(fleet_sync_receiver::copy_status(&mut CopyScope(copy)).await?,CopyStatusAnswer::Status(CopyStatus { holder: Some(holder), fence, start: actual_start, acked, .. }) if holder==expected_holder && fence==Epoch::new(epoch)? && actual_start==start && acked==start)
    );
    Ok(())
}
async fn assert_row(host: &TestHost, expected: &str) -> TestResult {
    let mut copies = host.copies.lock().await;
    let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
    let row = sqlx_core::query::query::<Turso>(
        "SELECT body FROM records WHERE id='019eef00-0000-7000-8000-000000000001'",
    )
    .fetch_one(copy)
    .await?;
    assert_eq!(row.try_get::<String, _>("body")?, expected);
    Ok(())
}

#[tokio::test]
async fn successive_clean_moves_keep_an_inherited_position_with_or_without_rows() -> TestResult {
    for populated in [false, true] {
        let directory = tempfile::tempdir()?;
        let host = TestHost::open(directory.path(), true).await?;
        let store = open_store(
            &directory.path().join("registry.db"),
            Arc::clone(&host),
            true,
        )
        .await?;
        let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
        register_first(&engine).await?;
        let start = if populated {
            push_rows(&host, 9).await?
        } else {
            ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0))
        };
        grant_and_replay(&engine, &store, &host, movement(2401, 1, 2)?, 2, start, "2").await?;
        grant_and_replay(&engine, &store, &host, movement(2402, 2, 3)?, 3, start, "3").await?;
        if populated {
            assert_row(&host, "kept row").await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn claim_then_clean_move_keeps_seed_rows_and_the_predecessor_epoch_position() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    let import = import_request(201, "a")?;
    assert_eq!(
        task_answer::<ImportTask<TestStore, Turso>>(
            &engine,
            &lifecycle_request_key(import.attempt)?,
            import
        )
        .await?,
        ImportAnswer::Imported
    );
    let start = seed_import(&host).await?;
    grant_and_replay(
        &engine,
        &store,
        &host,
        HolderChangeRequest::Claim {
            attempt: transfer(2401)?,
            database: database()?,
            claimant: peer(2)?,
        },
        2,
        start,
        "1",
    )
    .await?;
    grant_and_replay(&engine, &store, &host, movement(2402, 2, 3)?, 3, start, "2").await?;
    assert_row(&host, "imported row").await?;
    Ok(())
}

#[tokio::test]
async fn takeover_then_clean_move_preserves_rows_and_replays_one_successor_grant() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), true).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    register_first(&engine).await?;
    let start = push_rows(&host, 9).await?;
    let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
        &engine,
        &RequestKey::new("idle-takeover")?,
        PreviewTakeover {
            database: database()?,
        },
    )
    .await?;
    grant_and_replay(
        &engine,
        &store,
        &host,
        HolderChangeRequest::Takeover {
            attempt: transfer(2401)?,
            database: database()?,
            preview,
            to: peer(2)?,
        },
        2,
        start,
        "2",
    )
    .await?;
    grant_and_replay(&engine, &store, &host, movement(2402, 2, 3)?, 3, start, "3").await?;
    assert_row(&host, "kept row").await?;
    Ok(())
}

#[tokio::test]
async fn a_release_from_a_future_epoch_still_fails_before_acknowledgement_or_fence() -> TestResult {
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
    let lease = scalar(&store, "SELECT record AS value FROM _fleet_sync_leases").await?;
    *host.release_answer.lock().await = Some(ReleaseAnswer::Released {
        final_position: ReplicationPosition::new(Epoch::new(2)?, CommitSequence::new(0)),
    });
    let calls = host.calls.lock().await.len();
    let id = engine
        .start::<HolderChangeTask<TestStore, Turso>>(
            &holder_change_request_key(transfer(2401)?)?,
            movement(2401, 1, 2)?,
        )
        .await?;
    assert!(
        matches!(engine.status(&id).await?.state,OperationState::Failed { reason:durable_steps::OperationFailure::Refused { code,.. } } if code=="registry_refused")
    );
    assert_eq!(host.calls.lock().await.len(), calls + 1);
    assert_eq!(
        scalar(&store, "SELECT record AS value FROM _fleet_sync_leases").await?,
        lease
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_history"
        )
        .await?,
        "1"
    );
    Ok(())
}
