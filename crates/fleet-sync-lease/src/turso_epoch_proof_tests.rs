//! Missing epoch evidence refuses authority changes before any external action.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_change_tests::register_first, turso_registry_fixture::*, *};
use durable_steps::{AtomicStore, OperationState, RequestKey, Uuid, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

async fn execute_fixture_sql(store: &TestStore, sql: &'static str) -> TestResult {
    store
        .transaction(move |scope| {
            Box::pin(async move {
                sqlx_core::raw_sql::raw_sql(sql)
                    .execute(scope.executor())
                    .await?;
                Ok(())
            })
        })
        .await?;
    Ok(())
}

async fn move_holder(
    engine: &durable_steps::Engine<TestStore, TestHost>,
    attempt: u128,
    from: u64,
    to: u128,
) -> TestResult<HolderChangeAnswer> {
    let attempt = TransferId::new(Uuid::from_u128(attempt))?;
    task_answer::<HolderChangeTask<TestStore, Turso>>(
        engine,
        &holder_change_request_key(attempt)?,
        HolderChangeRequest::Move {
            attempt,
            database: database()?,
            from_epoch: Epoch::new(from)?,
            to: peer(to)?,
        },
    )
    .await
}

#[tokio::test]
async fn missing_or_exhausted_floor_refuses_without_changing_authority_or_calling_peers()
-> TestResult {
    for damage in [
        "DELETE FROM _fleet_sync_epoch_floor",
        "UPDATE _fleet_sync_epoch_floor SET epoch='18446744073709551615'",
    ] {
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
        execute_fixture_sql(&store, damage).await?;
        let lease = scalar(&store, "SELECT record AS value FROM _fleet_sync_leases").await?;
        let calls = host.calls.lock().await.len();

        assert_eq!(
            move_holder(&engine, 2001, 1, 2).await?,
            HolderChangeAnswer::Refused(HolderChangeRefusal::EpochUnprovable)
        );
        assert_eq!(host.calls.lock().await.len(), calls);
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
        assert_eq!(
            scalar(
                &store,
                "SELECT coalesce(max(epoch),'absent') AS value FROM _fleet_sync_epoch_floor"
            )
            .await?,
            if damage.starts_with("DELETE") {
                "absent"
            } else {
                "18446744073709551615"
            }
        );
    }
    Ok(())
}

#[tokio::test]
async fn floor_behind_the_current_lease_cannot_reuse_a_previously_granted_epoch() -> TestResult {
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
    assert!(
        matches!(move_holder(&engine, 2001, 1, 2).await?, HolderChangeAnswer::Granted { epoch, .. } if epoch==Epoch::new(2)?)
    );
    execute_fixture_sql(&store, "UPDATE _fleet_sync_epoch_floor SET epoch='1'").await?;
    let lease = scalar(&store, "SELECT record AS value FROM _fleet_sync_leases").await?;
    let calls = host.calls.lock().await.len();

    assert_eq!(
        move_holder(&engine, 2002, 2, 3).await?,
        HolderChangeAnswer::Refused(HolderChangeRefusal::EpochUnprovable)
    );
    assert_eq!(host.calls.lock().await.len(), calls);
    assert_eq!(
        scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        "1"
    );
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
        "2"
    );
    Ok(())
}

#[tokio::test]
async fn unavailable_preview_saves_no_confirmation_or_epoch_until_the_copy_answers() -> TestResult {
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
    host.status_unreachable.store(true, Ordering::SeqCst);
    let id = engine
        .start::<PreviewTakeoverTask<TestStore, Turso>>(
            &RequestKey::new("preview-unavailable")?,
            PreviewTakeover {
                database: database()?,
            },
        )
        .await?;
    assert!(matches!(
        engine.status(&id).await?.state,
        OperationState::Running { .. }
    ));
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_takeover_previews"
        )
        .await?,
        "0"
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_transfers"
        )
        .await?,
        "0"
    );
    assert_eq!(
        scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        "1"
    );

    host.status_unreachable.store(false, Ordering::SeqCst);
    host.advance();
    assert_eq!(engine.drive_due().await?.completed, vec![id]);
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_takeover_previews"
        )
        .await?,
        "1"
    );
    assert_eq!(
        scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        "1"
    );
    Ok(())
}

#[tokio::test]
async fn absent_copy_and_floor_cannot_create_a_preview_or_manufacture_epoch_evidence() -> TestResult
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
    register_first(&engine).await?;
    execute_fixture_sql(&store, "DELETE FROM _fleet_sync_epoch_floor").await?;
    {
        let mut copies = host.copies.lock().await;
        let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
        sqlx_core::query::query::<Turso>("DELETE FROM _fleet_sync_copy")
            .execute(copy)
            .await?;
    }

    let id = engine
        .start::<PreviewTakeoverTask<TestStore, Turso>>(
            &RequestKey::new("preview-without-evidence")?,
            PreviewTakeover {
                database: database()?,
            },
        )
        .await?;
    assert!(
        matches!(engine.status(&id).await?.state, OperationState::Failed { reason: durable_steps::OperationFailure::Refused { code, .. } } if code=="registry_refused")
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_takeover_previews"
        )
        .await?,
        "0"
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_transfers"
        )
        .await?,
        "0"
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_floor"
        )
        .await?,
        "0"
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
