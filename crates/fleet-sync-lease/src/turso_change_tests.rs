//! Holder changes use real registry and copy transactions; release uses the receipted peer fixture.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_copy_fixture::*, turso_registry_fixture::*, *};
use durable_steps::{AtomicStore, OperationState, Uuid};
use fleet_sync_protocol::*;
use sqlx_core::row::Row;
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

fn transfer(value: u128) -> TestResult<TransferId> {
    Ok(TransferId::new(Uuid::from_u128(value))?)
}
pub(super) async fn register_first(engine: &TestEngine) -> TestResult {
    let request = registration(201, 1)?;
    let answer = task_answer::<RegistrationTask<TestStore, Turso>>(
        engine,
        &lifecycle_request_key(request.attempt)?,
        request,
    )
    .await?;
    if !matches!(answer, RegistrationAnswer::Registered { .. }) {
        return Err("registration did not grant".into());
    }
    Ok(())
}
#[tokio::test]
async fn claim_grants_the_next_epoch_and_settles_once_after_seed_publication() -> TestResult {
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
            import.clone()
        )
        .await?,
        ImportAnswer::Imported
    );
    let start = seed_import(&host).await?;
    let request = HolderChangeRequest::Claim {
        attempt: transfer(301)?,
        database: database()?,
        claimant: peer(2)?,
    };
    let answer = task_answer::<HolderChangeTask<TestStore, Turso>>(
        &engine,
        &holder_change_request_key(transfer(301)?)?,
        request.clone(),
    )
    .await?;
    assert!(
        matches!(answer,HolderChangeAnswer::Granted{epoch,start:actual,..} if epoch==Epoch::new(2)?&&actual==start)
    );
    assert_eq!(
        task_answer::<HolderChangeTask<TestStore, Turso>>(
            &engine,
            &holder_change_request_key(transfer(301)?)?,
            request
        )
        .await?,
        answer
    );
    assert_eq!(host.gate.settlement_count().await?, "1");
    assert_eq!(
        host.gate.settlement_outcome(transfer(301)?).await?,
        Some(SettleOutcome::Granted {
            epoch: Epoch::new(2)?
        })
    );
    assert_eq!(
        scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        "2"
    );
    let reimport = import_request(202, "a")?;
    assert_eq!(
        task_answer::<ImportTask<TestStore, Turso>>(
            &engine,
            &lifecycle_request_key(reimport.attempt)?,
            reimport
        )
        .await?,
        ImportAnswer::ImportConflict {
            existing: None,
            offered: import.digest
        }
    );
    Ok(())
}

#[tokio::test]
async fn lost_release_answer_replays_one_call_even_past_the_unsent_bound_and_keeps_rows()
-> TestResult {
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
    let final_position = push_rows(&host, 9).await?;
    host.lose_release.store(true, Ordering::SeqCst);
    let request = HolderChangeRequest::Move {
        attempt: transfer(301)?,
        database: database()?,
        from_epoch: Epoch::new(1)?,
        to: peer(2)?,
    };
    let id = engine
        .start::<HolderChangeTask<TestStore, Turso>>(
            &holder_change_request_key(transfer(301)?)?,
            request,
        )
        .await?;
    assert!(matches!(
        engine.status(&id).await?.state,
        OperationState::Running { .. }
    ));
    host.advance();
    assert_eq!(engine.drive_due().await?.completed.len(), 1);
    let OperationState::Completed { result } = engine.status(&id).await?.state else {
        return Err("move did not finish".into());
    };
    assert!(
        matches!(serde_json::from_slice::<HolderChangeAnswer>(&result)?,HolderChangeAnswer::Granted{epoch,start,..} if epoch==Epoch::new(2)?&&start==final_position)
    );
    let calls = host
        .calls
        .lock()
        .await
        .iter()
        .filter(|(kind, _)| kind == "fleet-sync.holder-release")
        .map(|(_, call)| call.clone())
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls.first(), calls.last());
    let mut copies = host.copies.lock().await;
    let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
    let row = sqlx_core::query::query::<Turso>(
        "SELECT body FROM records WHERE id='019eef00-0000-7000-8000-000000000001'",
    )
    .fetch_one(&mut *copy)
    .await?;
    assert_eq!(row.try_get::<String, _>("body")?, "kept row");
    assert!(
        matches!(fleet_sync_receiver::copy_status(&mut CopyScope(copy)).await?,CopyStatusAnswer::Status(CopyStatus{fence,holder:Some(holder),acked,..}) if fence==Epoch::new(2)?&&holder==peer(2)?&&acked==final_position)
    );
    Ok(())
}

#[tokio::test]
async fn never_sent_move_ends_unreachable_without_fencing_or_changing_holder() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), true).await?;
    host.release_unreachable.store(true, Ordering::SeqCst);
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    register_first(&engine).await?;
    let request = HolderChangeRequest::Move {
        attempt: transfer(301)?,
        database: database()?,
        from_epoch: Epoch::new(1)?,
        to: peer(2)?,
    };
    let id = engine
        .start::<HolderChangeTask<TestStore, Turso>>(
            &holder_change_request_key(transfer(301)?)?,
            request,
        )
        .await?;
    assert!(matches!(
        engine.status(&id).await?.state,
        OperationState::Running { .. }
    ));
    host.advance();
    assert_eq!(engine.drive_due().await?.completed.len(), 1);
    let OperationState::Completed { result } = engine.status(&id).await?.state else {
        return Err("move did not finish".into());
    };
    assert_eq!(
        serde_json::from_slice::<HolderChangeAnswer>(&result)?,
        HolderChangeAnswer::Refused(HolderChangeRefusal::HolderUnreachable {
            last_seen: Some(SyncTime::from_unix_millis(1000))
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
    let database = database()?;
    let current = store
        .transaction(move |scope| {
            Box::pin(async move {
                lease_status(scope, database, SyncTime::from_unix_millis(1_001_000))
                    .await
                    .map_err(storage)
            })
        })
        .await?
        .ok_or("lease missing")?;
    assert_eq!(current.holder, peer(1)?);
    assert_eq!(current.epoch, Epoch::new(1)?);
    assert!(
        host.calls
            .lock()
            .await
            .iter()
            .all(|(kind, _)| kind != "fleet-sync.copy-fence")
    );
    Ok(())
}

#[tokio::test]
async fn takeover_refuses_a_moved_preview_then_new_preview_uses_a_later_reserved_epoch()
-> TestResult {
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
    let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
        &engine,
        &durable_steps::RequestKey::new("preview-1")?,
        PreviewTakeover {
            database: database()?,
        },
    )
    .await?;
    let cutoff = push_rows(&host, 9).await?;
    let first = HolderChangeRequest::Takeover {
        attempt: transfer(301)?,
        database: database()?,
        preview,
        to: peer(2)?,
    };
    assert_eq!(
        task_answer::<HolderChangeTask<TestStore, Turso>>(
            &engine,
            &holder_change_request_key(transfer(301)?)?,
            first
        )
        .await?,
        HolderChangeAnswer::Refused(HolderChangeRefusal::PreviewStale)
    );
    let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
        &engine,
        &durable_steps::RequestKey::new("preview-2")?,
        PreviewTakeover {
            database: database()?,
        },
    )
    .await?;
    assert_eq!(preview.acked, cutoff);
    let second = HolderChangeRequest::Takeover {
        attempt: transfer(302)?,
        database: database()?,
        preview,
        to: peer(2)?,
    };
    assert!(
        matches!(task_answer::<HolderChangeTask<TestStore,Turso>>(&engine,&holder_change_request_key(transfer(302)?)?,second).await?,HolderChangeAnswer::Granted{epoch,start,..} if epoch==Epoch::new(3)?&&start==cutoff)
    );
    assert!(
        host.calls
            .lock()
            .await
            .iter()
            .all(|(kind, _)| kind != "fleet-sync.holder-release")
    );
    Ok(())
}
