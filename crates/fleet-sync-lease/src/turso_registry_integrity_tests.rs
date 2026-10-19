//! Registry identity and in-progress transfer guards through the real operation host.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_change_tests::register_first, turso_registry_fixture::*, *};
use durable_steps::storage::TransactionScope;
use durable_steps::{AtomicStore, OperationState, Uuid};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

#[tokio::test]
async fn stored_preview_token_is_validated_before_a_registry_read_returns_it() -> TestResult {
    use crate::{registry_records::PreviewRecord, registry_storage::RegistryStorage};
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&directory.path().join("registry.db"), host, true).await?;
    let token = PreviewToken::new("019eef00-0000-7000-8000-000000000011")?;
    let record = PreviewRecord {
        database: database()?,
        preview: TakeoverPreview {
            token: token.clone(),
            fence: Epoch::new(1)?,
            acked: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0)),
            holder: peer(1)?,
            last_seen: None,
        },
    };
    let saving = record.clone();
    store
        .transaction(move |scope| {
            Box::pin(async move {
                RegistryStorage(scope)
                    .save_preview(&saving)
                    .await
                    .map_err(storage)
            })
        })
        .await?;
    let reading = token.clone();
    let decoded = store
        .transaction(move |scope| {
            Box::pin(async move {
                RegistryStorage(scope)
                    .preview(&reading)
                    .await
                    .map_err(storage)
            })
        })
        .await?;
    assert_eq!(decoded, Some(record.clone()));
    let mut corrupt = serde_json::to_value(record)?;
    *corrupt
        .get_mut("preview")
        .and_then(|preview| preview.get_mut("token"))
        .ok_or("missing preview token")? =
        serde_json::json!("019eef00-0000-4000-8000-000000000011");
    let corrupt = serde_json::to_string(&corrupt)?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                sqlx_core::query::query::<Turso>(
                    "UPDATE _fleet_sync_takeover_previews SET record=?",
                )
                .bind(corrupt)
                .execute(scope.executor())
                .await?;
                Ok(())
            })
        })
        .await?;
    let result = store
        .transaction(move |scope| {
            Box::pin(async move {
                RegistryStorage(scope)
                    .preview(&token)
                    .await
                    .map_err(storage)
            })
        })
        .await;
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
async fn import_replays_the_copy_before_publishing_registry_state_after_lost_answer() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("registry.db");
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&path, Arc::clone(&host), true).await?;
    let running = engine(Arc::clone(&store), Arc::clone(&host))?;
    let request = import_request(201, "a")?;
    let key = lifecycle_request_key(request.attempt)?;
    host.lose_create.store(true, Ordering::SeqCst);
    let id = running
        .start::<ImportTask<TestStore, Turso>>(&key, request.clone())
        .await?;
    assert!(matches!(
        running.status(&id).await?.state,
        OperationState::Running { .. }
    ));
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_databases"
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
    drop(running);
    drop(store);
    host.advance();
    let store = open_store(&path, Arc::clone(&host), false).await?;
    let resumed = engine(Arc::clone(&store), Arc::clone(&host))?;
    assert_eq!(resumed.drive_due().await?.completed, vec![id]);
    assert_eq!(
        task_answer::<ImportTask<TestStore, Turso>>(&resumed, &key, request).await?,
        ImportAnswer::Imported
    );
    let next = import_request(202, "a")?;
    assert_eq!(
        task_answer::<ImportTask<TestStore, Turso>>(
            &resumed,
            &lifecycle_request_key(next.attempt)?,
            next
        )
        .await?,
        ImportAnswer::AlreadyImported
    );
    assert_eq!(
        scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        "1"
    );
    let calls = host
        .calls
        .lock()
        .await
        .iter()
        .filter(|(kind, _)| kind == "fleet-sync.copy-create")
        .map(|(_, call)| call.clone())
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls.first(), calls.last());
    Ok(())
}

#[tokio::test]
async fn stored_database_identity_mismatch_is_not_treated_as_an_import_replay() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    let request = import_request(201, "a")?;
    assert_eq!(
        task_answer::<ImportTask<TestStore, Turso>>(
            &engine,
            &lifecycle_request_key(request.attempt)?,
            request
        )
        .await?,
        ImportAnswer::Imported
    );
    let original = scalar(&store, "SELECT record AS value FROM _fleet_sync_databases").await?;
    let mut record: serde_json::Value = serde_json::from_str(&original)?;
    let object = record.as_object_mut().ok_or("database object")?;
    object.insert(
        "database".into(),
        serde_json::to_value(DatabaseId::new(Uuid::from_u128(
            0x019eef0000007000800000000000076c,
        ))?)?,
    );
    let changed = serde_json::to_string(&record)?;
    let database = database()?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                sqlx_core::query::query::<Turso>("UPDATE _fleet_sync_databases SET record=?")
                    .bind(changed)
                    .execute(scope.executor())
                    .await?;
                assert!(matches!(
                    database_lifecycle(scope, database).await,
                    Err(LeaseError::Corrupt { .. })
                ));
                Ok(())
            })
        })
        .await?;
    let offered = import_request(202, "a")?;
    let id = engine
        .start::<ImportTask<TestStore, Turso>>(&lifecycle_request_key(offered.attempt)?, offered)
        .await?;
    assert!(matches!(
        engine.status(&id).await?.state,
        OperationState::Failed { .. }
    ));
    assert_eq!(
        host.calls
            .lock()
            .await
            .iter()
            .filter(|(kind, _)| kind == "fleet-sync.copy-create")
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn a_pending_move_blocks_another_move_and_renewal_returns_fresh_authority_after_grant()
-> TestResult {
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
    host.lose_release.store(true, Ordering::SeqCst);
    let attempt = TransferId::new(Uuid::from_u128(0x019eef0000007000800000000000076d))?;
    let request = HolderChangeRequest::Move {
        reason: fleet_sync_protocol::HolderChangeReason::new("exercise holder change")?,
        attempt,
        database: database()?,
        from_epoch: Epoch::new(1)?,
        to: peer(2)?,
    };
    let id = engine
        .start::<HolderChangeTask<TestStore, Turso>>(&holder_change_request_key(attempt)?, request)
        .await?;
    let other = TransferId::new(Uuid::from_u128(0x019eef0000007000800000000000076e))?;
    assert_eq!(
        task_answer::<HolderChangeTask<TestStore, Turso>>(
            &engine,
            &holder_change_request_key(other)?,
            HolderChangeRequest::Move {
                reason: fleet_sync_protocol::HolderChangeReason::new("exercise holder change")?,
                attempt: other,
                database: database()?,
                from_epoch: Epoch::new(1)?,
                to: peer(3)?
            }
        )
        .await?,
        HolderChangeAnswer::Refused(HolderChangeRefusal::TransferInProgress { transfer: attempt })
    );
    assert_eq!(
        scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        "2"
    );
    let request = RenewRequest {
        attempt: RequestId::new(Uuid::from_u128(0x019eef0000007000800000000000076f))?,
        database: database()?,
        epoch: Epoch::new(1)?,
    };
    let holder = peer(1)?;
    let policy = config()?.lease;
    let original = store
        .transaction(move |scope| {
            Box::pin(async move {
                renew(
                    scope,
                    &request,
                    &holder,
                    SyncTime::from_unix_millis(1000),
                    &policy,
                )
                .await
                .map_err(storage)
            })
        })
        .await?;
    assert!(
        matches!(original,RenewAnswer::Renewed{current:LeaseStatus{epoch,..},transfer_in_progress:Some(active),..} if epoch==Epoch::new(1)? && active==attempt)
    );
    host.advance();
    assert_eq!(engine.drive_due().await?.completed, vec![id]);
    let request = RenewRequest {
        attempt: RequestId::new(Uuid::from_u128(0x019eef0000007000800000000000076f))?,
        database: database()?,
        epoch: Epoch::new(1)?,
    };
    let holder = peer(1)?;
    let policy = config()?.lease;
    let replay = store
        .transaction(move |scope| {
            Box::pin(async move {
                renew(
                    scope,
                    &request,
                    &holder,
                    SyncTime::from_unix_millis(1_001_000),
                    &policy,
                )
                .await
                .map_err(storage)
            })
        })
        .await?;
    assert!(
        matches!(replay,RenewAnswer::Renewed{recorded:RenewResult{epoch:recorded,..},current:LeaseStatus{epoch,..},replayed:true,transfer_in_progress:None} if recorded==Epoch::new(1)? && epoch==Epoch::new(2)?)
    );
    Ok(())
}
