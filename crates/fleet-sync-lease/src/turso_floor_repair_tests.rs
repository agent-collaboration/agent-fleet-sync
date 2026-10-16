//! The authoritative copy proves a restored registry's missing epoch floor.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_change_tests::register_first, turso_registry_copy::FenceGate, turso_registry_fixture::*,
    *,
};
use durable_steps::{RequestKey, Uuid};
use fleet_sync_protocol::*;
use sqlx_core::connection::Connection;
use sqlx_turso::Turso;
use std::{sync::Arc, time::Duration};

fn transfer(value: u128) -> TestResult<TransferId> {
    Ok(TransferId::new(Uuid::from_u128(value))?)
}
async fn advance_copy(host: &TestHost, from: u64, to: u64) -> TestResult {
    let mut copies = host.copies.lock().await;
    let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
    let mut transaction = copy.begin_with("BEGIN IMMEDIATE").await?;
    let request = FenceRequest {
        database: database()?,
        transfer_id: transfer(900 + u128::from(to))?,
        new_epoch: Epoch::new(to)?,
        expect: FenceExpectation {
            epoch: Epoch::new(from)?,
            acked: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0)),
        },
        target_schema_max: 10,
        holder: peer(1)?,
    };
    assert!(
        matches!(fleet_sync_receiver::fence(&mut CopyScope(&mut transaction), &request).await?, FenceResult::Fenced { new_epoch, .. } if new_epoch == Epoch::new(to)?)
    );
    transaction.commit().await?;
    Ok(())
}

#[tokio::test]
async fn restored_registry_reserves_above_the_fresh_previews_copy_fence() -> TestResult {
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
    advance_copy(&host, 1, 5).await?;
    let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
        &engine,
        &RequestKey::new("copy-five")?,
        PreviewTakeover {
            database: database()?,
        },
    )
    .await?;
    assert_eq!(preview.fence, Epoch::new(5)?);
    let answer = task_answer::<HolderChangeTask<TestStore, Turso>>(
        &engine,
        &holder_change_request_key(transfer(911)?)?,
        HolderChangeRequest::Takeover {
            attempt: transfer(911)?,
            database: database()?,
            preview,
            to: peer(2)?,
        },
    )
    .await?;
    assert!(matches!(answer,HolderChangeAnswer::Granted{epoch,..} if epoch == Epoch::new(6)?));
    assert_eq!(
        scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        "6"
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
async fn fence_not_advanced_repairs_once_from_the_real_copys_proven_fence() -> TestResult {
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
    let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
        &engine,
        &RequestKey::new("copy-one")?,
        PreviewTakeover {
            database: database()?,
        },
    )
    .await?;
    let gate = FenceGate::new();
    host.fence_gates
        .lock()
        .await
        .insert(transfer(912)?, Arc::clone(&gate));
    let request = HolderChangeRequest::Takeover {
        attempt: transfer(912)?,
        database: database()?,
        preview,
        to: peer(2)?,
    };
    let key = holder_change_request_key(transfer(912)?)?;
    let drive = task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, request);
    let control = async {
        gate.started.notified().await;
        advance_copy(&host, 1, 5).await?;
        // The second dispatch can proceed: this test only moves the copy once.
        host.fence_gates.lock().await.remove(&transfer(912)?);
        gate.proceed.notify_one();
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let (answer, control) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(drive, control)
    })
    .await?;
    control?;
    assert!(matches!(answer?,HolderChangeAnswer::Granted{epoch,..} if epoch == Epoch::new(6)?));
    assert_eq!(
        scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        "6"
    );
    let calls = host
        .calls
        .lock()
        .await
        .iter()
        .filter(|(kind, _)| kind == "fleet-sync.copy-fence")
        .map(|(_, call)| call.clone())
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 2);
    assert_ne!(calls.first(), calls.last());
    Ok(())
}

#[tokio::test]
async fn a_second_floor_refusal_keeps_its_reservation_without_granting_or_changing_the_lease()
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
    let original_lease = scalar(&store, "SELECT record AS value FROM _fleet_sync_leases").await?;
    let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
        &engine,
        &RequestKey::new("moving-copy")?,
        PreviewTakeover {
            database: database()?,
        },
    )
    .await?;
    let gate = FenceGate::new();
    host.fence_gates
        .lock()
        .await
        .insert(transfer(913)?, Arc::clone(&gate));
    let request = HolderChangeRequest::Takeover {
        attempt: transfer(913)?,
        database: database()?,
        preview,
        to: peer(2)?,
    };
    let key = holder_change_request_key(transfer(913)?)?;
    let drive = task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, request.clone());
    let control = async {
        gate.started.notified().await;
        advance_copy(&host, 1, 5).await?;
        gate.proceed.notify_one();
        gate.started.notified().await;
        advance_copy(&host, 5, 7).await?;
        gate.proceed.notify_one();
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let (answer, control) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(drive, control)
    })
    .await?;
    control?;
    assert_eq!(
        answer?,
        HolderChangeAnswer::Refused(HolderChangeRefusal::EpochUnprovable)
    );
    assert_eq!(
        scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        "6"
    );
    assert_eq!(
        scalar(&store, "SELECT record AS value FROM _fleet_sync_leases").await?,
        original_lease
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
        task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, request).await?,
        HolderChangeAnswer::Refused(HolderChangeRefusal::EpochUnprovable)
    );
    assert_eq!(
        host.calls
            .lock()
            .await
            .iter()
            .filter(|(kind, _)| kind == "fleet-sync.copy-fence")
            .count(),
        2
    );
    Ok(())
}
