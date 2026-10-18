//! The real gate and copy receipts answer lost claim calls after journal reopen.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_copy_fixture::*, turso_registry_fixture::*, *};
use durable_steps::{OperationState, Uuid};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

#[tokio::test]
async fn claim_replays_reservation_and_fence_without_a_second_epoch_or_settlement() -> TestResult {
    for interruption in [
        "reservation-answer",
        "status-before-send",
        "fence-before-send",
        "fence-answer",
    ] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("registry.db");
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(&path, Arc::clone(&host), true).await?;
        let running = engine(Arc::clone(&store), Arc::clone(&host))?;
        let import = import_request(201, "a")?;
        assert_eq!(
            task_answer::<ImportTask<TestStore, Turso>>(
                &running,
                &lifecycle_request_key(import.attempt)?,
                import
            )
            .await?,
            ImportAnswer::Imported
        );
        seed_import(&host).await?;
        host.lose_reservation
            .store(interruption == "reservation-answer", Ordering::SeqCst);
        host.status_unreachable
            .store(interruption == "status-before-send", Ordering::SeqCst);
        host.fence_unreachable
            .store(interruption == "fence-before-send", Ordering::SeqCst);
        host.lose_fence
            .store(interruption == "fence-answer", Ordering::SeqCst);
        let attempt = TransferId::new(Uuid::from_u128(0x019eef000000700080000000000003e9))?;
        let request = HolderChangeRequest::Claim {
            attempt,
            database: database()?,
            claimant: peer(2)?,
        };
        let key = holder_change_request_key(attempt)?;
        let id = running
            .start::<HolderChangeTask<TestStore, Turso>>(&key, request.clone())
            .await?;
        assert!(matches!(
            running.status(&id).await?.state,
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
        assert_eq!(host.gate.settlement_count().await?, "0");
        drop(running);
        drop(store);
        host.advance();
        host.fence_unreachable.store(false, Ordering::SeqCst);
        host.status_unreachable.store(false, Ordering::SeqCst);
        let store = open_store(&path, Arc::clone(&host), false).await?;
        let resumed = engine(Arc::clone(&store), Arc::clone(&host))?;
        assert_eq!(resumed.drive_due().await?.completed, vec![id]);
        let answer =
            task_answer::<HolderChangeTask<TestStore, Turso>>(&resumed, &key, request.clone())
                .await?;
        assert!(matches!(answer,HolderChangeAnswer::Granted{epoch,..} if epoch==Epoch::new(2)?));
        host.advance();
        assert_eq!(
            task_answer::<HolderChangeTask<TestStore, Turso>>(&resumed, &key, request).await?,
            answer
        );
        assert_eq!(host.gate.settlement_count().await?, "1");
        assert_eq!(
            host.gate.settlement_outcome(attempt).await?,
            Some(SettleOutcome::Granted {
                epoch: Epoch::new(2)?
            })
        );
        assert_eq!(
            scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
            "2"
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_history"
            )
            .await?,
            "1"
        );
        let repeated = match interruption {
            "reservation-answer" => "fleet-sync.claim-reservation",
            "status-before-send" => "fleet-sync.copy-status",
            _ => "fleet-sync.copy-fence",
        };
        let calls = host
            .calls
            .lock()
            .await
            .iter()
            .filter(|(kind, _)| kind == repeated)
            .map(|(_, call)| call.clone())
            .collect::<Vec<_>>();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls.first(), calls.last());
    }
    Ok(())
}

#[tokio::test]
async fn an_unseeded_copy_refuses_claim_and_settles_its_reserved_attempt() -> TestResult {
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
    let attempt = TransferId::new(Uuid::from_u128(0x019eef000000700080000000000003ea))?;
    let request = HolderChangeRequest::Claim {
        attempt,
        database: database()?,
        claimant: peer(2)?,
    };
    let key = holder_change_request_key(attempt)?;
    assert_eq!(
        task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, request.clone()).await?,
        HolderChangeAnswer::Refused(HolderChangeRefusal::Unavailable)
    );
    assert_eq!(
        task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, request).await?,
        HolderChangeAnswer::Refused(HolderChangeRefusal::Unavailable)
    );
    assert_eq!(
        host.gate.settlement_outcome(attempt).await?,
        Some(SettleOutcome::Refused {
            reason: HolderChangeRefusal::Unavailable
        })
    );
    assert_eq!(host.gate.settlement_count().await?, "1");
    assert_eq!(
        scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        "2"
    );
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_leases"
        )
        .await?,
        "0"
    );
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
async fn claim_repairs_above_a_restored_imported_copys_fence_before_granting() -> TestResult {
    use sqlx_core::connection::Connection;
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
    let position = seed_import(&host).await?;
    {
        let mut copies = host.copies.lock().await;
        let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
        let mut transaction = copy.begin_with("BEGIN IMMEDIATE").await?;
        let request = FenceRequest {
            database: database()?,
            transfer_id: TransferId::new(Uuid::from_u128(0x019eef000000700080000000000003eb))?,
            new_epoch: Epoch::new(5)?,
            expect: FenceExpectation {
                epoch: Epoch::new(1)?,
                acked: position,
            },
            target_schema_max: 10,
            holder: peer(9)?,
        };
        assert!(matches!(
            fleet_sync_receiver::fence(&mut CopyScope(&mut transaction), &request).await?,
            FenceResult::Fenced { .. }
        ));
        transaction.commit().await?;
    }
    let attempt = TransferId::new(Uuid::from_u128(0x019eef000000700080000000000003ec))?;
    let request = HolderChangeRequest::Claim {
        attempt,
        database: database()?,
        claimant: peer(2)?,
    };
    let answer = task_answer::<HolderChangeTask<TestStore, Turso>>(
        &engine,
        &holder_change_request_key(attempt)?,
        request,
    )
    .await?;
    assert!(
        matches!(answer,HolderChangeAnswer::Granted{epoch,start,..} if epoch==Epoch::new(6)? && start==position)
    );
    assert_eq!(
        host.gate.settlement_outcome(attempt).await?,
        Some(SettleOutcome::Granted {
            epoch: Epoch::new(6)?
        })
    );
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
        "1"
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
