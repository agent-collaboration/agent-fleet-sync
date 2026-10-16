//! The real gate and copy receipts answer lost claim calls after journal reopen.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_copy_fixture::*, turso_registry_fixture::*, *};
use durable_steps::{OperationState, Uuid};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

#[tokio::test]
async fn claim_replays_reservation_and_fence_without_a_second_epoch_or_settlement() -> TestResult {
    for interruption in ["reservation-answer", "fence-before-send", "fence-answer"] {
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
        host.fence_unreachable
            .store(interruption == "fence-before-send", Ordering::SeqCst);
        host.lose_fence
            .store(interruption == "fence-answer", Ordering::SeqCst);
        let attempt = TransferId::new(Uuid::from_u128(1001))?;
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
        let repeated = if interruption == "reservation-answer" {
            "fleet-sync.claim-reservation"
        } else {
            "fleet-sync.copy-fence"
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
