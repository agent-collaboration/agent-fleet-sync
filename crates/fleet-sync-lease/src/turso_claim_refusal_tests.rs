//! Terminal copy refusals settle the reservation; a missing copy stays pending for repair.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_copy_fixture::{push_rows, seed_import},
    turso_registry_copy::FenceGate,
    turso_registry_fixture::*,
    *,
};
use durable_steps::Uuid;
use fleet_sync_protocol::*;
use sqlx_core::connection::Connection;
use sqlx_turso::Turso;
use std::{sync::Arc, time::Duration};

fn attempt() -> TestResult<TransferId> {
    Ok(TransferId::new(Uuid::from_u128(
        0x019eef00000070008000000000000834,
    ))?)
}
fn request() -> TestResult<HolderChangeRequest> {
    Ok(HolderChangeRequest::Claim {
        attempt: attempt()?,
        database: database()?,
        claimant: peer(2)?,
    })
}
async fn prepare(engine: &TestEngine, host: &Arc<TestHost>) -> TestResult {
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
    seed_import(host).await?;
    Ok(())
}
pub(super) async fn retire_copy(host: &TestHost) -> TestResult {
    let mut copies = host.copies.lock().await;
    let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
    let mut tx = copy.begin_with("BEGIN IMMEDIATE").await?;
    let CopyStatusAnswer::Status(status) =
        fleet_sync_receiver::copy_status(&mut CopyScope(&mut tx)).await?
    else {
        return Err("copy not following".into());
    };
    assert_eq!(
        fleet_sync_receiver::retire_copy(
            &mut CopyScope(&mut tx),
            &RetireCopy {
                database: database()?,
                expect_fence: status.fence,
                expect_acked: status.acked
            },
            SyncTime::from_unix_millis(1000)
        )
        .await?,
        RetireCopyAnswer::Retired
    );
    tx.commit().await?;
    Ok(())
}
async fn advance_copy(host: &TestHost, to: u64) -> TestResult {
    let mut copies = host.copies.lock().await;
    let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
    let mut tx = copy.begin_with("BEGIN IMMEDIATE").await?;
    let CopyStatusAnswer::Status(status) =
        fleet_sync_receiver::copy_status(&mut CopyScope(&mut tx)).await?
    else {
        return Err("copy not following".into());
    };
    assert!(matches!(
        fleet_sync_receiver::fence(
            &mut CopyScope(&mut tx),
            &FenceRequest {
                database: database()?,
                transfer_id: TransferId::new(Uuid::from_u128(
                    (2200 + u128::from(to)) | 0x019eef00000070008000000000000000
                ))?,
                new_epoch: Epoch::new(to)?,
                expect: FenceExpectation {
                    epoch: status.fence,
                    acked: status.acked
                },
                target_schema: Some(fleet_sync_protocol::SchemaSupport { max_version: 10 }),
                holder: peer(1)?
            }
        )
        .await?,
        FenceResult::Fenced { .. }
    ));
    tx.commit().await?;
    Ok(())
}
async fn assert_settled(
    host: &TestHost,
    store: &TestStore,
    reason: HolderChangeRefusal,
) -> TestResult {
    assert_eq!(host.gate.settlement_count().await?, "1");
    assert_eq!(
        host.gate.settlement_outcome(attempt()?).await?,
        Some(SettleOutcome::Refused { reason })
    );
    assert_eq!(
        scalar(
            store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_leases"
        )
        .await?,
        "0"
    );
    assert_eq!(
        scalar(
            store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_epoch_history"
        )
        .await?,
        "0"
    );
    Ok(())
}

#[tokio::test]
async fn retired_copy_status_settles_a_reserved_claim_without_granting() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), true).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    prepare(&engine, &host).await?;
    retire_copy(&host).await?;
    let key = holder_change_request_key(attempt()?)?;
    assert_eq!(
        task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, request()?).await?,
        HolderChangeAnswer::Refused(HolderChangeRefusal::Retired)
    );
    assert_settled(&host, &store, HolderChangeRefusal::Retired).await?;
    Ok(())
}

#[tokio::test]
async fn retired_and_moved_copies_after_status_settle_the_reserved_claim() -> TestResult {
    for change in ["retired", "moved"] {
        let directory = tempfile::tempdir()?;
        let host = TestHost::open(directory.path(), true).await?;
        let store = open_store(
            &directory.path().join("registry.db"),
            Arc::clone(&host),
            true,
        )
        .await?;
        let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
        prepare(&engine, &host).await?;
        let gate = FenceGate::new();
        host.fence_gates
            .lock()
            .await
            .insert(attempt()?, Arc::clone(&gate));
        let key = holder_change_request_key(attempt()?)?;
        let offered = request()?;
        let drive =
            task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, offered.clone());
        let control = async {
            gate.started.notified().await;
            match change {
                "retired" => retire_copy(&host).await?,
                "moved" => {
                    advance_copy(&host, 2).await?;
                    push_rows(&host, 1).await?;
                }
                _ => return Err("unknown fixture change".into()),
            }
            host.fence_gates.lock().await.remove(&attempt()?);
            gate.proceed.notify_one();
            Ok::<_, Box<dyn std::error::Error>>(())
        };
        let (answer, control) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(drive, control)
        })
        .await?;
        control?;
        let refusal = match change {
            "retired" => HolderChangeRefusal::Retired,
            _ => HolderChangeRefusal::PreviewStale,
        };
        assert_eq!(answer?, HolderChangeAnswer::Refused(refusal.clone()));
        assert_settled(&host, &store, refusal.clone()).await?;
        let calls = host.calls.lock().await.len();
        host.advance();
        assert_eq!(
            task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, offered).await?,
            HolderChangeAnswer::Refused(refusal)
        );
        assert_eq!(host.calls.lock().await.len(), calls);
        assert_eq!(
            scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
            if change == "moved" { "3" } else { "2" }
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_second_floor_refusal_settles_claim_epoch_unprovable_without_reusing_the_reservation()
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
    prepare(&engine, &host).await?;
    let gate = FenceGate::new();
    host.fence_gates
        .lock()
        .await
        .insert(attempt()?, Arc::clone(&gate));
    let key = holder_change_request_key(attempt()?)?;
    let offered = request()?;
    let drive = task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, offered.clone());
    let control = async {
        gate.started.notified().await;
        advance_copy(&host, 5).await?;
        gate.proceed.notify_one();
        gate.started.notified().await;
        advance_copy(&host, 7).await?;
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
    assert_settled(&host, &store, HolderChangeRefusal::EpochUnprovable).await?;
    assert_eq!(
        scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        "6"
    );
    let calls = host.calls.lock().await.len();
    host.advance();
    assert_eq!(
        task_answer::<HolderChangeTask<TestStore, Turso>>(&engine, &key, offered).await?,
        HolderChangeAnswer::Refused(HolderChangeRefusal::EpochUnprovable)
    );
    assert_eq!(host.calls.lock().await.len(), calls);
    Ok(())
}

#[tokio::test]
async fn a_copy_missing_after_status_keeps_the_claim_pending_until_seed_repair() -> TestResult {
    let directory = tempfile::tempdir()?;
    let registry_path = directory.path().join("registry.db");
    let host = TestHost::open(directory.path(), true).await?;
    let store = open_store(&registry_path, host.clone(), true).await?;
    let running = engine(store.clone(), host.clone())?;
    prepare(&running, &host).await?;
    let gate = FenceGate::new();
    host.fence_gates
        .lock()
        .await
        .insert(attempt()?, gate.clone());
    let key = holder_change_request_key(attempt()?)?;
    let offered = request()?;
    let drive = running.start::<HolderChangeTask<TestStore, Turso>>(&key, offered.clone());
    let control = async {
        gate.started.notified().await;
        let mut copies = host.copies.lock().await;
        let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
        sqlx_core::query::query::<Turso>("DELETE FROM _fleet_sync_copy")
            .execute(copy)
            .await?;
        drop(copies);
        host.fence_gates.lock().await.remove(&attempt()?);
        gate.proceed.notify_one();
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let (operation, control) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(drive, control)
    })
    .await?;
    control?;
    let operation = operation?;
    let before = crate::turso_workflow_tests::view(&store, attempt()?).await?;
    assert!(
        before.outcome.is_none(),
        "missing copy fence must not become a terminal refusal"
    );
    let waiting = before.waiting.as_ref().ok_or("copy wait missing")?;
    assert_eq!(waiting.reach, durable_steps::storage::CallReach::NoEffect);
    assert_eq!(
        waiting.copy_unavailable,
        Some(CopyUnavailableReason::Uninitialized)
    );
    assert_eq!(host.gate.settlement_count().await?, "0");
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_leases"
        )
        .await?,
        "0"
    );
    let floor = scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?;
    let reservation = scalar(&store, "SELECT intent AS value FROM _fleet_sync_transfers").await?;
    drop(running);
    drop(store);
    let reopened = open_store(&registry_path, host.clone(), false).await?;
    assert_eq!(
        crate::turso_workflow_tests::view(&reopened, attempt()?).await?,
        before
    );
    // Administrative repair replaces lost copy storage, then uses the actual
    // receiver's creation and complete seed publication path. No row is forged.
    let mut repaired = host
        .copy_connection_at(&directory.path().join("repaired-copy.db"))
        .await?;
    let mut transaction = repaired.begin_with("BEGIN IMMEDIATE").await?;
    assert_eq!(
        fleet_sync_receiver::create_copy(
            &mut CopyScope(&mut transaction),
            &CreateCopy {
                database: database()?,
                epoch: Epoch::new(1)?,
                holder: None,
                origin: CopyOrigin::AwaitingSeed {
                    expected_position: None
                },
            }
        )
        .await?,
        CreateCopyAnswer::CopyCreated
    );
    transaction.commit().await?;
    let previous = host
        .copies
        .lock()
        .await
        .insert(database()?, repaired)
        .ok_or("old copy missing")?;
    previous.close().await?;
    let position = seed_import(&host).await?;
    host.advance();
    let resumed = engine(reopened.clone(), host.clone())?;
    assert_eq!(resumed.drive_due().await?.completed, vec![operation]);
    assert_eq!(
        resumed
            .start::<HolderChangeTask<TestStore, Turso>>(&key, offered.clone())
            .await?,
        operation
    );
    let answer =
        task_answer::<HolderChangeTask<TestStore, Turso>>(&resumed, &key, offered.clone()).await?;
    assert!(
        matches!(answer, HolderChangeAnswer::Granted { epoch, start, .. } if epoch == Epoch::new(2)? && start == position)
    );
    assert_eq!(
        task_answer::<HolderChangeTask<TestStore, Turso>>(&resumed, &key, offered).await?,
        answer
    );
    assert_eq!(host.gate.settlement_count().await?, "1");
    assert_eq!(
        host.gate.settlement_outcome(attempt()?).await?,
        Some(SettleOutcome::Granted {
            epoch: Epoch::new(2)?
        })
    );
    assert_eq!(
        scalar(
            &reopened,
            "SELECT epoch AS value FROM _fleet_sync_epoch_floor"
        )
        .await?,
        floor
    );
    assert_eq!(
        scalar(
            &reopened,
            "SELECT intent AS value FROM _fleet_sync_transfers"
        )
        .await?,
        reservation
    );
    assert_eq!(
        host.calls
            .lock()
            .await
            .iter()
            .filter(|(kind, _)| kind == "fleet-sync.claim-reservation")
            .count(),
        1
    );
    let fences = host
        .calls
        .lock()
        .await
        .iter()
        .filter(|(kind, _)| kind == "fleet-sync.copy-fence")
        .map(|(_, call)| call.clone())
        .collect::<Vec<_>>();
    assert_eq!(fences.len(), 2);
    assert_eq!(fences.first(), fences.last());
    Ok(())
}
