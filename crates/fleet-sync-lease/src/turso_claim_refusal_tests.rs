//! Every typed terminal copy refusal after a claim reservation settles that reservation.
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
async fn retire_copy(host: &TestHost) -> TestResult {
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
                target_schema_max: 10,
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
async fn copy_changes_after_status_settle_unavailable_retired_and_stale_claim_fences() -> TestResult
{
    for change in ["missing", "retired", "moved"] {
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
                "missing" => {
                    // Model a lost copy record after status answered, before fence delivery.
                    let mut copies = host.copies.lock().await;
                    let copy = copies.get_mut(&database()?).ok_or("copy missing")?;
                    sqlx_core::query::query::<Turso>("DELETE FROM _fleet_sync_copy")
                        .execute(copy)
                        .await?;
                }
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
            "missing" => HolderChangeRefusal::Unavailable,
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
