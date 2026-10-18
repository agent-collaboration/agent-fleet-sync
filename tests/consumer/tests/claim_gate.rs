use durable_steps::{CallId, OperationId, StepName, Uuid};
use fleet_sync_lease::HolderChangeGate;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{Consumer, ConsumerStore, TestResult};

fn call(step: &str) -> TestResult<CallId> {
    Ok(CallId {
        operation: OperationId::new(Uuid::now_v7())?,
        step: StepName::new(step)?,
    })
}
#[tokio::test]
async fn settlement_before_reserve_closes_the_claim_after_reopen_and_conflicting_outcome_refuses()
-> TestResult {
    let root = tempfile::tempdir()?;
    let path = root.path().join("consumer.db");
    let store = ConsumerStore::open(&path).await?;
    let consumer = Consumer::new(store.clone())?;
    let request = HolderChangeRequest::Claim {
        attempt: TransferId::new(Uuid::now_v7())?,
        database: DatabaseId::new(Uuid::now_v7())?,
        claimant: PeerId::new(Uuid::now_v7())?,
    };
    let settlement = call("settle")?;
    consumer
        .settle(&request, SettleOutcome::Cancelled, settlement.clone())
        .await?;
    assert!(matches!(
        consumer.reserve(&request, call("reserve")?).await?,
        GateAnswer::Refused { .. }
    ));
    assert!(
        consumer
            .settle(
                &request,
                SettleOutcome::Granted {
                    epoch: Epoch::new(2)?
                },
                call("conflicting")?
            )
            .await
            .is_err()
    );
    store.close().await;
    let store = ConsumerStore::open(&path).await?;
    let consumer = Consumer::new(store.clone())?;
    assert!(matches!(
        consumer.reserve(&request, call("late-reserve")?).await?,
        GateAnswer::Refused { .. }
    ));
    consumer
        .settle(&request, SettleOutcome::Cancelled, settlement.clone())
        .await?;
    let receipts = store.claim_receipts().await?;
    assert_eq!(receipts.len(), 1);
    let receipt = receipts.first().ok_or("claim receipt missing")?;
    assert!(receipt.reserved_by.is_none());
    assert_eq!(receipt.settle_attempts, 2);
    assert!(
        matches!(&receipt.settlement, Some(saved) if saved.call==settlement && saved.outcome==SettleOutcome::Cancelled)
    );
    store.close().await;
    Ok(())
}
