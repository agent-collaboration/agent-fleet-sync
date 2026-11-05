//! Real engine/store setup shared by expiry timing and recovery scenarios.
use super::{network::*, *};
use durable_steps::{AtomicStore, OperationId, Task, storage::TransactionScope};
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;

pub const DEADLINE: i64 = 5;
pub fn at(milliseconds: i64) -> SyncTime {
    SyncTime::from_unix_millis(milliseconds)
}
pub fn clock(network: &NetworkFixture, milliseconds: i64) {
    for fixture in [
        &network.sender,
        &network.relay,
        &network.recipient,
        &network.status,
    ] {
        fixture
            .host
            .clock
            .store(u64::try_from(milliseconds).unwrap(), Ordering::SeqCst);
    }
}
pub async fn seed(network: &NetworkFixture) {
    network
        .sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let item = DeliveryItem::<TestTypes>::new(
                    item_id(1),
                    "sender".into(),
                    Recipient {
                        peer: peer(2),
                        address: "inbox".into(),
                    },
                    Some(at(DEADLINE)),
                    "deadline body".into(),
                )
                .unwrap();
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .enqueue_and_admit::<SubmitTask, RecordRemoval>(
                        item,
                        peer(3),
                        Submitter::Machine(peer(1)),
                        at(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let submitted = network.engine(&network.sender).drive_due().await.unwrap();
    assert_eq!(submitted.completed.len(), 1);
    assert!(submitted.failed.is_empty());
}
pub async fn push_operation(store: &TestStore) -> OperationId {
    store
        .transaction(|scope| {
            Box::pin(async move {
                let row =
                    query::<Turso>("SELECT operation_id FROM _fleet_sync_operations WHERE kind=?1")
                        .bind(PushTask::KIND)
                        .fetch_one(scope.executor())
                        .await?;
                OperationId::try_from(row.try_get::<String, _>("operation_id")?)
                    .map_err(durable_steps::StoreError::backend)
            })
        })
        .await
        .unwrap()
}
pub async fn recipient_rows(network: &NetworkFixture) -> (i64, i64) {
    network.recipient.store.transaction(|scope|Box::pin(async move {
        let row=query::<Turso>("SELECT (SELECT count(*) FROM _fleet_sync_inbox_items) AS items,(SELECT count(*) FROM _fleet_sync_operations) AS operations").fetch_one(scope.executor()).await?;
        Ok((row.try_get("items")?,row.try_get("operations")?))
    })).await.unwrap()
}
pub async fn publish(network: &NetworkFixture) {
    drain(&network.engine(&network.relay)).await;
    drain(&network.engine(&network.status)).await;
}
