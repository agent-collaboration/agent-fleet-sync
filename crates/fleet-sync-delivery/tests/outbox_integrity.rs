#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use support::*;

#[tokio::test]
async fn failure_status_can_be_written_after_the_custody_body_is_corrupted() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(original) = item(1, "original") else {
                    return Err(durable_steps::StoreError::backend(
                        "fixture is not an original",
                    ));
                };
                let mut outbox = Outbox::<_, TestTypes>::new(scope, limits());
                let admitted = outbox
                    .enqueue(original, peer(9))
                    .await
                    .map_err(storage_error)?;
                assert!(matches!(admitted, OutboxAdmission::Queued { .. }));
                Ok(())
            })
        })
        .await
        .unwrap();
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT payload FROM _fleet_sync_outbox_items WHERE item_id=?1",
                )
                .bind(item_id(1).as_uuid().to_string())
                .fetch_one(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                let raw: String = row
                    .try_get("payload")
                    .map_err(durable_steps::StoreError::backend)?;
                let mut record: serde_json::Value = serde_json::from_str(&raw).unwrap();
                record["value"]["body"] = serde_json::json!("corrupted after admission");
                sqlx_core::query::query::<sqlx_turso::Turso>(
                    "UPDATE _fleet_sync_outbox_items SET payload=?1 WHERE item_id=?2",
                )
                .bind(record.to_string())
                .bind(item_id(1).as_uuid().to_string())
                .execute(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    fixture.store.transaction(|scope|Box::pin(async move {
        let mut outbox=Outbox::<_,TestTypes>::new(scope,limits());
        outbox.record_failure(item_id(1),DeliveryFailureKind::CustodyCorrupt.into()).await.map_err(storage_error)?;
        let status=outbox.status(item_id(1)).await.map_err(storage_error)?;
        assert!(matches!(status.state,OutboxStatusState::Known{status:DeliveryStatus::Refused{reason:DeliveryRefusal::ProcessingFailed{code}},..} if code.as_str()=="custodyCorrupt"));
        Ok(())
    })).await.unwrap();
}
