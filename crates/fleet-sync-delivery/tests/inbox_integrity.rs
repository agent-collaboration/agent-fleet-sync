#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, SyncTime, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use support::*;

#[tokio::test]
async fn empty_cycles_are_fresh_and_overlapping_triggers_coalesce() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                let first = inbox
                    .begin_receive(DeliveryStream::Items)
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                assert!(
                    inbox
                        .begin_receive(DeliveryStream::Items)
                        .await
                        .map_err(storage_error)?
                        .is_none()
                );
                assert!(
                    inbox
                        .begin_receive(DeliveryStream::Items)
                        .await
                        .map_err(storage_error)?
                        .is_none()
                );
                let rerun = inbox
                    .finish_receive(first)
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                assert_eq!(rerun.cycle.value(), first.cycle.value() + 1);
                assert!(
                    inbox
                        .finish_receive(rerun)
                        .await
                        .map_err(storage_error)?
                        .is_none()
                );
                let fresh = inbox
                    .begin_receive(DeliveryStream::Items)
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                assert!(fresh.cycle > rerun.cycle);
                assert_eq!(fresh.after.value(), 0);
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn a_full_inbox_still_stores_a_reserved_cancel_from_controls() {
    let fixture = Fixture::new().await;
    fixture.store.transaction(|scope|Box::pin(async move {
        let original=item(1,"held original");
        let configured=configured_limits(100_000,20,serde_json::to_vec(&original).unwrap().len() as u64+u64::from(limits().settlement_allowance()));
        let DeliveryEnvelope::Item(original)=original else {return Err(durable_steps::StoreError::backend("fixture is not an item"));};
        let mut inbox=Inbox::<_,TestTypes>::new(scope,peer(2),configured);
        inbox.enqueue_local(original,SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        let items=inbox.begin_receive(DeliveryStream::Items).await.map_err(storage_error)?.unwrap();
        let controls=inbox.begin_receive(DeliveryStream::Controls).await.map_err(storage_error)?.unwrap();
        assert_eq!(inbox.receive_budget(items,10,100_000).await.map_err(storage_error)?.0,0);
        assert_eq!(inbox.receive_budget(controls,10,100_000).await.map_err(storage_error)?.0,10);
        let effects=inbox.store_batch(controls,FetchResult{stream:DeliveryStream::Controls,entries:vec![FetchEntry::Item{seq:DeliverySeq::new(1),item:cancel(2,1)}],through:DeliverySeq::new(1)},&TestConsumer,SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        assert!(effects.attempts.is_empty());
        assert!(matches!(inbox.local_outcome(item_id(1)).await.map_err(storage_error)?,Some(RecipientOutcome::Prevented{cancel}) if cancel==item_id(2)));
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn failure_outcome_survives_corrupted_recipient_custody() {
    let fixture = Fixture::new().await;
    let attempt = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(original) = item(1, "original") else {
                    return Err(durable_steps::StoreError::backend("fixture is not an item"));
                };
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local(original, SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?
                    .ok_or_else(|| durable_steps::StoreError::backend("fixture did not admit"))
            })
        })
        .await
        .unwrap();
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT payload FROM _fleet_sync_inbox_items WHERE item_id=?1",
                )
                .bind(item_id(1).as_uuid().to_string())
                .fetch_one(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                let raw: String = row
                    .try_get("payload")
                    .map_err(durable_steps::StoreError::backend)?;
                let mut record: serde_json::Value = serde_json::from_str(&raw).unwrap();
                record["value"]["body"] = serde_json::json!("corrupted recipient body");
                sqlx_core::query::query::<sqlx_turso::Turso>(
                    "UPDATE _fleet_sync_inbox_items SET payload=?1 WHERE item_id=?2",
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
    fixture.store.transaction(move|scope|Box::pin(async move {
        let mut inbox=Inbox::<_,TestTypes>::new(scope,peer(2),limits());
        inbox.record_failure(attempt,DeliveryFailureKind::CustodyCorrupt,&TestConsumer,SyncTime::from_unix_millis(0)).await.map_err(storage_error)?;
        assert!(matches!(inbox.local_outcome(item_id(1)).await.map_err(storage_error)?,Some(RecipientOutcome::Refused{reason:DeliveryRefusal::ProcessingFailed{kind}}) if kind.as_str()=="custodyCorrupt"));
        Ok(())
    })).await.unwrap();
}
