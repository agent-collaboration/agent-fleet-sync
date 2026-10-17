#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use support::{network::*, *};

fn expiring(id: u64, deadline: i64) -> DeliveryEnvelope<TestTypes> {
    DeliveryEnvelope::Item(
        DeliveryItem::new(
            item_id(id),
            "sender".into(),
            Recipient {
                peer: peer(2),
                address: "inbox".into(),
            },
            Some(SyncTime::from_unix_millis(deadline)),
            format!("body {id}"),
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn deadline_scan_is_atomic_leaves_fetched_custody_alone_and_publishes_the_due_version_once() {
    let network = NetworkFixture::new().await;
    network
        .mailbox
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
                mailbox
                    .submit(peer(1), expiring(2, 20), SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?;
                // Fetch wins the owner transaction before the deadline. Its receipt/report now owns expiry.
                mailbox
                    .fetch(
                        peer(2),
                        DeliveryStream::Items,
                        DeliverySeq::new(0),
                        1,
                        100_000,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                for (id, deadline) in [(1, 50), (3, 75)] {
                    mailbox
                        .submit(
                            peer(1),
                            expiring(id, deadline),
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)?;
                }
                assert_eq!(
                    mailbox
                        .expire_due(SyncTime::from_unix_millis(49))
                        .await
                        .map_err(storage_error)?,
                    Some(SyncTime::from_unix_millis(50))
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    let rolled_back: Result<(), StoreError> = network
        .mailbox
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .expire_due(SyncTime::from_unix_millis(50))
                    .await
                    .map_err(storage_error)?;
                Err(StoreError::backend("force expiry transaction rollback"))
            })
        })
        .await;
    assert!(rolled_back.is_err());
    network.mailbox.store.transaction(|scope| Box::pin(async move {
        let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
        assert!(matches!(mailbox.query(&[item_id(1)]).await.map_err(storage_error)?.statuses[0].state,
            ItemStatusState::Known { status: DeliveryStatus::Sent { .. }, version, .. } if version.value() == 1));
        let row = sqlx_core::query::query::<sqlx_turso::Turso>(
            "SELECT version FROM _fleet_sync_status_owed WHERE item_id=?1",
        ).bind(item_id(1).as_uuid().to_string()).fetch_one(scope.executor()).await.map_err(StoreError::backend)?;
        assert_eq!(row.try_get::<i64, _>("version").unwrap(), 1);
        let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
        assert_eq!(mailbox.expire_due(SyncTime::from_unix_millis(50)).await.map_err(storage_error)?,
            Some(SyncTime::from_unix_millis(75)));
        assert_eq!(mailbox.expire_due(SyncTime::from_unix_millis(50)).await.map_err(storage_error)?,
            Some(SyncTime::from_unix_millis(75)));
        for (id, expired) in [(1, true), (2, false)] {
            let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                "SELECT payload_bytes,record FROM _fleet_sync_mail_items WHERE item_id=?1",
            ).bind(item_id(id).as_uuid().to_string()).fetch_one(scope.executor()).await.map_err(StoreError::backend)?;
            let record: serde_json::Value = serde_json::from_str(&row.try_get::<String, _>("record").unwrap()).unwrap();
            assert_eq!(row.try_get::<i64, _>("payload_bytes").unwrap() == 0, expired);
            assert_eq!(record.pointer("/value/fetched").and_then(serde_json::Value::as_bool), Some(!expired));
            assert_eq!(record.pointer("/value/version").and_then(serde_json::Value::as_u64), Some(if expired { 2 } else { 1 }));
        }
        admit_publications::<_, TestTypes, PublishTask>(scope).await.map_err(storage_error)?;
        Ok(())
    })).await.unwrap();
    // Actual Engine publication tasks carry the due revision into the sender's stream.
    drain(&network.engine(&network.mailbox)).await;
    network
        .status
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let events = StatusStream::<_, TestTypes>::new(scope, peer(1))
                    .fetch(peer(1), StatusCursor::new(0), 20)
                    .await
                    .map_err(storage_error)?
                    .events;
                let due: Vec<_> = events
                    .into_iter()
                    .filter(|event| event.item == item_id(1))
                    .collect();
                assert_eq!(due.len(), 1);
                assert_eq!(due[0].version.value(), 2);
                assert!(matches!(due[0].status, DeliveryStatus::Expired));
                Ok(())
            })
        })
        .await
        .unwrap();
}
