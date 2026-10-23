#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use support::*;

async fn lost_body() -> Fixture {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .submit(
                        peer(1),
                        item(1, "required custody"),
                        SyncTime::from_unix_millis(0),
                        "fixture-route".to_owned(),
                        None,
                    )
                    .await
                    .map_err(storage_error)?;
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT record FROM _fleet_sync_mail_items WHERE item_id=?1",
                )
                .bind(item_id(1).as_uuid().to_string())
                .fetch_one(scope.executor())
                .await
                .map_err(StoreError::backend)?;
                let mut record: serde_json::Value =
                    serde_json::from_str(&row.try_get::<String, _>("record").unwrap()).unwrap();
                *record.pointer_mut("/value/payload").unwrap() = serde_json::Value::Null;
                sqlx_core::query::query::<sqlx_turso::Turso>(
                    "UPDATE _fleet_sync_mail_items SET record=?1, payload_bytes=0 WHERE item_id=?2",
                )
                .bind(record.to_string())
                .bind(item_id(1).as_uuid().to_string())
                .execute(scope.executor())
                .await
                .map_err(StoreError::backend)?;
                // A later healthy row must never let Fetch silently advance past lost custody.
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .submit(
                        peer(1),
                        item(2, "later healthy"),
                        SyncTime::from_unix_millis(0),
                        "fixture-route".to_owned(),
                        None,
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    fixture
}

#[tokio::test]
async fn query_rejects_unreleased_nonterminal_custody_without_its_body() {
    let fixture = lost_body().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .query(&[item_id(1)])
                        .await,
                    Err(DeliveryError::Corrupt { .. })
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn replay_rejects_unreleased_nonterminal_custody_without_its_body() {
    let fixture = lost_body().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .submit(
                            peer(1),
                            item(1, "required custody"),
                            SyncTime::from_unix_millis(1),
                            "fixture-route".to_owned(),
                            None
                        )
                        .await,
                    Err(DeliveryError::Corrupt { .. })
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn fetch_validates_missing_body_before_filtering_or_advancing_to_later_items() {
    let fixture = lost_body().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .fetch(
                            peer(2),
                            DeliveryStream::Items,
                            DeliverySeq::new(0),
                            10,
                            100_000,
                            SyncTime::from_unix_millis(1)
                        )
                        .await,
                    Err(DeliveryError::Corrupt { .. })
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn received_rejects_missing_custody_before_advancing_the_relay_receipt() {
    let fixture = lost_body().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .received(
                            peer(2),
                            DeliveryStream::Items,
                            DeliverySeq::new(1),
                            SyncTime::from_unix_millis(1)
                        )
                        .await,
                    Err(DeliveryError::Corrupt { .. })
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn received_terminal_and_compacted_rows_without_bodies_remain_valid_and_fetch_skips_them() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
                mailbox
                    .submit(
                        peer(1),
                        item(1, "received"),
                        SyncTime::from_unix_millis(0),
                        "fixture-route".to_owned(),
                        None,
                    )
                    .await
                    .map_err(storage_error)?;
                mailbox
                    .fetch(
                        peer(2),
                        DeliveryStream::Items,
                        DeliverySeq::new(0),
                        1,
                        100_000,
                        SyncTime::from_unix_millis(1),
                    )
                    .await
                    .map_err(storage_error)?;
                mailbox
                    .received(
                        peer(2),
                        DeliveryStream::Items,
                        DeliverySeq::new(1),
                        SyncTime::from_unix_millis(2),
                    )
                    .await
                    .map_err(storage_error)?;
                for id in [2, 3] {
                    let expired = DeliveryEnvelope::Item(
                        DeliveryItem::<TestTypes>::new(
                            item_id(id),
                            "sender".into(),
                            Recipient {
                                peer: peer(2),
                                address: "inbox".into(),
                            },
                            Some(SyncTime::from_unix_millis(1)),
                            "expired".into(),
                        )
                        .unwrap(),
                    );
                    mailbox
                        .submit(
                            peer(1),
                            expired,
                            SyncTime::from_unix_millis(2),
                            "fixture-route".to_owned(),
                            None,
                        )
                        .await
                        .map_err(storage_error)?;
                }
                mailbox
                    .retire(peer(1), &call("compact-positive"), &[item_id(3)])
                    .await
                    .map_err(storage_error)?;
                let statuses = mailbox
                    .query(&[item_id(1), item_id(2), item_id(3)])
                    .await
                    .map_err(storage_error)?
                    .statuses;
                assert!(matches!(
                    &statuses[0].state,
                    ItemStatusState::Known {
                        status: DeliveryStatus::AtRecipient,
                        ..
                    }
                ));
                for status in &statuses[1..] {
                    assert!(matches!(
                        status.state,
                        ItemStatusState::Known {
                            status: DeliveryStatus::Expired,
                            ..
                        }
                    ));
                }
                let fetched = mailbox
                    .fetch(
                        peer(2),
                        DeliveryStream::Items,
                        DeliverySeq::new(0),
                        10,
                        100_000,
                        SyncTime::from_unix_millis(3),
                    )
                    .await
                    .map_err(storage_error)?;
                assert!(fetched.entries.is_empty());
                Ok(())
            })
        })
        .await
        .unwrap();
}
