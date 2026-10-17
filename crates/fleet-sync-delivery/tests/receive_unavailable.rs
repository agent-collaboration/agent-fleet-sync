#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use std::sync::atomic::Ordering;
use support::network::*;

#[tokio::test]
async fn mailbox_unavailability_keeps_the_same_cycle_pending_and_preserves_prior_custody() {
    for received_unavailable in [false, true] {
        let network = NetworkFixture::new().await;
        network
            .enqueue(1, "recipient custody remains accepted")
            .await;
        drain(&network.engine(&network.sender)).await;
        if received_unavailable {
            network
                .routes
                .unavailable_receipt
                .store(true, Ordering::SeqCst);
        } else {
            network
                .routes
                .unavailable_fetch
                .store(true, Ordering::SeqCst);
        }
        network.receive(DeliveryStream::Items).await;
        let report = network
            .engine(&network.recipient)
            .drive_due()
            .await
            .unwrap();
        assert!(report.failed.is_empty());
        assert_eq!(report.retried.len(), 1);
        assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 0);
        network
            .recipient
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                        "SELECT record FROM _fleet_sync_inbox_state",
                    )
                    .fetch_one(scope.executor())
                    .await
                    .map_err(StoreError::backend)?;
                    let state: serde_json::Value =
                        serde_json::from_str(&row.try_get::<String, _>("record").unwrap()).unwrap();
                    assert!(!state.pointer("/items/active").unwrap().is_null());
                    assert_eq!(
                        state
                            .pointer("/items/received")
                            .and_then(serde_json::Value::as_u64),
                        Some(u64::from(received_unavailable))
                    );
                    assert_eq!(
                        state
                            .pointer("/items/acknowledged")
                            .and_then(serde_json::Value::as_u64),
                        Some(0)
                    );
                    let rows = sqlx_core::query::query::<sqlx_turso::Turso>(
                        "SELECT payload_bytes FROM _fleet_sync_inbox_items",
                    )
                    .fetch_all(scope.executor())
                    .await
                    .map_err(StoreError::backend)?;
                    assert_eq!(rows.len(), usize::from(received_unavailable));
                    if let Some(row) = rows.first() {
                        assert!(row.try_get::<i64, _>("payload_bytes").unwrap() > 0);
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
        network
            .mailbox
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                        "SELECT record,payload_bytes FROM _fleet_sync_mail_items",
                    )
                    .fetch_one(scope.executor())
                    .await
                    .map_err(StoreError::backend)?;
                    let record: serde_json::Value =
                        serde_json::from_str(&row.try_get::<String, _>("record").unwrap()).unwrap();
                    assert!(row.try_get::<i64, _>("payload_bytes").unwrap() > 0);
                    assert!(record.pointer("/value/releasedAt").unwrap().is_null());
                    assert_eq!(
                        record
                            .pointer("/value/fetched")
                            .and_then(serde_json::Value::as_bool),
                        Some(received_unavailable)
                    );
                    Ok(())
                })
            })
            .await
            .unwrap();
        // A trigger during the wait coalesces rather than replacing the active cycle.
        network.receive(DeliveryStream::Items).await;
        network
            .routes
            .unavailable_fetch
            .store(false, Ordering::SeqCst);
        network
            .routes
            .unavailable_receipt
            .store(false, Ordering::SeqCst);
        network.advance();
        drain(&network.engine(&network.recipient)).await;
        drain(&network.engine(&network.mailbox)).await;
        drain(&network.engine(&network.status)).await;
        assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            sender_status(&network, 1).await,
            OutboxStatusState::Known {
                status: DeliveryStatus::Delivered { .. },
                ..
            }
        ));
        network
            .recipient
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                        "SELECT record FROM _fleet_sync_inbox_state",
                    )
                    .fetch_one(scope.executor())
                    .await
                    .map_err(StoreError::backend)?;
                    let state: serde_json::Value =
                        serde_json::from_str(&row.try_get::<String, _>("record").unwrap()).unwrap();
                    assert_eq!(
                        state
                            .pointer("/items/received")
                            .and_then(serde_json::Value::as_u64),
                        Some(1)
                    );
                    assert_eq!(
                        state
                            .pointer("/items/acknowledged")
                            .and_then(serde_json::Value::as_u64),
                        Some(1)
                    );
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
}
