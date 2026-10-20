#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, JournalScope, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use serde_json::Value;
use sqlx_core::row::Row;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use support::{network::*, *};

async fn state(store: &TestStore) -> Value {
    store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT record FROM _fleet_sync_inbox_state",
                )
                .fetch_one(scope.executor())
                .await
                .map_err(StoreError::backend)?;
                Ok(serde_json::from_str(&row.try_get::<String, _>("record").unwrap()).unwrap())
            })
        })
        .await
        .unwrap()
}

async fn reopen_recipient(network: &mut NetworkFixture) {
    let host = network.recipient.host.clone();
    let path = network.recipient.directory.path().join("delivery.db");
    assert_eq!(Arc::strong_count(&network.recipient.store), 1);
    // Drop the actual old store before reopening its database file.
    let placeholder = Fixture::new().await;
    let old = std::mem::replace(&mut network.recipient.store, placeholder.store.clone());
    drop(old);
    network.recipient.store = Arc::new(TestStore::open_and_migrate(path, host).await.unwrap());
    *network.routes.recipient.lock().unwrap() = Arc::downgrade(&network.recipient.store);
}

#[tokio::test]
async fn capacity_taken_after_fetch_budget_waits_with_saved_batch_and_recovers_after_reopen() {
    let mut network = NetworkFixture::new().await;
    let capacity = serde_json::to_vec(&item(1, "remote A")).unwrap().len() as u64
        + u64::from(limits().settlement_allowance())
        + 128; // FetchEntry framing also fits its byte budget.
    let config = DeliveryTaskConfig {
        local: LocalDeliveryConfig {
            limits: configured_limits(100_000, 20, capacity),
            ..config().local
        },
        ..config()
    };
    network.enqueue(1, "remote A").await;
    drain(&network.engine(&network.sender)).await;
    network.receive(DeliveryStream::Items).await;
    let reached = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *network.routes.fetch_barrier.lock().unwrap() = Some(FetchBarrier {
        reached: reached.clone(),
        release: release.clone(),
    });
    let engine = network.engine_with_config(&network.recipient, config);
    let drive = tokio::spawn(async move { engine.drive_due().await.unwrap() });
    tokio::time::timeout(Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    let local = network
        .recipient
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(local) = item(9, "local A!") else {
                    return Err(StoreError::backend("fixture must be a local item"));
                };
                Inbox::<_, TestTypes>::new(scope, peer(2), config.local.limits)
                    .enqueue_local(local, SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
        .unwrap();
    release.notify_one();
    let report = tokio::time::timeout(Duration::from_secs(5), drive)
        .await
        .unwrap()
        .unwrap();
    assert!(
        report.failed.is_empty(),
        "valid batch pressure must not fail: {report:?}"
    );
    assert_eq!(report.retried.len(), 1, "store must wait for room");
    let pending = state(&network.recipient.store).await;
    assert!(
        pending
            .pointer("/items/active")
            .is_some_and(|value| !value.is_null())
    );
    assert_eq!(
        pending.pointer("/items/received"),
        Some(&serde_json::json!(0))
    );
    network.recipient.store.transaction(|scope| Box::pin(async move {
        let rows = sqlx_core::query::query::<sqlx_turso::Turso>(
            "SELECT step_name, CAST(output AS TEXT) AS output FROM _fleet_sync_steps ORDER BY position",
        ).fetch_all(scope.executor()).await.map_err(StoreError::backend)?;
        assert_eq!(rows.len(), 2, "only budget and fetched batch are saved");
        assert_eq!(rows[1].try_get::<String, _>("step_name").unwrap(), "fetch");
        assert!(rows[1].try_get::<String, _>("output").unwrap().contains("remote A"));
        Ok(())
    })).await.unwrap();
    reopen_recipient(&mut network).await;
    assert_eq!(state(&network.recipient.store).await, pending);
    network
        .recipient
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                scope
                    .admit::<DeliverTask>(
                        &delivery_request_key(&(local.item, local.attempt)).unwrap(),
                        &local,
                    )
                    .await
                    .map_err(StoreError::backend)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    drain(&network.engine_with_config(&network.recipient, config)).await;
    network.advance();
    drain(&network.engine_with_config(&network.recipient, config)).await;
    drain(&network.engine(&network.mailbox)).await;
    drain(&network.engine(&network.status)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 2);
    assert!(matches!(sender_status(&network, 1).await,
        OutboxStatusState::Known { status: DeliveryStatus::Delivered { acceptance }, .. }
        if acceptance.receipt == "remote A"));
    network
        .mailbox
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .retire(peer(1), &call("retire-pressure"), &[item_id(1)])
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    network.receive(DeliveryStream::Controls).await;
    drain(&network.engine_with_config(&network.recipient, config)).await;
    network.enqueue(2, "remote B").await;
    drain(&network.engine(&network.sender)).await;
    network.receive(DeliveryStream::Items).await;
    drain(&network.engine_with_config(&network.recipient, config)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 3);
    let final_state = state(&network.recipient.store).await;
    for (stream, through) in [("items", 2), ("controls", 1)] {
        assert_eq!(
            final_state.pointer(&format!("/{stream}/received")),
            Some(&serde_json::json!(through))
        );
        assert_eq!(
            final_state.pointer(&format!("/{stream}/acknowledged")),
            Some(&serde_json::json!(through))
        );
        assert!(
            final_state
                .pointer(&format!("/{stream}/active"))
                .unwrap()
                .is_null()
        );
    }
    reopen_recipient(&mut network).await;
    assert_eq!(state(&network.recipient.store).await, final_state);
    drain(&network.engine_with_config(&network.recipient, config)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn terminal_receive_failure_clears_only_its_active_cycle_so_a_new_trigger_drains() {
    let network = NetworkFixture::new().await;
    network.enqueue(1, "after invalid answer").await;
    drain(&network.engine(&network.sender)).await;
    network.receive(DeliveryStream::Items).await;
    network.routes.invalid_fetch.store(true, Ordering::SeqCst);
    let failed = network
        .engine(&network.recipient)
        .drive_due()
        .await
        .unwrap();
    assert_eq!(failed.failed.len(), 1);
    assert!(
        state(&network.recipient.store)
            .await
            .pointer("/items/active")
            .unwrap()
            .is_null(),
        "terminal receive must release cycle ownership"
    );
    network.receive(DeliveryStream::Items).await;
    drain(&network.engine(&network.recipient)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    let state = state(&network.recipient.store).await;
    assert_eq!(
        state.pointer("/items/received"),
        Some(&serde_json::json!(1))
    );
    assert_eq!(
        state.pointer("/items/acknowledged"),
        Some(&serde_json::json!(1))
    );
}
