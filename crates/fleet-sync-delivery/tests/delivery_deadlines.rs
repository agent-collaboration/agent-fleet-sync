#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, SyncTime, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use std::sync::atomic::Ordering;
use support::{network::*, *};

async fn enqueue_expiring(network: &NetworkFixture, expiry: i64) {
    network
        .sender
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let original = DeliveryItem::<TestTypes>::new(
                    item_id(1),
                    "sender".into(),
                    Recipient {
                        peer: peer(2),
                        address: "inbox".into(),
                    },
                    Some(SyncTime::from_unix_millis(expiry)),
                    "expiring body".into(),
                )
                .unwrap();
                Outbox::<_, TestTypes>::new(scope, limits())
                    .enqueue_and_admit::<SubmitTask>(original, peer(3))
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
}
async fn assert_sender_expired(network: &NetworkFixture) {
    network
        .sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, TestTypes>::new(scope, limits())
                        .status(item_id(1))
                        .await
                        .map_err(storage_error)?
                        .state,
                    OutboxStatusState::Known {
                        status: DeliveryStatus::Expired,
                        ..
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}
async fn reach_scheduled_held(network: &NetworkFixture) {
    let engine = network.engine(&network.recipient);
    // Drive the real admitted work until its owner reports the scheduled wait.
    let received = engine.drive_due().await.unwrap();
    assert!(received.failed.is_empty());
    let attempted = engine.drive_due().await.unwrap();
    assert!(attempted.failed.is_empty());
    let waiting = if attempted.retried.is_empty() {
        engine.drive_due().await.unwrap()
    } else {
        attempted
    };
    assert!(waiting.failed.is_empty());
    assert_eq!(waiting.retried.len(), 1);
    drain(&engine).await;
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .local_outcome(item_id(1))
                        .await
                        .map_err(storage_error)?,
                    Some(RecipientOutcome::Held)
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}
#[tokio::test]
async fn held_until_does_not_repeat_early_and_the_next_attempt_uses_a_fresh_call() {
    let network = NetworkFixture::new().await;
    network.enqueue(1, "held body").await;
    drain(&network.engine(&network.sender)).await;
    network
        .routes
        .local_answers
        .lock()
        .unwrap()
        .push_back(LocalOutcome::Held {
            until: Some(SyncTime::from_unix_millis(50)),
        });
    network.receive(DeliveryStream::Items).await;
    reach_scheduled_held(&network).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    network.recipient.host.clock.store(49, Ordering::SeqCst);
    drain(&network.engine(&network.recipient)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    network.recipient.host.clock.store(50, Ordering::SeqCst);
    drain(&network.engine(&network.recipient)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 2);
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .local_outcome(item_id(1))
                        .await
                        .map_err(storage_error)?,
                    Some(RecipientOutcome::Delivered { .. })
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}
#[tokio::test]
async fn expiry_before_fetch_never_delivers_and_relay_publishes_final_expired() {
    let network = NetworkFixture::new().await;
    enqueue_expiring(&network, 50).await;
    drain(&network.engine(&network.sender)).await;
    network.advance();
    network.receive(DeliveryStream::Items).await;
    drain(&network.engine(&network.recipient)).await;
    drain(&network.engine(&network.mailbox)).await;
    drain(&network.engine(&network.status)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 0);
    assert_sender_expired(&network).await;
}

#[tokio::test]
async fn expired_on_first_relay_admission_reaches_the_sender_after_the_submit_receipt() {
    let network = NetworkFixture::new().await;
    enqueue_expiring(&network, 50).await;
    network.sender.host.clock.store(50, Ordering::SeqCst);
    // Force the real Submit receipt to reach Outbox before the owed publication.
    drain(&network.engine(&network.sender)).await;
    assert!(
        matches!(sender_status(&network,1).await,OutboxStatusState::Known {status:DeliveryStatus::Expired,version,..} if version.value()==1)
    );
    drain(&network.engine(&network.mailbox)).await;
    drain(&network.engine(&network.status)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 0);
    assert_sender_expired(&network).await;
}

#[tokio::test]
async fn an_initial_expiry_publication_before_the_late_submit_receipt_keeps_the_same_snapshot() {
    let network = NetworkFixture::new().await;
    enqueue_expiring(&network, 50).await;
    let input = network
        .sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT input FROM _fleet_sync_operations",
                )
                .fetch_one(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                let bytes: Vec<u8> = row
                    .try_get("input")
                    .map_err(durable_steps::StoreError::backend)?;
                serde_json::from_slice::<SubmitInput>(&bytes)
                    .map_err(durable_steps::StoreError::backend)
            })
        })
        .await
        .unwrap();
    let reference = input.reference();
    let resolved = network
        .sender
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes>::new(scope, limits())
                    .resolve_submit(&reference)
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    let answer = network
        .mailbox
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let answer = Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .submit(peer(1), resolved.item, SyncTime::from_unix_millis(50))
                    .await
                    .map_err(storage_error)?;
                admit_publications::<_, TestTypes, PublishTask>(scope)
                    .await
                    .map_err(storage_error)?;
                Ok(answer)
            })
        })
        .await
        .unwrap();
    // Real publication tasks commit Expired@1 before the late call answer is applied.
    drain(&network.engine(&network.mailbox)).await;
    drain(&network.engine(&network.status)).await;
    assert!(
        matches!(sender_status(&network,1).await,OutboxStatusState::Known {status:DeliveryStatus::Expired,version,..} if version.value()==1)
    );
    network
        .sender
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes>::new(scope, limits())
                    .mark_submitted(&input, answer)
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    assert!(
        matches!(sender_status(&network,1).await,OutboxStatusState::Known {status:DeliveryStatus::Expired,version,..} if version.value()==1)
    );
    drain(&network.engine(&network.sender)).await;
    assert_eq!(network.routes.sent_bodies.load(Ordering::SeqCst), 0);
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn a_held_item_expires_before_its_later_consumer_deadline_without_a_second_effect() {
    let network = NetworkFixture::new().await;
    enqueue_expiring(&network, 50).await;
    drain(&network.engine(&network.sender)).await;
    network
        .routes
        .local_answers
        .lock()
        .unwrap()
        .push_back(LocalOutcome::Held {
            until: Some(SyncTime::from_unix_millis(100)),
        });
    network.receive(DeliveryStream::Items).await;
    reach_scheduled_held(&network).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    network.advance();
    drain(&network.engine(&network.recipient)).await;
    drain(&network.engine(&network.mailbox)).await;
    drain(&network.engine(&network.status)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    assert_sender_expired(&network).await;
}
