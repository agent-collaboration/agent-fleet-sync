#![allow(clippy::unwrap_used)]
use super::support;
use durable_steps::{AtomicStore, SyncTime};
use fleet_sync_delivery::*;
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
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .enqueue_and_admit::<SubmitTask, RecordRemoval>(
                        original,
                        peer(3),
                        fleet_sync_delivery::Submitter::Machine(peer(1)),
                        SyncTime::from_unix_millis(0),
                    )
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
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
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
    let delivered = engine.drive_due().await.unwrap();
    assert!(delivered.failed.is_empty());
    let waiting = engine.drive_due().await.unwrap();
    assert!(waiting.failed.is_empty());
    assert_eq!(waiting.retried.len(), 1);
    drain(&engine).await;
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
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
    drain(&network.engine(&network.relay)).await;
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
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
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
async fn expiry_before_push_never_delivers_and_relay_publishes_final_expired() {
    let network = NetworkFixture::new().await;
    enqueue_expiring(&network, 50).await;
    drain(&network.engine(&network.sender)).await;
    network.advance();
    drain(&network.engine(&network.relay)).await;
    drain(&network.engine(&network.recipient)).await;
    drain(&network.engine(&network.relay)).await;
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
    drain(&network.engine(&network.relay)).await;
    drain(&network.engine(&network.status)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 0);
    assert_sender_expired(&network).await;
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
    drain(&network.engine(&network.relay)).await;
    reach_scheduled_held(&network).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    network.advance();
    drain(&network.engine(&network.recipient)).await;
    drain(&network.engine(&network.relay)).await;
    drain(&network.engine(&network.status)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    assert_sender_expired(&network).await;
}
