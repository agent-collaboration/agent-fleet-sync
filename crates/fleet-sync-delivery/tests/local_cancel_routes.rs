#![allow(clippy::unwrap_used)]
#[path = "support/local_cancellation.rs"]
mod local_cancellation;
mod support;
use durable_steps::AtomicStore;
use fleet_sync_delivery::*;
use local_cancellation::*;
use std::sync::atomic::Ordering;
use support::{network::*, *};

#[tokio::test]
async fn local_admission_leaves_relayed_cancel_pending_until_normal_completion() {
    let network = NetworkFixture::new().await;
    // This is a real remote-control custody setup, not the local cancel entry.
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                let cycle = inbox
                    .begin_receive(DeliveryStream::Controls)
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                let effects = inbox
                    .store_batch(
                        cycle,
                        FetchResult {
                            stream: DeliveryStream::Controls,
                            entries: vec![FetchEntry::Item {
                                seq: DeliverySeq::new(1),
                                item: DeliveryEnvelope::Cancel(local_cancel(2, 1, "remote sender")),
                            }],
                            through: DeliverySeq::new(1),
                        },
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                assert!(effects.attempts.is_empty() && effects.reports.is_empty());
                Ok(())
            })
        })
        .await
        .unwrap();
    let before = snapshot(&network.recipient).await;
    assert!(outcome(&network.recipient, 2).await.is_none());
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let admitted = Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local_and_admit::<DeliverTask, _>(
                        local_item(1),
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                assert!(
                    matches!(admitted, LocalItemAdmission::Attempt(_)),
                    "a Relayed control must not block local admission"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    let admitted = snapshot(&network.recipient).await;
    assert_eq!(
        admitted.state, before.state,
        "local admission changed receive/ack state"
    );
    assert_eq!(admitted.links, before.links);
    assert!(
        admitted.custody.contains(before.custody.first().unwrap()),
        "local admission changed the Relayed control"
    );
    assert!(admitted.reports.is_empty());
    assert!(outcome(&network.recipient, 2).await.is_none());
    let input = DeliveryAttempt {
        item: item_id(1),
        attempt: 0,
        not_before: SyncTime::from_unix_millis(0),
    };
    let engine = network.engine(&network.recipient);
    engine
        .start_and_settle::<DeliverTask>(
            &delivery_request_key(&(input.item, input.attempt)).unwrap(),
            input,
        )
        .await
        .unwrap();
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        outcome(&network.recipient, 1).await,
        Some(RecipientOutcome::Delivered { .. })
    ));
    // Existing completion owns this later resolution and its report obligation.
    assert!(matches!(
        outcome(&network.recipient, 2).await,
        Some(RecipientOutcome::Refused {
            reason: DeliveryRefusal::CancelSenderMismatch
        })
    ));
    assert_eq!(snapshot(&network.recipient).await.reports.len(), 1);
}

#[tokio::test]
async fn local_cancel_of_a_relayed_original_returns_its_real_report_obligation() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                let cycle = inbox
                    .begin_receive(DeliveryStream::Items)
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                let effects = inbox
                    .store_batch(
                        cycle,
                        FetchResult {
                            stream: DeliveryStream::Items,
                            entries: vec![FetchEntry::Item {
                                seq: DeliverySeq::new(1),
                                item: DeliveryEnvelope::Item(local_item(1)),
                            }],
                            through: DeliverySeq::new(1),
                        },
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                assert_eq!(effects.attempts.len(), 1);
                assert!(effects.reports.is_empty());
                Ok(())
            })
        })
        .await
        .unwrap();
    let before = snapshot(&fixture).await;
    let admission = enqueue_cancel(&fixture, 2, 1).await;
    assert_eq!(admission.reference.item, item_id(2));
    assert!(admission.effects.attempts.is_empty());
    assert_eq!(admission.effects.reports.len(), 1);
    let reference = *admission.effects.reports.first().unwrap();
    assert_eq!(reference.item, item_id(1));
    fixture.store.transaction(move |scope| Box::pin(async move {
        let report = Inbox::<_, TestTypes>::new(scope, peer(2), limits()).resolve_report(reference).await.map_err(storage_error)?;
        assert!(matches!(report.outcome, RecipientOutcome::Prevented { cancel } if cancel == item_id(2)));
        Ok(())
    })).await.unwrap();
    let after = snapshot(&fixture).await;
    assert_eq!(after.state, before.state);
    assert_eq!(after.reports.len(), 1);
    assert!(matches!(
        outcome(&fixture, 2).await,
        Some(RecipientOutcome::Prevented { .. })
    ));
}
