#![allow(clippy::unwrap_used)]
use super::cancellation_support::*;
use super::support;
use durable_steps::AtomicStore;
use fleet_sync_delivery::*;
use std::sync::atomic::Ordering;
use support::{network::*, *};

#[tokio::test]
async fn failed_preserved_outcome_projection_rolls_back_push_promotion_and_report_debt() {
    use super::projection::{ProjectionConsumer, prepare_projection_table, read_committed_state};
    let fixture = Fixture::new().await;
    require_local_attempt(enqueue_original(&fixture, 1, limits()).await).unwrap();
    enqueue_cancel(&fixture, 2, 1).await;
    prepare_projection_table(&fixture).await;
    let before = snapshot(&fixture).await;
    let consumer = std::sync::Arc::new(ProjectionConsumer::default());
    consumer.refuse_outcome.store(true, Ordering::SeqCst);
    let error = fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                accept_fixture_push(
                    scope,
                    limits(),
                    DeliveryEnvelope::Item(local_item(1)),
                    consumer.as_ref(),
                    SyncTime::from_unix_millis(0),
                )
                .await
                .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("outcome projection refused"));
    assert_eq!(snapshot(&fixture).await, before);
    assert!(read_committed_state(&fixture).await.projections.is_empty());
}

#[tokio::test]
async fn push_promotes_a_prevented_local_original_and_reports_once_across_reopen() {
    let mut network = NetworkFixture::new().await;
    require_local_attempt(enqueue_original(&network.recipient, 1, limits()).await).unwrap();
    enqueue_cancel(&network.recipient, 2, 1).await;
    assert!(matches!(outcome(&network.recipient, 1).await,
        Some(RecipientOutcome::Prevented { cancel }) if cancel == item_id(2)));
    assert!(snapshot(&network.recipient).await.reports.is_empty());

    network.enqueue(1, "local body").await;
    drain(&network.engine(&network.sender)).await;
    drain(&network.engine(&network.relay)).await;
    let promoted = snapshot(&network.recipient).await;
    assert_eq!(
        promoted.reports.len(),
        1,
        "Push must owe the preserved outcome"
    );
    network.recipient = reopen(network.recipient).await;
    *network.routes.recipient.lock().unwrap() = std::sync::Arc::downgrade(&network.recipient.store);
    assert_eq!(snapshot(&network.recipient).await, promoted);
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    accept_fixture_push(
                        scope,
                        limits(),
                        DeliveryEnvelope::Item(local_item(1)),
                        &TestConsumer,
                        SyncTime::from_unix_millis(0)
                    )
                    .await
                    .map_err(storage_error)?,
                    PushResult::Replayed { .. }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(
        snapshot(&network.recipient).await,
        promoted,
        "replay must not renew report debt"
    );
    let report = network
        .engine(&network.recipient)
        .drive_due()
        .await
        .unwrap();
    assert_eq!(
        report.failed.len(),
        1,
        "a terminal refusal must finish the Report operation"
    );
    assert!(report.retried.is_empty());
    assert_eq!(network.routes.report_calls.load(Ordering::SeqCst), 1);
    network.recipient = reopen(network.recipient).await;
    *network.routes.recipient.lock().unwrap() = std::sync::Arc::downgrade(&network.recipient.store);
    for millis in [60_001, 3_600_000, 86_400_000] {
        network.recipient.host.clock.store(millis, Ordering::SeqCst);
        drain(&network.engine(&network.recipient)).await;
    }
    assert_eq!(
        network.routes.report_calls.load(Ordering::SeqCst),
        1,
        "reopen must not retry a terminal Report"
    );
    drain(&network.engine(&network.relay)).await;
    drain(&network.engine(&network.status)).await;
    assert!(matches!(
        sender_status(&network, 1).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Cancelled,
            ..
        }
    ));
    assert_eq!(
        snapshot(&network.recipient).await.reports.len(),
        1,
        "refused report evidence remains until retention"
    );
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn local_admission_leaves_relayed_cancel_pending_until_normal_completion() {
    let network = NetworkFixture::new().await;
    // This is a real remote-control custody setup, not the local cancel entry.
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let answer = accept_fixture_push(
                    scope,
                    limits(),
                    DeliveryEnvelope::Cancel(local_cancel(2, 1, "remote sender")),
                    &TestConsumer,
                    SyncTime::from_unix_millis(0),
                )
                .await
                .map_err(storage_error)?;
                assert!(matches!(answer, PushResult::Accepted { .. }));
                let mut inbox =
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings);
                assert!(
                    inbox
                        .pending_report(item_id(2))
                        .await
                        .map_err(storage_error)?
                        .is_none()
                );
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
                let admitted =
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                        .enqueue_local_and_admit::<DeliverTask, RecordRemoval, _>(
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
                let answer = accept_fixture_push(
                    scope,
                    limits(),
                    DeliveryEnvelope::Item(local_item(1)),
                    &TestConsumer,
                    SyncTime::from_unix_millis(0),
                )
                .await
                .map_err(storage_error)?;
                assert!(matches!(answer, PushResult::Accepted { .. }));
                let mut inbox =
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings);
                assert!(
                    inbox
                        .pending_report(item_id(1))
                        .await
                        .map_err(storage_error)?
                        .is_none()
                );
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
        let report = Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings).resolve_report(reference).await.map_err(storage_error)?;
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
