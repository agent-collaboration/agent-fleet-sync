#![allow(clippy::unwrap_used)]
#[path = "support/local_cancellation.rs"]
mod local_cancellation;
mod support;
use durable_steps::{AtomicStore, Engine, JournalScope, Task, storage::TransactionScope};
use fleet_sync_delivery::*;
use local_cancellation::*;
use sqlx_core::query::query;
use sqlx_turso::Turso;
use std::{sync::Arc, time::Duration};
use support::{
    interleaving::{AfterAnswer, InterleavedDeliver},
    network::*,
    *,
};

fn reference(id: u64) -> DeliveryItemRef {
    let item = local_item(id);
    DeliveryItemRef {
        item: item.id(),
        digest: item.digest().clone(),
    }
}
async fn retire(fixture: &Fixture, reference: DeliveryItemRef) -> LocalRetirement {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .retire_local(&reference)
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn retirement_waits_for_pending_delivery_after_cancellation() {
    let network = NetworkFixture::new().await;
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local_and_admit::<DeliverTask, _>(
                        local_item(1),
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    enqueue_cancel(&network.recipient, 2, 1).await;
    assert!(matches!(
        outcome(&network.recipient, 1).await,
        Some(RecipientOutcome::Prevented { .. })
    ));
    let before = snapshot(&network.recipient).await;
    let retired = retire(&network.recipient, reference(1)).await;
    // Drive even if the old retirement returned success: the regression must
    // expose the real missing-header failure, rather than only a new answer.
    let report = network.engine(&network.recipient).drive_due().await;
    assert!(
        report.is_ok(),
        "pending cancelled delivery lost its header: {report:?}"
    );
    assert!(
        matches!(
            retired,
            LocalRetirement::Refused(LocalRetirementRefusal::DeliveryInFlight)
        ),
        "{retired:?}"
    );
    assert!(report.unwrap().failed.is_empty());
    assert_eq!(snapshot(&network.recipient).await, before);
    assert_eq!(
        retire(&network.recipient, reference(1)).await,
        LocalRetirement::Retired
    );
}

#[tokio::test]
async fn current_header_attempt_does_not_hide_an_older_unsettled_delivery() {
    let network = NetworkFixture::new().await;
    network
        .routes
        .local_answers
        .lock()
        .unwrap()
        .push_back(LocalOutcome::Held { until: None });
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local_and_admit::<DeliverTask, _>(
                        local_item(1),
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let reached = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let mut engine = Engine::new(
        network.recipient.store.clone(),
        network.recipient.host.clone(),
    );
    engine
        .register(InterleavedDeliver::new(
            network.recipient.store.clone(),
            network.recipient.host.clone(),
            AfterAnswer::OutcomeCheckpoint {
                reached: reached.clone(),
                resume: resume.clone(),
            },
        ))
        .unwrap();
    let engine = Arc::new(engine);
    let running = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.drive_due().await })
    };
    tokio::time::timeout(Duration::from_secs(3), reached.notified())
        .await
        .unwrap();
    assert!(matches!(
        outcome(&network.recipient, 1).await,
        Some(RecipientOutcome::Held)
    ));
    let second = network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .release_held::<DeliverTask>(item_id(1), SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
        .unwrap()
        .id();
    enqueue_cancel(&network.recipient, 2, 1).await;
    let drained = engine.drive_due().await.unwrap();
    assert!(drained.completed.contains(&second));
    assert!(drained.failed.is_empty());
    // The latest attempt's operation has finished while attempt0 is still
    // held after its committed outcome, before its separate finish transaction.
    let retired = retire(&network.recipient, reference(1)).await;
    resume.notify_one();
    let earlier = tokio::time::timeout(Duration::from_secs(3), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(earlier.completed.len(), 1);
    assert!(earlier.failed.is_empty());
    assert!(
        matches!(
            retired,
            LocalRetirement::Refused(LocalRetirementRefusal::DeliveryInFlight)
        ),
        "older operation missed: {retired:?}"
    );
    assert_eq!(
        retire(&network.recipient, reference(1)).await,
        LocalRetirement::Retired
    );
}

#[tokio::test]
async fn held_delivery_finishes_its_operation_then_terminal_custody_can_retire() {
    let network = NetworkFixture::new().await;
    network
        .routes
        .local_answers
        .lock()
        .unwrap()
        .push_back(LocalOutcome::Held { until: None });
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local_and_admit::<DeliverTask, _>(
                        local_item(1),
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    drain(&network.engine(&network.recipient)).await;
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(
                    scope
                        .running_request_keys(DeliverTask::KIND)
                        .await?
                        .is_empty()
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(
        retire(&network.recipient, reference(1)).await,
        LocalRetirement::Refused(LocalRetirementRefusal::Held)
    );
    enqueue_cancel(&network.recipient, 2, 1).await;
    assert_eq!(
        retire(&network.recipient, reference(1)).await,
        LocalRetirement::Retired
    );
}

#[tokio::test]
async fn older_report_sequence_preserves_custody_without_mutation() {
    let network = NetworkFixture::new().await;
    network
        .routes
        .local_answers
        .lock()
        .unwrap()
        .push_back(LocalOutcome::Held { until: None });
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local_and_admit::<DeliverTask, _>(
                        local_item(1),
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    drain(&network.engine(&network.recipient)).await;
    enqueue_cancel(&network.recipient, 2, 1).await;
    // Local outcomes owe no remote report. Explicitly admit the valid earlier
    // sequence1 task while the actual Held -> Prevented projection is at2.
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                scope
                    .admit::<ReportTask>(
                        &delivery_request_key(&(item_id(1), ReportSeq::new(1)))
                            .map_err(storage_error)?,
                        &ReportRef {
                            item: item_id(1),
                            report_seq: ReportSeq::new(1),
                        },
                    )
                    .await
                    .map_err(durable_steps::StoreError::backend)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let before = snapshot(&network.recipient).await;
    assert_eq!(
        retire(&network.recipient, reference(1)).await,
        LocalRetirement::Refused(LocalRetirementRefusal::DeliveryInFlight)
    );
    assert_eq!(snapshot(&network.recipient).await, before);
}

#[tokio::test]
async fn malformed_running_delivery_key_is_corruption_before_any_deletion() {
    let fixture = Fixture::new().await;
    let input = require_local_attempt(enqueue_original(&fixture, 1, limits()).await).unwrap();
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .record_failure(
                        input,
                        DeliveryFailureKind::ExecutionFailed,
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                scope
                    .admit::<DeliverTask>(
                        &durable_steps::RequestKey::new("malformed-key").unwrap(),
                        &input,
                    )
                    .await
                    .map_err(durable_steps::StoreError::backend)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let before = snapshot(&fixture).await;
    let result = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Ok(Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .retire_local(&reference(1))
                    .await)
            })
        })
        .await
        .unwrap();
    assert!(matches!(
        result,
        Err(DeliveryError::Corrupt {
            field: "delivery operation request key"
        })
    ));
    assert_eq!(snapshot(&fixture).await, before);
}

#[tokio::test]
async fn empty_stored_running_key_is_corruption_without_deleting_custody() {
    let fixture = Fixture::new().await;
    let input = require_local_attempt(enqueue_original(&fixture, 1, limits()).await).unwrap();
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .record_failure(
                        input,
                        DeliveryFailureKind::ExecutionFailed,
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                let operation = scope
                    .admit::<DeliverTask>(
                        &delivery_request_key(&(input.item, input.attempt))
                            .map_err(storage_error)?,
                        &input,
                    )
                    .await
                    .map_err(durable_steps::StoreError::backend)?
                    .id();
                let changed = query::<Turso>(
                    "UPDATE _fleet_sync_operations SET request_key='' WHERE operation_id=?1",
                )
                .bind(operation.as_uuid().to_string())
                .execute(scope.executor())
                .await?;
                assert_eq!(changed.rows_affected(), 1);
                Ok(())
            })
        })
        .await
        .unwrap();
    let before = snapshot(&fixture).await;

    let result = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Ok(Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .retire_local(&reference(1))
                    .await)
            })
        })
        .await
        .unwrap();

    assert!(
        matches!(
            result,
            Err(DeliveryError::Corrupt {
                field: "delivery operation request key"
            })
        ),
        "empty stored journal key lost its corruption classification: {result:?}"
    );
    assert_eq!(snapshot(&fixture).await, before);
}

#[tokio::test]
async fn running_key_backend_failure_stays_storage_without_deleting_custody() {
    let fixture = Fixture::new().await;
    let input = require_local_attempt(enqueue_original(&fixture, 1, limits()).await).unwrap();
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .record_failure(
                        input,
                        DeliveryFailureKind::ExecutionFailed,
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                query::<Turso>("DROP TABLE _fleet_sync_operations")
                    .execute(scope.executor())
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let before = snapshot(&fixture).await;

    let result = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Ok(Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .retire_local(&reference(1))
                    .await)
            })
        })
        .await
        .unwrap();

    assert!(
        matches!(result, Err(DeliveryError::Storage(_))),
        "{result:?}"
    );
    assert_eq!(snapshot(&fixture).await, before);
}
