#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, Engine, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use std::sync::atomic::Ordering;
use support::{network::*, *};

#[tokio::test]
async fn non_deduplicating_local_answer_loss_is_uncertain_and_never_repeated() {
    let network = NetworkFixture::new().await;
    network.enqueue(1, "one possible effect").await;
    drain(&network.engine(&network.sender)).await;
    network.routes.lose_delivery.store(true, Ordering::SeqCst);
    network.receive(DeliveryStream::Items).await;
    let recipient = network.engine(&network.recipient);
    assert_eq!(recipient.drive_due().await.unwrap().completed.len(), 1);
    let report = recipient.drive_due().await.unwrap();
    assert_eq!(report.failed.len(), 1);
    assert!(matches!(
        report.failed[0].1,
        durable_steps::OperationFailure::Uncertain { .. }
    ));
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    network.advance();
    drain(&network.engine(&network.recipient)).await;
    drain(&network.engine(&network.mailbox)).await;
    drain(&network.engine(&network.status)).await;
    assert!(matches!(
        sender_status(&network, 1).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Uncertain,
            ..
        }
    ));
    network.receive(DeliveryStream::Items).await;
    drain(&network.engine(&network.recipient)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn oversized_local_receipt_is_uncertain_at_call_checkpoint_and_after_restart() {
    let network = NetworkFixture::new().await;
    network
        .enqueue(1, "one external effect with invalid receipt")
        .await;
    drain(&network.engine(&network.sender)).await;
    network
        .routes
        .local_answers
        .lock()
        .unwrap()
        .push_back(LocalOutcome::Delivered {
            acceptance: Acceptance {
                receipt: "oversized".repeat(1024),
            },
        });
    network.receive(DeliveryStream::Items).await;
    let reached = std::sync::Arc::new(tokio::sync::Notify::new());
    let engine = std::sync::Arc::new(interleaved_engine(
        &network,
        support::interleaving::AfterAnswer::Checkpoint {
            reached: reached.clone(),
            resume: std::sync::Arc::new(tokio::sync::Notify::new()),
        },
    ));
    // Receive commits first; the next operation pauses after durable CallDone.
    assert_eq!(engine.drive_due().await.unwrap().completed.len(), 1);
    let worker = tokio::spawn(async move { engine.drive_due().await });
    tokio::time::timeout(std::time::Duration::from_secs(10), reached.notified())
        .await
        .unwrap();
    let checkpoint = network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT output FROM _fleet_sync_steps WHERE step_name='deliver'",
                )
                .fetch_one(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                row.try_get::<Vec<u8>, _>("output")
                    .map_err(durable_steps::StoreError::backend)
            })
        })
        .await
        .unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert!(
        checkpoint.len() < 512,
        "unbounded checkpoint: {} bytes",
        checkpoint.len()
    );
    assert!(String::from_utf8_lossy(&checkpoint).contains("uncertain"));
    assert!(!String::from_utf8_lossy(&checkpoint).contains("oversized"));

    // A new Engine recovers the saved answer after the interrupted drive lease.
    network.advance();
    drain(&network.engine(&network.recipient)).await;
    drain(&network.engine(&network.mailbox)).await;
    drain(&network.engine(&network.status)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        sender_status(&network, 1).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Uncertain,
            ..
        }
    ));
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
                    Some(RecipientOutcome::Uncertain)
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    network.receive(DeliveryStream::Items).await;
    drain(&network.engine(&network.recipient)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
}

fn interleaved_engine(
    network: &NetworkFixture,
    action: support::interleaving::AfterAnswer,
) -> Engine<TestStore, TestHost> {
    let mut engine = Engine::new(
        network.recipient.store.clone(),
        network.recipient.host.clone(),
    );
    engine
        .register(support::interleaving::InterleavedDeliver::new(
            network.recipient.store.clone(),
            network.recipient.host.clone(),
            action,
        ))
        .unwrap();
    engine.register(ReportTask::new(config())).unwrap();
    engine
        .register(ReceiveTask::new(
            config(),
            std::sync::Arc::new(TestConsumer),
        ))
        .unwrap();
    engine
}
#[tokio::test]
async fn a_release_after_the_saved_held_answer_survives_until_outcome_once() {
    let network = NetworkFixture::new().await;
    network.enqueue(1, "held then released").await;
    drain(&network.engine(&network.sender)).await;
    network
        .routes
        .local_answers
        .lock()
        .unwrap()
        .push_back(LocalOutcome::Held { until: None });
    network.receive(DeliveryStream::Items).await;
    drain(&interleaved_engine(
        &network,
        support::interleaving::AfterAnswer::Release,
    ))
    .await;
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
async fn cancel_waiting_for_a_saved_delivered_answer_becomes_too_late() {
    let network = NetworkFixture::new().await;
    network.enqueue(1, "delivered before outcome").await;
    drain(&network.engine(&network.sender)).await;
    network.receive(DeliveryStream::Items).await;
    let engine = interleaved_engine(&network, support::interleaving::AfterAnswer::Cancel);
    assert_eq!(engine.drive_due().await.unwrap().completed.len(), 1);
    assert!(engine.drive_due().await.unwrap().failed.is_empty());
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT record FROM _fleet_sync_reports WHERE item_id=?1",
                )
                .bind(item_id(2).as_uuid().to_string())
                .fetch_one(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                let report: RecipientReport<TestTypes> =
                    serde_json::from_str(&row.try_get::<String, _>("record").unwrap()).unwrap();
                assert!(matches!(
                    report.outcome,
                    RecipientOutcome::TooLate {
                        at: TooLateAt::Delivered,
                        ..
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn cancel_waiting_for_a_saved_held_answer_prevents_the_original_in_the_outcome_transaction() {
    let network = NetworkFixture::new().await;
    network.enqueue(1, "held before cancel").await;
    drain(&network.engine(&network.sender)).await;
    // Controls arrives at the recipient later, while the relay already owns the cancel.
    network
        .mailbox
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .submit(peer(1), cancel(2, 1), SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    network
        .routes
        .local_answers
        .lock()
        .unwrap()
        .push_back(LocalOutcome::Held { until: None });
    network.receive(DeliveryStream::Items).await;
    let engine = interleaved_engine(&network, support::interleaving::AfterAnswer::Cancel);
    assert_eq!(engine.drive_due().await.unwrap().completed.len(), 1);
    assert!(engine.drive_due().await.unwrap().failed.is_empty());
    network.recipient.store.transaction(|scope|Box::pin(async move {
        let mut inbox=Inbox::<_,TestTypes>::new(scope,peer(2),limits());
        assert!(matches!(inbox.local_outcome(item_id(1)).await.map_err(storage_error)?,Some(RecipientOutcome::Prevented {cancel}) if cancel==item_id(2)));
        for item in [item_id(1),item_id(2)] {
            let reference=inbox.pending_report(item).await.map_err(storage_error)?.unwrap();
            assert!(matches!(inbox.resolve_report(reference).await.map_err(storage_error)?.outcome,RecipientOutcome::Prevented {cancel} if cancel==item_id(2)));
        }
        Ok(())
    })).await.unwrap();
    drain(&engine).await;
    network.advance();
    drain(&network.engine(&network.recipient)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn cancel_waiting_for_an_uncertain_attempt_becomes_too_late() {
    let network = NetworkFixture::new().await;
    network.enqueue(1, "possible effect").await;
    drain(&network.engine(&network.sender)).await;
    network.receive(DeliveryStream::Items).await;
    network.routes.lose_delivery.store(true, Ordering::SeqCst);
    let engine = interleaved_engine(&network, support::interleaving::AfterAnswer::Cancel);
    assert_eq!(engine.drive_due().await.unwrap().completed.len(), 1);
    assert_eq!(engine.drive_due().await.unwrap().failed.len(), 1);
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT record FROM _fleet_sync_reports WHERE item_id=?1",
                )
                .bind(item_id(2).as_uuid().to_string())
                .fetch_one(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                let report: RecipientReport<TestTypes> =
                    serde_json::from_str(&row.try_get::<String, _>("record").unwrap()).unwrap();
                assert!(matches!(
                    report.outcome,
                    RecipientOutcome::TooLate {
                        at: TooLateAt::Uncertain,
                        ..
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}
