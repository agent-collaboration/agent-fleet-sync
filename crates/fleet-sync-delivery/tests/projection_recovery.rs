#![allow(clippy::unwrap_used)]
#[path = "support/projection.rs"]
mod projection;
mod support;

use durable_steps::{AtomicStore, Engine, storage::TransactionScope};
use fleet_sync_delivery::*;
use projection::*;
use sqlx_core::row::Row;
use sqlx_turso::Turso;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use support::{
    network::{NetworkFixture, drain},
    *,
};

#[tokio::test]
async fn abort_after_saved_answer_recovers_custody_and_projection_together() {
    let network = NetworkFixture::new().await;
    prepare_projection_table(&network.recipient).await;
    enqueue_projection_attempt(&network.recipient, 1, None).await;
    let consumer = Arc::new(ProjectionConsumer::default());
    let reached = Arc::new(tokio::sync::Notify::new());
    let mut interrupted = Engine::new(
        network.recipient.store.clone(),
        network.recipient.host.clone(),
    );
    interrupted
        .register(interleaving::InterleavedDeliver::with_consumer(
            network.recipient.store.clone(),
            network.recipient.host.clone(),
            interleaving::AfterAnswer::Checkpoint {
                reached: reached.clone(),
                resume: Arc::new(tokio::sync::Notify::new()),
            },
            consumer.clone(),
        ))
        .unwrap();
    let worker = tokio::spawn(async move { interrupted.drive_due().await });
    tokio::time::timeout(Duration::from_secs(10), reached.notified())
        .await
        .unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());

    let before = read_committed_state(&network.recipient).await;
    assert!(before.projections.is_empty());
    let header: serde_json::Value =
        serde_json::from_str(&before.custody.first().unwrap().0).unwrap();
    assert_eq!(header["progress"]["kind"], "attempting");
    assert!(before.custody.first().unwrap().1.is_some());
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<Turso>(
                    "SELECT output FROM _fleet_sync_steps WHERE step_name='deliver'",
                )
                .fetch_one(scope.executor())
                .await?;
                let saved: Vec<u8> = row.try_get("output")?;
                let answer: durable_steps::CallResult<LocalDeliveryOutcome<TestTypes>> =
                    serde_json::from_slice(&saved).unwrap();
                let durable_steps::CallResult::Answered(DispatchOutcome::Answered(
                    LocalDeliveryAnswer::Observed {
                        outcome: LocalOutcome::Delivered { acceptance },
                        observation: (),
                    },
                )) = answer
                else {
                    return Err(durable_steps::StoreError::backend(
                        "saved answer must be delivered",
                    ));
                };
                assert_eq!(acceptance.receipt, "rich receipt: accepted once");
                Ok(())
            })
        })
        .await
        .unwrap();

    network.advance();
    drain(&projection_engine(&network.recipient, consumer)).await;
    let after = read_committed_state(&network.recipient).await;
    assert_eq!(
        outcome_values(&after),
        vec![
            serde_json::json!({"kind":"delivered","value":{"acceptance":{"receipt":"rich receipt: accepted once"}}})
        ]
    );
    let header: serde_json::Value =
        serde_json::from_str(&after.custody.first().unwrap().0).unwrap();
    assert_eq!(header["progress"]["kind"], "final");
    assert_eq!(
        header["progress"]["value"]["outcome"],
        outcome_values(&after).first().unwrap().clone()
    );
    assert!(after.custody.first().unwrap().1.is_none());
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn task_failure_projects_malformed_digest_without_header_validation() {
    let fixture = Fixture::new().await;
    prepare_projection_table(&fixture).await;
    enqueue_projection_attempt(&fixture, 1, None).await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row =
                    sqlx_core::query::query::<Turso>("SELECT record FROM _fleet_sync_inbox_items")
                        .fetch_one(scope.executor())
                        .await?;
                let mut header: serde_json::Value =
                    serde_json::from_str(&row.try_get::<String, _>("record")?).unwrap();
                header["digest"] = serde_json::json!("malformed digest");
                sqlx_core::query::query::<Turso>("UPDATE _fleet_sync_inbox_items SET record=?1")
                    .bind(header.to_string())
                    .execute(scope.executor())
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let report = projection_engine(&fixture, Arc::new(ProjectionConsumer::default()))
        .drive_due()
        .await
        .unwrap();
    assert_eq!(report.failed.len(), 1);
    let after = read_committed_state(&fixture).await;
    assert_eq!(
        outcome_values(&after),
        vec![
            serde_json::json!({"kind":"refused","value":{"reason":{"kind":"processingFailed","value":{"kind":"custodyCorrupt"}}}})
        ]
    );
    let header: serde_json::Value =
        serde_json::from_str(&after.custody.first().unwrap().0).unwrap();
    assert_eq!(header["digest"], "malformed digest");
    assert_eq!(
        header["progress"]["value"]["outcome"],
        outcome_values(&after).first().unwrap().clone()
    );
}
