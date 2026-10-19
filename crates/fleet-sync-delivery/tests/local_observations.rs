#![allow(clippy::unwrap_used)]
#[path = "support/local_observation.rs"]
mod local_observation;
mod support;
use durable_steps::{AtomicStore, Engine, JournalScope, storage::TransactionScope};
use fleet_sync_delivery::*;
use local_observation::*;
use sqlx_core::row::Row;
use sqlx_turso::Turso;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use support::{item_id, limits, peer};

#[test]
fn observation_bound_counts_canonical_json_bytes_including_quotes() {
    let valid = DeliverCall::<ObservationTypes>::validated_answer(LocalDelivery {
        outcome: LocalOutcome::Held { until: None },
        observation: "x".repeat(94),
    });
    assert!(
        matches!(valid, DispatchOutcome::Answered(LocalDeliveryAnswer::Observed { observation, .. }) if observation.len() == 94)
    );
    let oversized = DeliverCall::<ObservationTypes>::validated_answer(LocalDelivery {
        outcome: LocalOutcome::Held { until: None },
        observation: "x".repeat(95),
    });
    assert!(matches!(
        oversized,
        DispatchOutcome::Answered(LocalDeliveryAnswer::Uncertain)
    ));
}

#[tokio::test]
async fn held_and_refused_attempts_project_distinct_local_observations() {
    let fixture = ObservationFixture::new().await;
    fixture.host.consumer.answers.lock().unwrap().extend([
        LocalDelivery {
            outcome: LocalOutcome::Held { until: None },
            observation: "held: no desktop connection".into(),
        },
        LocalDelivery {
            outcome: LocalOutcome::Refused {
                reason: DeliveryRefusal::RecipientRevoked,
            },
            observation: "rejected: reconnect with an active identity".into(),
        },
    ]);
    let consumer = fixture.host.consumer.clone();
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, ObservationTypes>::new(scope, peer(2), limits());
                let input = inbox
                    .begin_receive(DeliveryStream::Items)
                    .await
                    .map_err(durable_steps::StoreError::backend)?
                    .unwrap();
                let entries = [1, 2]
                    .into_iter()
                    .map(|id| FetchEntry::Item {
                        seq: DeliverySeq::new(id),
                        item: DeliveryEnvelope::Item(
                            DeliveryItem::<ObservationTypes>::new(
                                item_id(id),
                                "sender".into(),
                                Recipient {
                                    peer: peer(2),
                                    address: "inbox".into(),
                                },
                                None,
                                "body".into(),
                            )
                            .unwrap(),
                        ),
                    })
                    .collect();
                let effects = inbox
                    .store_batch(
                        input,
                        FetchResult {
                            stream: DeliveryStream::Items,
                            entries,
                            through: DeliverySeq::new(2),
                        },
                        consumer.as_ref(),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(durable_steps::StoreError::backend)?;
                for attempt in effects.attempts {
                    scope
                        .admit::<ObservationTask>(
                            &delivery_request_key(&(attempt.item, attempt.attempt)).unwrap(),
                            &attempt,
                        )
                        .await
                        .map_err(durable_steps::StoreError::backend)?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    let report = fixture.engine().drive_due().await.unwrap();
    assert_eq!(report.completed.len(), 2);
    assert!(report.failed.is_empty());
    let projections = fixture.projections().await;
    assert_eq!(
        projections,
        vec![
            (
                item_id(1).as_uuid().to_string(),
                serde_json::json!({"kind":"held"}),
                Some("held: no desktop connection".into())
            ),
            (
                item_id(2).as_uuid().to_string(),
                serde_json::json!({"kind":"refused","value":{"reason":{"kind":"recipientRevoked"}}}),
                Some("rejected: reconnect with an active identity".into())
            ),
        ]
    );
    fixture.store.transaction(|scope| Box::pin(async move {
        let reports=sqlx_core::query::query::<Turso>("SELECT record FROM _fleet_sync_reports").fetch_all(scope.executor()).await?;
        assert_eq!(reports.len(),2);
        let rows=sqlx_core::query::query::<Turso>("SELECT record FROM _fleet_sync_inbox_items UNION ALL SELECT record FROM _fleet_sync_reports").fetch_all(scope.executor()).await?;
        for row in rows {let raw:String=row.try_get("record")?;assert!(!raw.contains("desktop connection"));assert!(!raw.contains("active identity"));assert!(!raw.contains("observation"));}
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn saved_answer_replays_the_same_observation_after_task_abort() {
    let fixture = ObservationFixture::new().await;
    fixture
        .host
        .consumer
        .answers
        .lock()
        .unwrap()
        .push_back(LocalDelivery {
            outcome: LocalOutcome::Held { until: None },
            observation: "held receipt from the original call".into(),
        });
    fixture.enqueue(1, None).await;
    let reached = Arc::new(tokio::sync::Notify::new());
    let mut engine = Engine::new(fixture.store.clone(), fixture.host.clone());
    engine
        .register(PausedObservationTask {
            inner: fixture.task(),
            reached: reached.clone(),
            release: Arc::new(tokio::sync::Notify::new()),
        })
        .unwrap();
    let worker = tokio::spawn(async move { engine.drive_due().await });
    tokio::time::timeout(Duration::from_secs(10), reached.notified())
        .await
        .unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert!(fixture.projections().await.is_empty());
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<Turso>(
                    "SELECT output FROM _fleet_sync_steps WHERE step_name='deliver'",
                )
                .fetch_one(scope.executor())
                .await?;
                let answer: durable_steps::CallResult<LocalDeliveryOutcome<ObservationTypes>> =
                    serde_json::from_slice(&row.try_get::<Vec<u8>, _>("output")?).unwrap();
                let durable_steps::CallResult::Answered(DispatchOutcome::Answered(
                    LocalDeliveryAnswer::Observed {
                        outcome: LocalOutcome::Held { until: None },
                        observation,
                    },
                )) = answer
                else {
                    return Err(durable_steps::StoreError::backend(
                        "saved held observation missing",
                    ));
                };
                assert_eq!(observation, "held receipt from the original call");
                Ok(())
            })
        })
        .await
        .unwrap();
    fixture.host.clock.store(100_000, Ordering::SeqCst);
    assert_eq!(
        fixture.engine().drive_due().await.unwrap().completed.len(),
        1
    );
    assert_eq!(
        fixture.projections().await,
        vec![(
            item_id(1).as_uuid().to_string(),
            serde_json::json!({"kind":"held"}),
            Some("held receipt from the original call".into())
        )]
    );
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn oversized_observation_is_uncertain_without_projection_data_or_resend() {
    let fixture = ObservationFixture::new().await;
    fixture
        .host
        .consumer
        .answers
        .lock()
        .unwrap()
        .push_back(LocalDelivery {
            outcome: LocalOutcome::Held { until: None },
            observation: "oversized observation".repeat(100),
        });
    fixture.enqueue(1, None).await;
    assert_eq!(
        fixture.engine().drive_due().await.unwrap().completed.len(),
        1
    );
    assert_eq!(
        fixture.projections().await,
        vec![(
            item_id(1).as_uuid().to_string(),
            serde_json::json!({"kind":"uncertain"}),
            None
        )]
    );
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 1);
    fixture.engine().drive_due().await.unwrap();
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn expiry_terminal_failure_and_cancellation_record_no_local_observation() {
    let fixture = ObservationFixture::new().await;
    fixture
        .enqueue(1, Some(SyncTime::from_unix_millis(0)))
        .await;
    fixture.enqueue(2, None).await;
    let consumer = fixture.host.consumer.clone();
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, ObservationTypes>::new(scope, peer(2), limits());
                inbox
                    .record_failure(
                        DeliveryAttempt {
                            item: item_id(2),
                            attempt: 0,
                            not_before: SyncTime::from_unix_millis(0),
                        },
                        DeliveryFailureKind::ExecutionFailed,
                        consumer.as_ref(),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(durable_steps::StoreError::backend)?;
                let original = DeliveryEnvelope::Item(
                    DeliveryItem::<ObservationTypes>::new(
                        item_id(3),
                        "sender".into(),
                        Recipient {
                            peer: peer(2),
                            address: "inbox".into(),
                        },
                        None,
                        "body".into(),
                    )
                    .unwrap(),
                );
                let input = inbox
                    .begin_receive(DeliveryStream::Items)
                    .await
                    .map_err(durable_steps::StoreError::backend)?
                    .unwrap();
                inbox
                    .store_batch(
                        input,
                        FetchResult {
                            stream: DeliveryStream::Items,
                            entries: vec![
                                FetchEntry::Item {
                                    seq: DeliverySeq::new(1),
                                    item: original,
                                },
                                FetchEntry::Item {
                                    seq: DeliverySeq::new(2),
                                    item: DeliveryEnvelope::Item(
                                        DeliveryItem::<ObservationTypes>::new(
                                            item_id(2),
                                            "sender".into(),
                                            Recipient {
                                                peer: peer(2),
                                                address: "inbox".into(),
                                            },
                                            None,
                                            "body".into(),
                                        )
                                        .unwrap(),
                                    ),
                                },
                            ],
                            through: DeliverySeq::new(2),
                        },
                        consumer.as_ref(),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(durable_steps::StoreError::backend)?;
                let cancellation = DeliveryEnvelope::Cancel(
                    DeliveryCancel::<ObservationTypes>::new(
                        item_id(4),
                        "sender".into(),
                        Recipient {
                            peer: peer(2),
                            address: "inbox".into(),
                        },
                        None,
                        CancelBody { target: item_id(3) },
                    )
                    .unwrap(),
                );
                let input = inbox
                    .begin_receive(DeliveryStream::Controls)
                    .await
                    .map_err(durable_steps::StoreError::backend)?
                    .unwrap();
                inbox
                    .store_batch(
                        input,
                        FetchResult {
                            stream: DeliveryStream::Controls,
                            entries: vec![FetchEntry::Item {
                                seq: DeliverySeq::new(1),
                                item: cancellation,
                            }],
                            through: DeliverySeq::new(1),
                        },
                        consumer.as_ref(),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(durable_steps::StoreError::backend)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    fixture.engine().drive_due().await.unwrap();
    let projections = fixture.projections().await;
    assert_eq!(projections.len(), 5);
    assert!(
        projections
            .iter()
            .all(|(_, _, observation)| observation.is_none())
    );
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn observation_hook_error_rolls_back_projection_and_outcome_together() {
    let fixture = ObservationFixture::new().await;
    fixture
        .host
        .consumer
        .answers
        .lock()
        .unwrap()
        .push_back(LocalDelivery {
            outcome: LocalOutcome::Refused {
                reason: DeliveryRefusal::RecipientRevoked,
            },
            observation: "refusal receipt survives projection failure".into(),
        });
    fixture
        .host
        .consumer
        .refuse_projection
        .store(true, Ordering::SeqCst);
    fixture.enqueue(1, None).await;
    let report = fixture.engine().drive_due().await.unwrap();
    assert_eq!(report.retried.len(), 1);
    assert!(report.completed.is_empty() && report.failed.is_empty());
    assert!(fixture.projections().await.is_empty());
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row =
                    sqlx_core::query::query::<Turso>("SELECT record FROM _fleet_sync_inbox_items")
                        .fetch_one(scope.executor())
                        .await?;
                let header: serde_json::Value =
                    serde_json::from_str(&row.try_get::<String, _>("record")?).unwrap();
                assert_eq!(header["progress"]["kind"], "attempting");
                Ok(())
            })
        })
        .await
        .unwrap();
    fixture
        .host
        .consumer
        .refuse_projection
        .store(false, Ordering::SeqCst);
    fixture.host.clock.store(100_000, Ordering::SeqCst);
    assert_eq!(
        fixture.engine().drive_due().await.unwrap().completed.len(),
        1
    );
    assert_eq!(
        fixture.projections().await,
        vec![(
            item_id(1).as_uuid().to_string(),
            serde_json::json!({"kind":"refused","value":{"reason":{"kind":"recipientRevoked"}}}),
            Some("refusal receipt survives projection failure".into())
        )]
    );
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 1);
}
