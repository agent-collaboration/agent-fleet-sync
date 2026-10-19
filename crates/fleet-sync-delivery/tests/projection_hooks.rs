#![allow(clippy::unwrap_used)]
#[path = "support/projection.rs"]
mod projection;
mod support;

use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use projection::*;
use sqlx_core::row::Row;
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};
use support::{
    network::{NetworkFixture, drain},
    *,
};

#[tokio::test]
async fn received_projection_commits_once_and_replay_writes_nothing() {
    let fixture = Fixture::new().await;
    prepare_projection_table(&fixture).await;
    let consumer = Arc::new(ProjectionConsumer::default());
    let input =
        receive_projection_item(&fixture, consumer.clone(), item(1, "incoming body"), false)
            .await
            .unwrap();
    let before = read_committed_state(&fixture).await;
    assert_eq!(before.custody.len(), 1);
    assert_eq!(
        before.projections,
        vec![(
            item_id(1).as_uuid().to_string(),
            "received".into(),
            "incoming body".into()
        )]
    );
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let effects = Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .store_batch(
                        input,
                        projection_batch(item(1, "incoming body")),
                        consumer.as_ref(),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                assert!(effects.attempts.is_empty() && effects.reports.is_empty());
                let duplicate = FetchResult {
                    stream: DeliveryStream::Items,
                    entries: vec![FetchEntry::Item {
                        seq: DeliverySeq::new(2),
                        item: item(1, "incoming body"),
                    }],
                    through: DeliverySeq::new(2),
                };
                let effects = Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .store_batch(
                        input,
                        duplicate,
                        consumer.as_ref(),
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
    assert_eq!(read_committed_state(&fixture).await, before);
}

#[tokio::test]
async fn received_hook_error_and_later_abort_roll_back_custody_and_projection() {
    for refuse_hook in [true, false] {
        let fixture = Fixture::new().await;
        prepare_projection_table(&fixture).await;
        let consumer = Arc::new(ProjectionConsumer::default());
        consumer
            .refuse_received
            .store(refuse_hook, Ordering::SeqCst);
        let before = read_committed_state(&fixture).await;
        assert!(
            receive_projection_item(
                &fixture,
                consumer.clone(),
                item(1, "incoming body"),
                !refuse_hook
            )
            .await
            .is_err()
        );
        assert_eq!(read_committed_state(&fixture).await, before);
        consumer.refuse_received.store(false, Ordering::SeqCst);
        receive_projection_item(&fixture, consumer, item(1, "incoming body"), false)
            .await
            .unwrap();
        let after = read_committed_state(&fixture).await;
        assert_eq!((after.custody.len(), after.projections.len()), (1, 1));
    }
}

#[tokio::test]
async fn local_attempt_projects_every_outcome_and_exact_acceptance() {
    for (answer, expected) in [
        (
            LocalOutcome::Delivered {
                acceptance: Acceptance {
                    receipt: "rich receipt: accepted once".into(),
                },
            },
            serde_json::json!({"kind":"delivered","value":{"acceptance":{"receipt":"rich receipt: accepted once"}}}),
        ),
        (
            LocalOutcome::Uncertain,
            serde_json::json!({"kind":"uncertain"}),
        ),
        (LocalOutcome::Dropped, serde_json::json!({"kind":"dropped"})),
        (
            LocalOutcome::Refused {
                reason: DeliveryRefusal::RecipientRevoked,
            },
            serde_json::json!({"kind":"refused","value":{"reason":{"kind":"recipientRevoked"}}}),
        ),
    ] {
        let network = NetworkFixture::new().await;
        prepare_projection_table(&network.recipient).await;
        network
            .routes
            .local_answers
            .lock()
            .unwrap()
            .push_back(answer);
        enqueue_projection_attempt(&network.recipient, 1, None).await;
        let consumer = Arc::new(ProjectionConsumer::default());
        drain(&projection_engine(&network.recipient, consumer)).await;
        let state = read_committed_state(&network.recipient).await;
        assert_eq!(outcome_values(&state), vec![expected.clone()]);
        let header: serde_json::Value =
            serde_json::from_str(&state.custody.first().unwrap().0).unwrap();
        assert_eq!(header["progress"]["value"]["outcome"], expected);
        assert!(
            state.custody.first().unwrap().1.is_none(),
            "local custody compacts with its projection"
        );
    }
}

#[tokio::test]
async fn held_then_released_projects_both_attempts_and_stale_failure_is_inert() {
    let network = NetworkFixture::new().await;
    prepare_projection_table(&network.recipient).await;
    network
        .routes
        .local_answers
        .lock()
        .unwrap()
        .push_back(LocalOutcome::Held { until: None });
    let first = enqueue_projection_attempt(&network.recipient, 1, None).await;
    let consumer = Arc::new(ProjectionConsumer::default());
    drain(&projection_engine(&network.recipient, consumer.clone())).await;
    assert_eq!(
        outcome_values(&read_committed_state(&network.recipient).await),
        vec![serde_json::json!({"kind":"held"})]
    );
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .release_held::<ProjectionTask>(item_id(1), SyncTime::from_unix_millis(0))
                        .await
                        .map_err(storage_error)?
                        .is_some()
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    drain(&projection_engine(&network.recipient, consumer.clone())).await;
    let before = read_committed_state(&network.recipient).await;
    assert_eq!(
        outcome_values(&before),
        vec![
            serde_json::json!({"kind":"held"}),
            serde_json::json!({"kind":"delivered","value":{"acceptance":{"receipt":"rich receipt: accepted once"}}})
        ]
    );
    network
        .recipient
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                for attempt in [
                    first,
                    DeliveryAttempt {
                        attempt: 1,
                        ..first
                    },
                ] {
                    let effects = inbox
                        .record_failure(
                            attempt,
                            DeliveryFailureKind::ExecutionFailed,
                            consumer.as_ref(),
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)?;
                    assert!(effects.reports.is_empty());
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(read_committed_state(&network.recipient).await, before);
}

#[tokio::test]
async fn expired_attempt_projects_without_dispatching() {
    let network = NetworkFixture::new().await;
    prepare_projection_table(&network.recipient).await;
    enqueue_projection_attempt(&network.recipient, 1, Some(SyncTime::from_unix_millis(0))).await;
    drain(&projection_engine(
        &network.recipient,
        Arc::new(ProjectionConsumer::default()),
    ))
    .await;
    assert_eq!(
        outcome_values(&read_committed_state(&network.recipient).await),
        vec![serde_json::json!({"kind":"expired"})]
    );
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn malformed_digest_failure_projects_and_hook_error_rolls_back_both() {
    let fixture = Fixture::new().await;
    prepare_projection_table(&fixture).await;
    let attempt = enqueue_projection_attempt(&fixture, 1, None).await;
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
    let before = read_committed_state(&fixture).await;
    let consumer = Arc::new(ProjectionConsumer::default());
    consumer.refuse_outcome.store(true, Ordering::SeqCst);
    for abort_after in [false, true] {
        let consumer = consumer.clone();
        consumer
            .refuse_outcome
            .store(!abort_after, Ordering::SeqCst);
        let result = fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .record_failure(
                            attempt,
                            DeliveryFailureKind::CustodyCorrupt,
                            consumer.as_ref(),
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)?;
                    if abort_after {
                        return Err(StoreError::backend(
                            "abort after failure custody and projection",
                        ));
                    }
                    Ok(())
                })
            })
            .await;
        assert!(result.is_err());
        assert_eq!(read_committed_state(&fixture).await, before);
    }
    consumer.refuse_outcome.store(false, Ordering::SeqCst);
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                inbox
                    .record_failure(
                        attempt,
                        DeliveryFailureKind::CustodyCorrupt,
                        consumer.as_ref(),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
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

#[tokio::test]
async fn receive_time_refusal_projects_once_when_local_custody_becomes_relayed() {
    let fixture = Fixture::new().await;
    prepare_projection_table(&fixture).await;
    let attempt = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(original) = item(1, "original") else {
                    return Err(StoreError::backend("fixture item"));
                };
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                let attempt = inbox
                    .enqueue_local(original, SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                inbox
                    .record_failure(
                        attempt,
                        DeliveryFailureKind::ExecutionFailed,
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(attempt)
            })
        })
        .await
        .unwrap();
    let consumer = Arc::new(ProjectionConsumer::default());
    let input = receive_projection_item(&fixture, consumer.clone(), item(1, "original"), false)
        .await
        .unwrap();
    let before = read_committed_state(&fixture).await;
    assert_eq!(before.reports.len(), 1);
    assert_eq!(
        outcome_values(&before),
        vec![
            serde_json::json!({"kind":"refused","value":{"reason":{"kind":"processingFailed","value":{"kind":"executionFailed"}}}})
        ]
    );
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                inbox
                    .store_batch(
                        input,
                        projection_batch(item(1, "original")),
                        consumer.as_ref(),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                inbox
                    .record_failure(
                        attempt,
                        DeliveryFailureKind::ExecutionFailed,
                        consumer.as_ref(),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(read_committed_state(&fixture).await, before);
}

#[tokio::test]
async fn cancellation_projects_original_and_control_in_one_transaction() {
    let fixture = Fixture::new().await;
    prepare_projection_table(&fixture).await;
    let consumer = Arc::new(ProjectionConsumer::default());
    receive_projection_item(&fixture, consumer.clone(), item(1, "original"), false)
        .await
        .unwrap();
    let before = read_committed_state(&fixture).await;
    consumer.refuse_outcome.store(true, Ordering::SeqCst);
    assert!(
        receive_projection_item(&fixture, consumer.clone(), cancel(2, 1), false)
            .await
            .is_err()
    );
    assert_eq!(read_committed_state(&fixture).await, before);
    consumer.refuse_outcome.store(false, Ordering::SeqCst);
    let input = receive_projection_item(&fixture, consumer.clone(), cancel(2, 1), false)
        .await
        .unwrap();
    let after = read_committed_state(&fixture).await;
    assert_eq!(after.reports.len(), 2);
    let expected =
        serde_json::json!({"kind":"prevented","value":{"cancel":item_id(2).as_uuid().to_string()}});
    assert_eq!(outcome_values(&after), vec![expected.clone(), expected]);
    assert_eq!(
        after
            .projections
            .iter()
            .map(|(item, kind, _)| (item.clone(), kind.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (item_id(1).as_uuid().to_string(), "received"),
            (item_id(1).as_uuid().to_string(), "outcome"),
            (item_id(2).as_uuid().to_string(), "outcome")
        ]
    );
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .store_batch(
                        input,
                        projection_batch(cancel(2, 1)),
                        consumer.as_ref(),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(read_committed_state(&fixture).await, after);
}

#[tokio::test]
async fn control_outcomes_preserve_too_late_refusal_and_expiry() {
    for (sender, expiry, expected) in [
        (
            "sender",
            None,
            serde_json::json!({"kind":"tooLate","value":{"cancel":item_id(2).as_uuid().to_string(),"at":"delivered"}}),
        ),
        (
            "other sender",
            None,
            serde_json::json!({"kind":"refused","value":{"reason":{"kind":"cancelSenderMismatch"}}}),
        ),
        (
            "sender",
            Some(SyncTime::from_unix_millis(0)),
            serde_json::json!({"kind":"expired"}),
        ),
    ] {
        let network = NetworkFixture::new().await;
        prepare_projection_table(&network.recipient).await;
        enqueue_projection_attempt(&network.recipient, 1, None).await;
        let consumer = Arc::new(ProjectionConsumer::default());
        drain(&projection_engine(&network.recipient, consumer.clone())).await;
        let before = read_committed_state(&network.recipient).await;
        let cancellation = DeliveryEnvelope::Cancel(
            DeliveryCancel::<TestTypes>::new(
                item_id(2),
                sender.into(),
                Recipient {
                    peer: peer(2),
                    address: "inbox".into(),
                },
                expiry,
                CancelBody { target: item_id(1) },
            )
            .unwrap(),
        );
        receive_projection_item(&network.recipient, consumer, cancellation, false)
            .await
            .unwrap();
        let after = read_committed_state(&network.recipient).await;
        let mut expected_outcomes = outcome_values(&before);
        expected_outcomes.push(expected.clone());
        assert_eq!(outcome_values(&after), expected_outcomes);
        assert_eq!(after.reports.len(), 1);
        let report: serde_json::Value =
            serde_json::from_str(after.reports.first().unwrap()).unwrap();
        assert_eq!(report["outcome"], expected);
    }
}
