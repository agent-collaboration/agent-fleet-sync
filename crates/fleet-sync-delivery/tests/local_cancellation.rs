#![allow(clippy::unwrap_used)]
#[path = "support/local_cancellation.rs"]
mod local_cancellation;
#[path = "support/projection.rs"]
mod projection;
mod support;
use durable_steps::{AtomicStore, Engine};
use fleet_sync_delivery::*;
use local_cancellation::*;
use projection::{ProjectionConsumer, prepare_projection_table, read_committed_state};
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;
use support::{network::*, *};

#[tokio::test]
async fn local_cancel_before_original_survives_reopen_and_prevents_admission() {
    let fixture = Fixture::new().await;
    let admission = enqueue_cancel(&fixture, 2, 1).await;
    assert_eq!(admission.reference.item, item_id(2));
    assert!(admission.effects.attempts.is_empty() && admission.effects.reports.is_empty());
    assert!(outcome(&fixture, 2).await.is_none());
    let fixture = reopen(fixture).await;
    fixture
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
                    matches!(admitted, LocalItemAdmission::SettledOnAdmission),
                    "a cancelled original must not enter the journal"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_no_pending_operations(&fixture).await;
    assert!(
        matches!(outcome(&fixture,1).await,Some(RecipientOutcome::Prevented{cancel}) if cancel==item_id(2))
    );
    assert!(
        matches!(outcome(&fixture,2).await,Some(RecipientOutcome::Prevented{cancel}) if cancel==item_id(2))
    );
    assert!(matches!(
        enqueue_original(&fixture, 1, limits()).await,
        LocalItemAdmission::Replayed
    ));
    let state = snapshot(&fixture).await;
    assert_eq!(state.custody.len(), 2);
    assert_eq!(state.links.len(), 1);
    assert_no_remote_activity(&state);
}

#[tokio::test]
async fn local_cancel_after_original_finishes_a_stale_attempt_without_dispatch() {
    let network = NetworkFixture::new().await;
    let input =
        require_local_attempt(enqueue_original(&network.recipient, 1, limits()).await).unwrap();
    enqueue_cancel(&network.recipient, 2, 1).await;
    network
        .recipient
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let check = Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .check_delivery(input, SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?;
                assert!(matches!(check, DeliveryCheck::Finish { outcome: None }));
                Ok(())
            })
        })
        .await
        .unwrap();
    drain(&network.engine(&network.recipient)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 0);
    assert_no_remote_activity(&snapshot(&network.recipient).await);
}

#[tokio::test]
async fn local_cancel_replay_preserves_canonical_identity_and_projects_once() {
    let fixture = Fixture::new().await;
    prepare_projection_table(&fixture).await;
    require_local_attempt(enqueue_original(&fixture, 1, limits()).await).unwrap();
    let first = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local_cancel(
                        local_cancel(2, 1, "sender"),
                        &ProjectionConsumer::default(),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    let committed = read_committed_state(&fixture).await;
    assert_eq!(
        committed.projections.len(),
        2,
        "original and cancel outcomes are projected once"
    );
    let before = snapshot(&fixture).await;
    for replay_id in [2, 3] {
        let replay = fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .enqueue_local_cancel(
                            local_cancel(replay_id, 1, "sender"),
                            &ProjectionConsumer::default(),
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)
                })
            })
            .await
            .unwrap();
        assert_eq!(replay.reference.item, first.reference.item);
        assert_eq!(replay.reference.digest, first.reference.digest);
        assert!(replay.effects.attempts.is_empty() && replay.effects.reports.is_empty());
        assert_eq!(snapshot(&fixture).await, before);
        assert_eq!(read_committed_state(&fixture).await, committed);
    }
    let expected = first.reference.digest;
    fixture.store.transaction(move |scope| Box::pin(async move {
        let answer=Inbox::<_,TestTypes>::new(scope,peer(2),limits()).enqueue_local_cancel(local_cancel(2,1,"changed sender"),&TestConsumer,SyncTime::from_unix_millis(0)).await;
        assert!(matches!(answer,Err(DeliveryError::Refused{reason:DeliveryRefusal::ItemConflict{existing}}) if existing==expected));
        Ok(())
    })).await.unwrap();
    assert_eq!(snapshot(&fixture).await, before);
    assert!(
        outcome(&fixture, 3).await.is_none(),
        "replay creates no alias or second row"
    );
}

#[tokio::test]
async fn local_cancel_authority_refusal_is_committed_as_an_outcome() {
    let fixture = Fixture::new().await;
    require_local_attempt(enqueue_original(&fixture, 1, limits()).await).unwrap();
    let admission = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local_cancel(
                        local_cancel(2, 1, "another sender"),
                        &TestConsumer,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    assert_eq!(admission.reference.item, item_id(2));
    assert!(matches!(
        outcome(&fixture, 2).await,
        Some(RecipientOutcome::Refused {
            reason: DeliveryRefusal::CancelSenderMismatch
        })
    ));
    assert!(outcome(&fixture, 1).await.is_none());
    let fixture = reopen(fixture).await;
    assert!(matches!(
        outcome(&fixture, 2).await,
        Some(RecipientOutcome::Refused {
            reason: DeliveryRefusal::CancelSenderMismatch
        })
    ));
    assert_no_remote_activity(&snapshot(&fixture).await);
}

#[tokio::test]
async fn local_cancel_projection_error_rolls_back_custody_link_and_original() {
    let fixture = Fixture::new().await;
    prepare_projection_table(&fixture).await;
    require_local_attempt(enqueue_original(&fixture, 1, limits()).await).unwrap();
    let before = snapshot(&fixture).await;
    let consumer = Arc::new(ProjectionConsumer::default());
    consumer.refuse_outcome.store(true, Ordering::SeqCst);
    let error = fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local_cancel(
                        local_cancel(2, 1, "sender"),
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
    assert!(outcome(&fixture, 1).await.is_none());
    enqueue_cancel(&fixture, 2, 1).await;
    assert!(matches!(
        outcome(&fixture, 1).await,
        Some(RecipientOutcome::Prevented { .. })
    ));
}

#[tokio::test]
async fn local_cancel_waits_for_attempting_original_and_uses_the_real_answer() {
    #[derive(Clone, Copy)]
    enum AttemptAnswer {
        Held,
        Delivered,
        Uncertain,
    }
    for answer_kind in [
        AttemptAnswer::Held,
        AttemptAnswer::Delivered,
        AttemptAnswer::Uncertain,
    ] {
        let network = NetworkFixture::new().await;
        let answer = match answer_kind {
            AttemptAnswer::Held => LocalOutcome::Held { until: None },
            AttemptAnswer::Delivered => LocalOutcome::Delivered {
                acceptance: Acceptance {
                    receipt: "accepted".into(),
                },
            },
            AttemptAnswer::Uncertain => LocalOutcome::Uncertain,
        };
        network
            .routes
            .local_answers
            .lock()
            .unwrap()
            .push_back(answer);
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
            .register(support::interleaving::InterleavedDeliver::new(
                network.recipient.store.clone(),
                network.recipient.host.clone(),
                support::interleaving::AfterAnswer::Checkpoint {
                    reached: reached.clone(),
                    resume: resume.clone(),
                },
            ))
            .unwrap();
        let engine = Arc::new(engine);
        let drive = tokio::spawn({
            let engine = engine.clone();
            async move { engine.drive_due().await }
        });
        tokio::time::timeout(Duration::from_secs(10), reached.notified())
            .await
            .unwrap();
        let admission = enqueue_cancel(&network.recipient, 2, 1).await;
        assert!(admission.effects.attempts.is_empty() && admission.effects.reports.is_empty());
        assert!(
            outcome(&network.recipient, 2).await.is_none(),
            "attempting is still uncertain"
        );
        resume.notify_one();
        let report = tokio::time::timeout(Duration::from_secs(10), drive)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            report.failed.is_empty() && report.retried.is_empty(),
            "{report:?}"
        );
        match answer_kind {
            AttemptAnswer::Held => assert!(matches!(
                outcome(&network.recipient, 2).await,
                Some(RecipientOutcome::Prevented { .. })
            )),
            AttemptAnswer::Delivered => assert!(matches!(
                outcome(&network.recipient, 2).await,
                Some(RecipientOutcome::TooLate {
                    at: TooLateAt::Delivered,
                    ..
                })
            )),
            AttemptAnswer::Uncertain => assert!(matches!(
                outcome(&network.recipient, 2).await,
                Some(RecipientOutcome::TooLate {
                    at: TooLateAt::Uncertain,
                    ..
                })
            )),
        }
        assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
        assert_no_remote_activity(&snapshot(&network.recipient).await);
    }
}

#[tokio::test]
async fn local_cancel_prevents_a_previously_held_original() {
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
    assert!(matches!(
        outcome(&network.recipient, 1).await,
        Some(RecipientOutcome::Held)
    ));
    enqueue_cancel(&network.recipient, 2, 1).await;
    assert!(matches!(
        outcome(&network.recipient, 1).await,
        Some(RecipientOutcome::Prevented { .. })
    ));
    drain(&network.engine(&network.recipient)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn expired_local_cancel_records_expired_and_preserves_the_original() {
    let fixture = Fixture::new().await;
    require_local_attempt(enqueue_original(&fixture, 1, limits()).await).unwrap();
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let cancel = DeliveryCancel::<TestTypes>::new(
                    item_id(2),
                    "sender".into(),
                    Recipient {
                        peer: peer(2),
                        address: "inbox".into(),
                    },
                    Some(SyncTime::from_unix_millis(1)),
                    CancelBody { target: item_id(1) },
                )
                .unwrap();
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local_cancel(cancel, &TestConsumer, SyncTime::from_unix_millis(1))
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    assert!(matches!(
        outcome(&fixture, 2).await,
        Some(RecipientOutcome::Expired)
    ));
    assert!(outcome(&fixture, 1).await.is_none());
}

#[tokio::test]
async fn local_original_admission_rolls_back_when_pending_cancel_projection_fails() {
    let fixture = Fixture::new().await;
    prepare_projection_table(&fixture).await;
    enqueue_cancel(&fixture, 2, 1).await;
    let before = snapshot(&fixture).await;
    let consumer = Arc::new(ProjectionConsumer::default());
    consumer.refuse_outcome.store(true, Ordering::SeqCst);
    let error = fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .enqueue_local_and_admit::<DeliverTask, _>(
                        local_item(1),
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
    assert_no_pending_operations(&fixture).await;
    assert!(matches!(
        enqueue_original(&fixture, 1, limits()).await,
        LocalItemAdmission::SettledOnAdmission
    ));
    assert!(matches!(
        outcome(&fixture, 1).await,
        Some(RecipientOutcome::Prevented { .. })
    ));
}

#[tokio::test]
async fn cancel_before_original_produces_zero_real_local_dispatches() {
    let network = NetworkFixture::new().await;
    enqueue_cancel(&network.recipient, 2, 1).await;
    let offered = network
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
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    // Run the real task before the assertions: a wrong build that omits
    // admission-time resolution must visibly dispatch this original once.
    drain(&network.engine(&network.recipient)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 0);
    assert!(matches!(offered, LocalItemAdmission::SettledOnAdmission));
    assert!(matches!(
        outcome(&network.recipient, 1).await,
        Some(RecipientOutcome::Prevented { .. })
    ));
    assert_no_remote_activity(&snapshot(&network.recipient).await);
}

#[tokio::test]
async fn local_cancel_after_delivered_original_reopens_as_too_late() {
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
    drain(&network.engine(&network.recipient)).await;
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    let fixture = reopen(network.recipient).await;
    let admission = enqueue_cancel(&fixture, 2, 1).await;
    assert!(admission.effects.reports.is_empty());
    assert!(
        matches!(outcome(&fixture,2).await,Some(RecipientOutcome::TooLate{cancel,at:TooLateAt::Delivered}) if cancel==item_id(2))
    );
    assert_no_remote_activity(&snapshot(&fixture).await);
}
