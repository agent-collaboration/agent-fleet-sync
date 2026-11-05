use super::support::{network::*, *};
use durable_steps::{AtomicStore, Engine, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

fn now() -> SyncTime {
    SyncTime::from_unix_millis(100_000)
}

fn push_engine(fixture: &Fixture) -> Engine<TestStore, TestHost> {
    let mut engine = Engine::new(fixture.store.clone(), fixture.host.clone());
    engine
        .register(PushTask::new(
            limits(),
            fixture.host.clone(),
            Arc::new(FixtureBindings),
        ))
        .unwrap();
    engine.register(PublishTask::new()).unwrap();
    engine
}

fn publication_engine(fixture: &Fixture) -> Engine<TestStore, TestHost> {
    let mut engine = Engine::new(fixture.store.clone(), fixture.host.clone());
    engine.register(PublishTask::new()).unwrap();
    engine
}

async fn reopen(fixture: Fixture) -> Fixture {
    let Fixture {
        store,
        host,
        directory,
    } = fixture;
    assert_eq!(Arc::strong_count(&store), 1);
    drop(store);
    let store = Arc::new(
        TestStore::open_and_migrate(directory.path().join("delivery.db"), host.clone())
            .await
            .unwrap(),
    );
    Fixture {
        store,
        host,
        directory,
    }
}

async fn reopen_network(network: NetworkFixture) -> NetworkFixture {
    let NetworkFixture {
        sender,
        relay,
        recipient,
        status,
        routes,
    } = network;
    let sender = reopen(sender).await;
    let relay = reopen(relay).await;
    let recipient = reopen(recipient).await;
    let status = reopen(status).await;
    *routes.sender.lock().unwrap() = Arc::downgrade(&sender.store);
    *routes.relay.lock().unwrap() = Arc::downgrade(&relay.store);
    *routes.recipient.lock().unwrap() = Arc::downgrade(&recipient.store);
    routes
        .publications
        .register("fixture-route", peer(1), &sender.store, &status.store);
    NetworkFixture {
        sender,
        relay,
        recipient,
        status,
        routes,
    }
}

async fn status(fixture: &Fixture) -> (u64, CancelResult) {
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let actual = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .status(item_id(2))
                    .await
                    .map_err(storage_error)?
                    .state;
                let observed = serde_json::to_value(&actual).unwrap();
                let OutboxStatusState::Known {
                    status: DeliveryStatus::Cancelled,
                    version,
                    cancel: Some(cancel),
                } = actual
                else {
                    return Err(StoreError::backend(format!(
                        "cancel has not settled: {observed}"
                    )));
                };
                Ok((version.value(), cancel))
            })
        })
        .await
        .unwrap()
}

async fn snapshot(fixture: &Fixture) -> Vec<String> {
    fixture.store.transaction(|scope| Box::pin(async move {
        query::<Turso>("SELECT json_array(item_id,status_kind,status_version,cancel_outcome,cancel_too_late_at,last_report_seq,payload,updated_at) AS state FROM _fleet_sync_outbox_items UNION ALL SELECT json_array(item_id,version,status_kind,cancel_outcome,cancel_too_late_at) AS state FROM _fleet_sync_status_owed UNION ALL SELECT json_array(cursor,item_id,version,status_kind,cancel_outcome,cancel_too_late_at) AS state FROM _fleet_sync_status_events ORDER BY state")
            .fetch_all(scope.executor()).await?.iter().map(|row| row.try_get("state")).collect::<Result<Vec<_>, _>>().map_err(StoreError::backend)
    })).await.unwrap()
}

async fn replay_answer(fixture: &Fixture) -> PushResult<Acceptance> {
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                accept_fixture_push(scope, limits(), cancel(2, 1), &TestConsumer, now())
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}

async fn receipt(
    fixture: &Fixture,
    answer: PushResult<Acceptance>,
    rollback: bool,
) -> Result<StatusPublicationResult, StoreError> {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let answer = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .receipt::<PublishTask>(
                        peer(2),
                        &DeliveryItemRef {
                            item: item_id(2),
                            digest: cancel(2, 1).digest().clone(),
                        },
                        answer,
                        now(),
                    )
                    .await
                    .map_err(storage_error)?;
                if rollback {
                    return Err(StoreError::backend("rollback receipt"));
                }
                Ok(answer)
            })
        })
        .await
}

async fn assert_invalid_receipt(fixture: &Fixture, answer: PushResult<Acceptance>) {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let before = query::<Turso>("SELECT total_changes() AS changes")
                    .fetch_one(scope.executor())
                    .await?
                    .try_get::<i64, _>("changes")?;
                let result = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .receipt::<PublishTask>(
                        peer(2),
                        &DeliveryItemRef {
                            item: item_id(2),
                            digest: cancel(2, 1).digest().clone(),
                        },
                        answer,
                        now(),
                    )
                    .await;
                assert!(
                    matches!(result, Err(DeliveryError::InvalidBatch)),
                    "invalid cancel receipt was not refused"
                );
                let after = query::<Turso>("SELECT total_changes() AS changes")
                    .fetch_one(scope.executor())
                    .await?
                    .try_get::<i64, _>("changes")?;
                assert_eq!(
                    after, before,
                    "receipt refusal wrote to the caller's transaction"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
}

async fn delayed_report(network: &NetworkFixture) -> RecipientReport<TestTypes> {
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut inbox =
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings);
                let reference = inbox
                    .pending_report(item_id(2))
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                inbox.resolve_report(reference).await.map_err(storage_error)
            })
        })
        .await
        .unwrap()
}

async fn apply_report(
    fixture: &Fixture,
    report: RecipientReport<TestTypes>,
    rollback: bool,
) -> Result<ReportResult, StoreError> {
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let answer = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .recipient_report::<PublishTask>(peer(2), report, now())
                    .await
                    .map_err(storage_error)?;
                if rollback {
                    return Err(StoreError::backend("rollback report"));
                }
                Ok(answer)
            })
        })
        .await
}

async fn completed_cancel_with_lost_receipt(too_late: bool) -> NetworkFixture {
    let network = NetworkFixture::new().await;
    network.enqueue(1, "original").await;
    drain(&network.engine(&network.sender)).await;
    drain(&push_engine(&network.relay)).await;
    if too_late {
        let mut engine = Engine::new(
            network.recipient.store.clone(),
            network.recipient.host.clone(),
        );
        engine
            .register(DeliverTask::new(
                config().local,
                Arc::new(TestConsumer),
                network.recipient.host.clone(),
                Arc::new(FixtureBindings),
            ))
            .unwrap();
        let delivered = engine.drive_due().await.unwrap();
        assert_eq!(delivered.completed.len(), 1);
        assert!(delivered.failed.is_empty() && delivered.retried.is_empty());
        assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    }
    network
        .sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Cancel(control) = cancel(2, 1) else {
                    return Err(StoreError::backend("cancel fixture"));
                };
                let (admission, _) =
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .enqueue_cancel_and_admit::<SubmitTask, RecordRemoval, _>(
                            control,
                            peer(3),
                            Submitter::Machine(peer(1)),
                            SyncTime::from_unix_millis(0),
                            &TestConsumer,
                        )
                        .await
                        .map_err(storage_error)?;
                assert!(matches!(admission, OutboxAdmission::Queued { .. }));
                Ok(())
            })
        })
        .await
        .unwrap();
    drain(&network.engine(&network.sender)).await;
    network.routes.lose_receipt.store(true, Ordering::SeqCst);
    let lost = push_engine(&network.relay).drive_due().await.unwrap();
    assert_eq!(
        lost.retried.len(),
        1,
        "first committed Push receipt must be lost"
    );
    assert!(lost.failed.is_empty());
    assert_eq!(
        network.routes.report_calls.load(Ordering::SeqCst),
        0,
        "reports must still be delayed"
    );
    let expected = if too_late {
        CancelResult::TooLate {
            at: TooLateAt::Delivered,
        }
    } else {
        CancelResult::Prevented
    };
    assert!(
        matches!(replay_answer(&network.recipient).await, PushResult::Replayed { status: DeliveryStatus::Cancelled, cancel: Some(actual) } if actual == expected)
    );
    network
}

async fn replay_before_report(too_late: bool) {
    // Arrange: real inbox completion, a lost receipt and no dispatched report.
    let network = completed_cancel_with_lost_receipt(too_late).await;
    let before = snapshot(&network.relay).await;
    assert!(
        receipt(
            &network.relay,
            replay_answer(&network.recipient).await,
            true
        )
        .await
        .is_err()
    );
    assert_eq!(
        snapshot(&network.relay).await,
        before,
        "receipt result, version and debt must roll back together"
    );
    let network = reopen_network(network).await;
    network.advance();

    // Act: retry Push through the durable engine before releasing the report.
    drain(&push_engine(&network.relay)).await;
    let expected = if too_late {
        CancelResult::TooLate {
            at: TooLateAt::Delivered,
        }
    } else {
        CancelResult::Prevented
    };
    assert_eq!(status(&network.relay).await, (2, expected));
    drain(&publication_engine(&network.relay)).await;
    assert_eq!(
        status(&network.sender).await,
        (2, expected),
        "sender must receive the replayed cancel result"
    );
    let published = snapshot(&network.relay).await;
    assert!(matches!(
        apply_report(&network.relay, delayed_report(&network).await, false)
            .await
            .unwrap(),
        ReportResult::Ack { .. }
    ));
    drain(&publication_engine(&network.relay)).await;
    assert_eq!(status(&network.relay).await, (2, expected));
    assert_eq!(status(&network.sender).await, (2, expected));

    // Assert: cold reopen and duplicate receipt/report retain one final revision.
    let network = reopen_network(network).await;
    let settled = snapshot(&network.relay).await;
    assert!(matches!(
        receipt(
            &network.relay,
            replay_answer(&network.recipient).await,
            false
        )
        .await
        .unwrap(),
        StatusPublicationResult::Applied
    ));
    assert!(matches!(
        apply_report(&network.relay, delayed_report(&network).await, false)
            .await
            .unwrap(),
        ReportResult::Replayed { .. }
    ));
    assert_eq!(snapshot(&network.relay).await, settled);
    assert_eq!(status(&network.sender).await, (2, expected));
    assert_eq!(status(&network.relay).await, (2, expected));
    assert_eq!(network.routes.push_bodies.load(Ordering::SeqCst), 3);
    assert_eq!(
        network.routes.local_calls.load(Ordering::SeqCst),
        u64::from(too_late)
    );
    // The delayed report records its sequence, but creates no extra event/debt.
    assert_eq!(published.len(), settled.len());
}

#[tokio::test]
async fn prevented_cancel_replay_publishes_final_result_before_delayed_report_across_reopen() {
    replay_before_report(false).await;
}

#[tokio::test]
async fn too_late_cancel_replay_publishes_final_result_before_delayed_report_across_reopen() {
    replay_before_report(true).await;
}

#[tokio::test]
async fn delayed_report_publishes_a_terminal_pending_cancel_improvement_atomically() {
    let network = completed_cancel_with_lost_receipt(false).await;
    // Persist the exact pre-fix F3 state: the old receipt kept Pending while
    // publishing terminal Cancelled@2. Real inbox custody supplies the report.
    network.relay.store.transaction(|scope| Box::pin(async move {
        query::<Turso>("UPDATE _fleet_sync_outbox_items SET status_kind='cancelled',status_version=2,live=0,payload=NULL,payload_bytes=0,settlement_charge_bytes=0,handed_off_at=0 WHERE item_id=?1")
            .bind(item_id(2).as_uuid().to_string()).execute(scope.executor()).await?;
        StatusStream::<_, TestTypes>::new(scope, Submitter::Machine(peer(1)), "fixture-route".into())
            .append_and_admit::<PublishTask>(&StatusPublication { item: item_id(2), status: DeliveryStatus::Cancelled, version: StatusVersion::new(2), cancel: Some(CancelResult::Pending) }, now())
            .await.map_err(storage_error)?;
        Ok(())
    })).await.unwrap();
    drain(&publication_engine(&network.relay)).await;
    assert_eq!(status(&network.sender).await, (2, CancelResult::Pending));
    let network = reopen_network(network).await;
    let before = snapshot(&network.relay).await;
    assert!(
        apply_report(&network.relay, delayed_report(&network).await, true)
            .await
            .is_err()
    );
    assert_eq!(
        snapshot(&network.relay).await,
        before,
        "report result, version and debt must roll back together"
    );
    assert!(matches!(
        apply_report(&network.relay, delayed_report(&network).await, false)
            .await
            .unwrap(),
        ReportResult::Ack { .. }
    ));
    assert_eq!(status(&network.relay).await, (3, CancelResult::Prevented));
    let network = reopen_network(network).await;
    drain(&publication_engine(&network.relay)).await;
    assert_eq!(status(&network.sender).await, (3, CancelResult::Prevented));
    let before = snapshot(&network.relay).await;
    assert!(matches!(
        apply_report(&network.relay, delayed_report(&network).await, false)
            .await
            .unwrap(),
        ReportResult::Replayed { .. }
    ));
    assert_eq!(snapshot(&network.relay).await, before);
}

#[tokio::test]
async fn final_cancel_results_refuse_regressive_receipts_and_contradictory_reports() {
    for too_late in [false, true] {
        let network = completed_cancel_with_lost_receipt(too_late).await;
        apply_report(&network.relay, delayed_report(&network).await, false)
            .await
            .unwrap();
        let before = snapshot(&network.relay).await;
        let regressive = PushResult::Replayed {
            status: DeliveryStatus::AtRecipient,
            cancel: Some(CancelResult::Pending),
        };
        assert_invalid_receipt(&network.relay, regressive).await;
        let opposite = if too_late {
            CancelResult::Prevented
        } else {
            CancelResult::TooLate {
                at: TooLateAt::Delivered,
            }
        };
        assert_invalid_receipt(
            &network.relay,
            PushResult::Replayed {
                status: DeliveryStatus::Cancelled,
                cancel: Some(opposite),
            },
        )
        .await;
        let contradictory = RecipientReport {
            item: item_id(2),
            report_seq: ReportSeq::new(2),
            outcome: if too_late {
                RecipientOutcome::Prevented { cancel: item_id(2) }
            } else {
                RecipientOutcome::TooLate {
                    cancel: item_id(2),
                    at: TooLateAt::Delivered,
                }
            },
        };
        assert!(matches!(
            apply_report(&network.relay, contradictory, false)
                .await
                .unwrap(),
            ReportResult::Refused {
                reason: DeliveryRefusal::ProcessingFailed {
                    kind: DeliveryFailureKind::ExecutionFailed
                }
            }
        ));
        assert_eq!(
            snapshot(&network.relay).await,
            before,
            "refused cancel updates must write nothing"
        );
    }
}
