use super::support::{
    network::{DeliverTask, PushTask, ReportTask},
    *,
};
use durable_steps::{AtomicStore, Engine, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

use super::support::retention::*;
fn too_old_error(result: Result<LocalItemAdmission, DeliveryError>) -> bool {
    matches!(
        result,
        Err(DeliveryError::Refused {
            reason: DeliveryRefusal::TooOld
        })
    )
}

#[tokio::test]
async fn sixty_one_day_ids_are_refused_at_machine_outbox_relay_push_and_local_inbox() {
    let fixture = Fixture::new().await;
    fixture.store.transaction(|scope|Box::pin(async move {
        let DeliveryEnvelope::Item(original)=item(1,"old") else {return Err(StoreError::backend("original"));};
        assert!(matches!(Outbox::<_,TestTypes>::new(scope,limits()).enqueue(original,peer(9),fleet_sync_delivery::Submitter::Machine(peer(1)),day(61)).await.map_err(storage_error)?,OutboxAdmission::Refused {reason:DeliveryRefusal::TooOld}));
        assert!(matches!(Outbox::<_,TestTypes>::new(scope,limits()).submit_relay::<PushTask,RecordRemoval>(fleet_sync_delivery::Submitter::Machine(peer(1)),item(1,"old"),peer(9),"route".into(),day(61)).await.map_err(storage_error)?,SubmitResult::Refused {reason:DeliveryRefusal::TooOld}));
        assert!(matches!(Inbox::<_,TestTypes>::new(scope,peer(2),limits()).accept_push::<DeliverTask,ReportTask,RecordRemoval,_>(fleet_sync_delivery::Submitter::Machine(peer(1)), item(1,"old"),peer(9),&TestConsumer,day(61)).await.map_err(storage_error)?,PushResult::Refused {reason:DeliveryRefusal::TooOld}));
        let DeliveryEnvelope::Item(original)=item(1,"old") else {return Err(StoreError::backend("original"));};
        assert!(too_old_error(Inbox::<_,TestTypes>::new(scope,peer(2),limits()).enqueue_local(original,&TestConsumer,day(61)).await));
        let row=query::<Turso>("SELECT (SELECT count(*) FROM _fleet_sync_outbox_items)+(SELECT count(*) FROM _fleet_sync_inbox_items)+(SELECT count(*) FROM _fleet_sync_operations) AS rows").fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<i64,_>("rows")?,0);
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn public_sender_fleet_push_and_local_admission_schedule_removal_from_creation_time() {
    use super::support::network::{PublishTask, SubmitTask};
    for role in ["sender", "fleet", "push", "local"] {
        for admitted_day in [2, 5, 11] {
            let network = super::support::network::NetworkFixture::new().await;
            let mut fixture = network.recipient;
            fixture
                .host
                .clock
                .store(day(admitted_day).unix_millis() as u64, Ordering::SeqCst);
            for sequence in 1..=4 {
                let envelope = envelope_at(
                    CREATED
                        + if sequence == 4 {
                            24 * 60 * 60 * 1000
                        } else {
                            0
                        },
                    sequence,
                );
                let identity = envelope.id();
                let admission = fixture
                    .store
                    .transaction(move |scope| {
                        Box::pin(async move {
                            match role {
                                "sender" => {
                                    let DeliveryEnvelope::Item(original) = envelope else {
                                        return Err(StoreError::backend("original"));
                                    };
                                    let (admission, _) =
                                        Outbox::<_, TestTypes>::new(scope, limits())
                                            .enqueue_and_admit::<SubmitTask, RecordRemoval>(
                                                original,
                                                peer(3),
                                                fleet_sync_delivery::Submitter::Machine(peer(1)),
                                                day(admitted_day),
                                            )
                                            .await
                                            .map_err(storage_error)?;
                                    assert!(matches!(admission, OutboxAdmission::Queued { .. }));
                                }
                                "fleet" => {
                                    let answer = Outbox::<_, TestTypes>::new(scope, limits())
                                        .submit_relay::<PushTask, RecordRemoval>(
                                            fleet_sync_delivery::Submitter::Machine(peer(1)),
                                            envelope,
                                            peer(3),
                                            "route".into(),
                                            day(admitted_day),
                                        )
                                        .await
                                        .map_err(storage_error)?;
                                    assert!(matches!(answer, SubmitResult::Accepted { .. }));
                                }
                                "push" => {
                                    let answer = Inbox::<_, TestTypes>::new(
                                        scope,
                                        peer(2),
                                        limits(),
                                    )
                                    .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                                        fleet_sync_delivery::Submitter::Machine(peer(1)),
                                        envelope,
                                        peer(3),
                                        &TestConsumer,
                                        day(admitted_day),
                                    )
                                    .await
                                    .map_err(storage_error)?;
                                    assert!(matches!(answer, PushResult::Accepted { .. }));
                                }
                                _ => {
                                    let DeliveryEnvelope::Item(original) = envelope else {
                                        return Err(StoreError::backend("original"));
                                    };
                                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                                        .enqueue_local_and_admit::<DeliverTask, RecordRemoval, _>(
                                            original,
                                            &TestConsumer,
                                            day(admitted_day),
                                        )
                                        .await
                                        .map_err(storage_error)?;
                                }
                            }
                            // Observe the already-admitted delivery input; no removal admission here.
                            Ok(DeliveryAttempt {
                                item: identity,
                                attempt: 0,
                                not_before: day(admitted_day),
                            })
                        })
                    })
                    .await
                    .unwrap();
                if sequence == 2 || sequence == 3 {
                    if role == "sender" || role == "fleet" {
                        fixture
                            .store
                            .transaction(move |scope| {
                                Box::pin(async move {
                                    if role == "sender" {
                                        let status = if sequence == 2 {
                                            DeliveryStatus::Held
                                        } else {
                                            DeliveryStatus::Delivered {
                                                acceptance: Acceptance {
                                                    receipt: "finished".into(),
                                                },
                                            }
                                        };
                                        Outbox::<_, TestTypes>::new(scope, limits())
                                            .publish_status(
                                                StatusPublication {
                                                    item: identity,
                                                    status,
                                                    version: StatusVersion::new(2),
                                                    cancel: None,
                                                },
                                                day(admitted_day),
                                            )
                                            .await
                                            .map_err(storage_error)?;
                                    } else {
                                        let outcome = if sequence == 2 {
                                            RecipientOutcome::Held
                                        } else {
                                            RecipientOutcome::Delivered {
                                                acceptance: Acceptance {
                                                    receipt: "finished".into(),
                                                },
                                            }
                                        };
                                        let answer = Outbox::<_, TestTypes>::new(scope, limits())
                                            .recipient_report::<PublishTask>(
                                                peer(2),
                                                RecipientReport {
                                                    item: identity,
                                                    report_seq: ReportSeq::new(1),
                                                    outcome,
                                                },
                                                day(admitted_day),
                                            )
                                            .await
                                            .map_err(storage_error)?;
                                        assert!(matches!(answer, ReportResult::Ack { .. }));
                                    }
                                    Ok(())
                                })
                            })
                            .await
                            .unwrap();
                    } else {
                        network
                            .routes
                            .local_answers
                            .lock()
                            .unwrap()
                            .push_back(if sequence == 2 {
                                LocalOutcome::Held { until: None }
                            } else {
                                LocalOutcome::Delivered {
                                    acceptance: Acceptance {
                                        receipt: "finished".into(),
                                    },
                                }
                            });
                        let mut engine = Engine::new(fixture.store.clone(), fixture.host.clone());
                        engine
                            .register(DeliverTask::new(
                                super::support::network::config().local,
                                Arc::new(TestConsumer),
                                fixture.host.clone(),
                            ))
                            .unwrap();
                        let result = engine
                            .start_and_settle::<DeliverTask>(
                                &delivery_request_key(&(identity, 0u64)).unwrap(),
                                admission,
                            )
                            .await
                            .unwrap();
                        assert!(matches!(
                            result.state,
                            durable_steps::OperationState::Completed { .. }
                        ));
                    }
                }
            }
            defer_delivery_work(&fixture, day(62)).await;
            fixture = reopen(fixture).await;
            *network.routes.recipient.lock().unwrap() = Arc::downgrade(&fixture.store);
            let side = if role == "sender" || role == "fleet" {
                "outbox"
            } else {
                "inbox"
            };
            let before = identities(&fixture.store, side).await;
            assert_eq!(before.len(), 4);
            for at in [
                day(59),
                SyncTime::from_unix_millis(day(60).unix_millis() - 1),
            ] {
                fixture
                    .host
                    .clock
                    .store(at.unix_millis() as u64, Ordering::SeqCst);
                let result = retention_engine(&fixture).drive_due().await.unwrap();
                assert!(result.completed.is_empty() && result.failed.is_empty());
                assert_eq!(identities(&fixture.store, side).await, before);
            }
            fixture
                .host
                .clock
                .store(day(60).unix_millis() as u64, Ordering::SeqCst);
            let result = retention_engine(&fixture).drive_due().await.unwrap();
            assert_eq!(
                result.completed.len(),
                3,
                "{role} must persist removal in admission"
            );
            assert!(result.failed.is_empty());
            assert_eq!(
                identities(&fixture.store, side).await,
                vec![
                    envelope_at(day(1).unix_millis(), 4)
                        .id()
                        .as_uuid()
                        .to_string()
                ]
            );
            fixture
                .host
                .clock
                .store(day(61).unix_millis() as u64, Ordering::SeqCst);
            assert_eq!(
                retention_engine(&fixture)
                    .drive_due()
                    .await
                    .unwrap()
                    .completed
                    .len(),
                1
            );
            assert!(identities(&fixture.store, side).await.is_empty());
        }
    }
}
