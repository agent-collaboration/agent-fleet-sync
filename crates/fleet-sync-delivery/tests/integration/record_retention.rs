use super::support::{
    network::{DeliverTask, PushTask, ReportTask},
    *,
};
use durable_steps::{AtomicStore, Engine, JournalScope, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

const CREATED: i64 = 1_717_986_918_400;
fn day(days: i64) -> SyncTime {
    SyncTime::from_unix_millis(CREATED + days * 24 * 60 * 60 * 1000)
}
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
        assert!(matches!(Outbox::<_,TestTypes>::new(scope,limits()).enqueue(original,peer(9),peer(1),day(61)).await.map_err(storage_error)?,OutboxAdmission::Refused {reason:DeliveryRefusal::TooOld}));
        assert!(matches!(Outbox::<_,TestTypes>::new(scope,limits()).submit_relay::<PushTask,RecordRemoval>(peer(1),item(1,"old"),peer(9),"route".into(),day(61)).await.map_err(storage_error)?,SubmitResult::Refused {reason:DeliveryRefusal::TooOld}));
        assert!(matches!(Inbox::<_,TestTypes>::new(scope,peer(2),limits()).accept_push::<DeliverTask,ReportTask,RecordRemoval,_>(item(1,"old"),peer(9),&TestConsumer,day(61)).await.map_err(storage_error)?,PushResult::Refused {reason:DeliveryRefusal::TooOld}));
        let DeliveryEnvelope::Item(original)=item(1,"old") else {return Err(StoreError::backend("original"));};
        assert!(too_old_error(Inbox::<_,TestTypes>::new(scope,peer(2),limits()).enqueue_local(original,&TestConsumer,day(61)).await));
        let row=query::<Turso>("SELECT (SELECT count(*) FROM _fleet_sync_outbox_items)+(SELECT count(*) FROM _fleet_sync_inbox_items)+(SELECT count(*) FROM _fleet_sync_operations) AS rows").fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<i64,_>("rows")?,0);
        Ok(())
    })).await.unwrap();
}

fn envelope_at(milliseconds: i64, sequence: u64) -> DeliveryEnvelope<TestTypes> {
    let id = ItemId::new(durable_steps::Uuid::from_u128(
        ((milliseconds as u128) << 80) | 0x70008000000000000000 | u128::from(sequence),
    ))
    .unwrap();
    DeliveryEnvelope::Item(
        DeliveryItem::new(
            id,
            "sender".into(),
            Recipient {
                peer: peer(2),
                address: "inbox".into(),
            },
            None,
            "retention body".into(),
        )
        .unwrap(),
    )
}
async fn identities(store: &TestStore, side: &str) -> Vec<String> {
    let statement = match side {
        "outbox" => "SELECT item_id FROM _fleet_sync_outbox_items ORDER BY item_id",
        _ => "SELECT item_id FROM _fleet_sync_inbox_items ORDER BY item_id",
    };
    store
        .transaction(move |scope| {
            Box::pin(async move {
                query::<Turso>(statement)
                    .fetch_all(scope.executor())
                    .await?
                    .iter()
                    .map(|row| row.try_get("item_id"))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(StoreError::backend)
            })
        })
        .await
        .unwrap()
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
fn retention_engine(fixture: &Fixture) -> Engine<TestStore, TestHost> {
    let mut engine = Engine::new(fixture.store.clone(), fixture.host.clone());
    engine.register(RecordRemoval::new()).unwrap();
    engine
}

#[tokio::test]
async fn actual_removal_task_uses_creation_time_for_live_held_terminal_rows_and_preserves_younger_records()
 {
    for side in ["outbox", "inbox"] {
        // Each role admits at a different time; the UUIDv7 time owns retention.
        for admitted_day in [2, 5, 11] {
            let network = super::support::network::NetworkFixture::new().await;
            network
                .routes
                .local_answers
                .lock()
                .unwrap()
                .push_back(LocalOutcome::Held { until: None });
            let mut fixture = network.recipient;
            fixture
                .host
                .clock
                .store(day(admitted_day).unix_millis() as u64, Ordering::SeqCst);
            fixture
                .store
                .transaction(move |scope| {
                    Box::pin(async move {
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
                            let DeliveryEnvelope::Item(original) = envelope else {
                                return Err(StoreError::backend("original"));
                            };
                            if side == "outbox" {
                                Outbox::<_, TestTypes>::new(scope, limits())
                                    .enqueue(original, peer(9), peer(1), day(admitted_day))
                                    .await
                                    .map_err(storage_error)?;
                                if sequence == 2 || sequence == 3 {
                                    let status = if sequence == 2 {
                                        DeliveryStatus::Held
                                    } else {
                                        DeliveryStatus::Delivered {
                                            acceptance: Acceptance {
                                                receipt: "finished before retention".into(),
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
                                            day(admitted_day + 1),
                                        )
                                        .await
                                        .map_err(storage_error)?;
                                }
                            } else {
                                let admission =
                                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                                        .enqueue_local(original, &TestConsumer, day(admitted_day))
                                        .await
                                        .map_err(storage_error)?;
                                if sequence == 2 {
                                    let attempt = require_local_attempt(admission)?;
                                    scope
                                        .admit::<DeliverTask>(
                                            &delivery_request_key(&(attempt.item, attempt.attempt))
                                                .map_err(storage_error)?,
                                            &attempt,
                                        )
                                        .await
                                        .map_err(StoreError::backend)?;
                                } else if sequence == 3 {
                                    let attempt = require_local_attempt(admission)?;
                                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                                        .record_failure(
                                            attempt,
                                            DeliveryFailureKind::ExecutionFailed,
                                            &TestConsumer,
                                            day(admitted_day + 1),
                                        )
                                        .await
                                        .map_err(storage_error)?;
                                }
                            }
                            admit_record_removal::<_, RecordRemoval>(scope, identity)
                                .await
                                .map_err(storage_error)?;
                        }
                        Ok(())
                    })
                })
                .await
                .unwrap();
            if side == "inbox" {
                let mut delivery = Engine::new(fixture.store.clone(), fixture.host.clone());
                delivery
                    .register(DeliverTask::new(
                        super::support::network::config().local,
                        Arc::new(TestConsumer),
                        fixture.host.clone(),
                    ))
                    .unwrap();
                assert_eq!(delivery.drive_due().await.unwrap().completed.len(), 1);
                fixture
                    .store
                    .transaction(|scope| {
                        Box::pin(async move {
                            assert!(matches!(
                                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                                    .local_outcome(envelope_at(CREATED, 2).id())
                                    .await
                                    .map_err(storage_error)?,
                                Some(RecipientOutcome::Held)
                            ));
                            Ok(())
                        })
                    })
                    .await
                    .unwrap();
            }
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
                let drive = retention_engine(&fixture).drive_due().await.unwrap();
                assert!(drive.completed.is_empty() && drive.failed.is_empty());
                assert_eq!(identities(&fixture.store, side).await, before);
            }
            fixture = reopen(fixture).await;
            fixture
                .host
                .clock
                .store(day(60).unix_millis() as u64, Ordering::SeqCst);
            let drive = retention_engine(&fixture).drive_due().await.unwrap();
            assert_eq!(drive.completed.len(), 3);
            assert!(drive.failed.is_empty());
            let younger = envelope_at(day(1).unix_millis(), 4)
                .id()
                .as_uuid()
                .to_string();
            assert_eq!(identities(&fixture.store, side).await, vec![younger]);
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
            fixture
                .store
                .transaction(move |scope| {
                    Box::pin(async move {
                        let DeliveryEnvelope::Item(original) = item(1, "retention body") else {
                            return Err(StoreError::backend("original"));
                        };
                        if side == "outbox" {
                            assert!(matches!(
                                Outbox::<_, TestTypes>::new(scope, limits())
                                    .enqueue(original, peer(9), peer(1), day(61))
                                    .await
                                    .map_err(storage_error)?,
                                OutboxAdmission::Refused {
                                    reason: DeliveryRefusal::TooOld
                                }
                            ));
                        } else {
                            assert!(too_old_error(
                                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                                    .enqueue_local(original, &TestConsumer, day(61))
                                    .await
                            ));
                        }
                        let DeliveryEnvelope::Item(fresh) = envelope_at(day(61).unix_millis(), 8)
                        else {
                            return Err(StoreError::backend("fresh"));
                        };
                        if side == "outbox" {
                            assert!(matches!(
                                Outbox::<_, TestTypes>::new(scope, limits())
                                    .enqueue(fresh, peer(9), peer(1), day(61))
                                    .await
                                    .map_err(storage_error)?,
                                OutboxAdmission::Queued { .. }
                            ));
                        } else {
                            assert!(matches!(
                                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                                    .enqueue_local(fresh, &TestConsumer, day(61))
                                    .await
                                    .map_err(storage_error)?,
                                LocalItemAdmission::Attempt(_)
                            ));
                        }
                        Ok(())
                    })
                })
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn pending_hop_and_local_tasks_finish_after_removal_without_redispatch_or_recreating_rows() {
    let network = super::support::network::NetworkFixture::new().await;
    network.routes.fleet_push.store(true, Ordering::SeqCst);
    network.enqueue(1, "pending sender").await;
    network
        .mailbox
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes>::new(scope, limits())
                    .submit_relay::<PushTask, RecordRemoval>(
                        peer(1),
                        item(1, "pending sender"),
                        peer(3),
                        "route".into(),
                        day(1),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                        item(1, "pending sender"),
                        peer(3),
                        &TestConsumer,
                        day(1),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    for fixture in [&network.sender, &network.mailbox, &network.recipient] {
        fixture
            .host
            .clock
            .store(day(60).unix_millis() as u64, Ordering::SeqCst);
        retention_engine(fixture)
            .start_and_settle::<RecordRemoval>(
                &delivery_request_key(&item_id(1)).unwrap(),
                item_id(1),
            )
            .await
            .unwrap();
        assert!(identities(&fixture.store, "outbox").await.is_empty());
        assert!(identities(&fixture.store, "inbox").await.is_empty());
        fixture
            .host
            .clock
            .store(day(61).unix_millis() as u64, Ordering::SeqCst);
        let result = network.engine(fixture).drive_due().await.unwrap();
        assert!(
            result.failed.is_empty(),
            "old tasks must finish without reading removed custody: {result:?}"
        );
        assert!(result.retried.is_empty());
        assert!(identities(&fixture.store, "outbox").await.is_empty());
        assert!(identities(&fixture.store, "inbox").await.is_empty());
    }
    assert_eq!(network.routes.sent_bodies.load(Ordering::SeqCst), 0);
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 0);
}
