use super::support::{
    network::*,
    sender_binding::{create_binding_directory, insert_binding},
    *,
};
use durable_steps::{AtomicStore, CallTarget, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;

// Consumer-owned ServiceId values deliberately remain UUIDv4 strings. They are
// never parsed as PeerId or used as a library row identity.
const SERVICE_A: &str = "11111111-1111-4111-8111-111111111111";
const SERVICE_B: &str = "22222222-2222-4222-8222-222222222222";
fn now() -> SyncTime {
    SyncTime::from_unix_millis(0)
}
fn service_item(identity: u64, service: &str) -> DeliveryItem<TestTypes> {
    DeliveryItem::new(
        item_id(identity),
        format!("svc:{service}|session"),
        Recipient {
            peer: peer(2),
            address: "inbox".into(),
        },
        None,
        "old sealed bytes".into(),
    )
    .unwrap()
}
async fn fixture() -> Fixture {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| Box::pin(async move { create_binding_directory(scope).await }))
        .await
        .unwrap();
    fixture
}
async fn counts(store: &TestStore) -> (i64, i64, i64, i64) {
    store.transaction(|scope|Box::pin(async move {
        let row=query::<Turso>("SELECT (SELECT count(*) FROM _fleet_sync_outbox_items) AS outbox,(SELECT count(*) FROM _fleet_sync_inbox_items) AS inbox,(SELECT count(*) FROM _fleet_sync_operations) AS operations,(SELECT count(*) FROM _fleet_sync_delivery_backfills) AS markers").fetch_one(scope.executor()).await?;
        Ok((row.try_get("outbox")?,row.try_get("inbox")?,row.try_get("operations")?,row.try_get("markers")?))
    })).await.unwrap()
}

#[tokio::test]
async fn binding_lookup_sees_uncommitted_host_truth_in_the_supplied_custody_scope() {
    let fixture = fixture().await;
    let result: Result<(), StoreError> = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                insert_binding(scope, 90, SERVICE_A, peer(9)).await?;
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .enqueue(
                            service_item(1, SERVICE_A),
                            peer(3),
                            Submitter::Machine(peer(9)),
                            now()
                        )
                        .await
                        .map_err(storage_error)?,
                    OutboxAdmission::Queued { .. }
                ));
                Err(StoreError::backend(
                    "deliberate rollback of binding and custody",
                ))
            })
        })
        .await;
    assert!(result.is_err());
    assert_eq!(counts(&fixture.store).await, (0, 0, 0, 0));
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = query::<Turso>("SELECT count(*) AS count FROM test_sender_bindings")
                    .fetch_one(scope.executor())
                    .await?;
                assert_eq!(row.try_get::<i64, _>("count")?, 0);
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn missing_admission_binding_is_a_non_refusal_and_writes_nothing() {
    unavailable_admission(false).await;
}
#[tokio::test]
async fn contradictory_admission_binding_is_visible_retryable_and_writes_nothing() {
    unavailable_admission(true).await;
}
async fn unavailable_admission(conflict: bool) {
    let fixture = fixture().await;
    if conflict {
        fixture
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    insert_binding(scope, 90, SERVICE_A, peer(9)).await?;
                    insert_binding(scope, 91, SERVICE_A, peer(8)).await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
    let before = counts(&fixture.store).await;
    fixture.store.transaction(move |scope|Box::pin(async move {
        let answer=Outbox::<_,TestTypes,_>::new(scope,limits(),&FixtureBindings).enqueue(service_item(1,SERVICE_A),peer(3),Submitter::Machine(peer(9)),now()).await;
        let expected=if conflict {SenderBindingFailure::Conflict} else {SenderBindingFailure::Unknown};
        assert!(matches!(answer,Ok(OutboxAdmission::BindingUnavailable {reason}) if reason==expected));
        Ok(())
    })).await.unwrap();
    assert_eq!(counts(&fixture.store).await, before);
}

#[tokio::test]
async fn known_service_owner_refuses_another_claim_without_writes_or_a_fabricated_peer() {
    let fixture = fixture().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move { insert_binding(scope, 90, SERVICE_A, peer(9)).await })
        })
        .await
        .unwrap();
    let before = counts(&fixture.store).await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .enqueue(
                            service_item(1, SERVICE_A),
                            peer(3),
                            Submitter::Machine(peer(1)),
                            now()
                        )
                        .await
                        .map_err(storage_error)?,
                    OutboxAdmission::Refused {
                        reason: DeliveryRefusal::NotSubmitter
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(counts(&fixture.store).await, before);
}

#[tokio::test]
async fn unknown_push_binding_is_retryable_without_custody_projection_or_work() {
    let fixture = fixture().await;
    let before = counts(&fixture.store).await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let answer =
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                        .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                            Submitter::Machine(peer(9)),
                            DeliveryEnvelope::Item(service_item(1, SERVICE_A)),
                            peer(3),
                            &TestConsumer,
                            now(),
                        )
                        .await;
                assert!(matches!(
                    answer,
                    Ok(PushResult::BindingUnavailable {
                        reason: SenderBindingFailure::Unknown
                    })
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(counts(&fixture.store).await, before);
}

#[tokio::test]
async fn confirmed_rollback_preserves_binding_row_and_reason_for_immediate_host_refresh() {
    let fixture = fixture().await;
    let error = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                // The host's unrelated write must roll back before it refreshes.
                insert_binding(scope, 90, SERVICE_B, peer(8)).await?;
                Err::<(), _>(delivery_rollback(DeliveryError::SenderBindingUnavailable {
                    row: CustodyRow::Inbox(item_id(1)),
                    reason: SenderBindingFailure::Unknown,
                }))
            })
        })
        .await
        .unwrap_err();
    assert!(
        matches!(classify_delivery_store_error(error), DeliveryTransactionFailure::BindingUnavailable {row: CustodyRow::Inbox(item), reason:SenderBindingFailure::Unknown} if item==item_id(1))
    );
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let count: i64 =
                    query::<Turso>("SELECT count(*) AS count FROM test_sender_bindings")
                        .fetch_one(scope.executor())
                        .await?
                        .try_get("count")?;
                assert_eq!(count, 0);
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn forged_service_claim_is_rejected_at_relay_and_recipient_without_new_work() {
    let fixture = fixture().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move { insert_binding(scope, 90, SERVICE_A, peer(9)).await })
        })
        .await
        .unwrap();
    let before = counts(&fixture.store).await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .submit_relay::<PushTask, RecordRemoval>(
                            Submitter::Machine(peer(1)),
                            DeliveryEnvelope::Item(service_item(1, SERVICE_A)),
                            peer(3),
                            "route".into(),
                            now()
                        )
                        .await
                        .map_err(storage_error)?,
                    SubmitResult::Refused {
                        reason: DeliveryRefusal::NotSubmitter
                    }
                ));
                assert!(matches!(
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                        .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                            Submitter::Machine(peer(1)),
                            DeliveryEnvelope::Item(service_item(1, SERVICE_A)),
                            peer(3),
                            &TestConsumer,
                            now()
                        )
                        .await
                        .map_err(storage_error)?,
                    PushResult::Refused {
                        reason: DeliveryRefusal::NotSubmitter
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(counts(&fixture.store).await, before);
}

#[tokio::test]
async fn reverse_binding_conflict_is_not_misreported_as_sender_forgery() {
    let fixture = fixture().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                insert_binding(scope, 90, SERVICE_A, peer(9)).await?;
                insert_binding(scope, 91, SERVICE_B, peer(9)).await?;
                assert!(matches!(
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                        .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                            Submitter::Machine(peer(9)),
                            DeliveryEnvelope::Item(service_item(1, SERVICE_A)),
                            peer(3),
                            &TestConsumer,
                            now()
                        )
                        .await
                        .map_err(storage_error)?,
                    PushResult::BindingUnavailable {
                        reason: SenderBindingFailure::Conflict
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(counts(&fixture.store).await, (0, 0, 0, 0));
}

#[tokio::test]
async fn failed_rollback_never_claims_a_binding_is_safe_to_refresh() {
    let fixture = fixture().await;
    let error = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                query::<Turso>("ROLLBACK").execute(scope.executor()).await?;
                Err::<(), _>(delivery_rollback(DeliveryError::SenderBindingUnavailable {
                    row: CustodyRow::Inbox(item_id(1)),
                    reason: SenderBindingFailure::Unknown,
                }))
            })
        })
        .await
        .unwrap_err();
    assert!(matches!(
        classify_delivery_store_error(error),
        DeliveryTransactionFailure::OutcomeUnknown(_)
    ));
}

#[tokio::test]
async fn submit_dispatch_binding_miss_retries_without_settling_the_fleet_custody() {
    use std::sync::atomic::Ordering;
    let network = NetworkFixture::new().await;
    network.routes.fleet_push.store(true, Ordering::SeqCst);
    *network.routes.authenticated_submitter.lock().unwrap() = Submitter::Machine(peer(9));
    let reference = network
        .sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                create_binding_directory(scope).await?;
                insert_binding(scope, 90, SERVICE_A, peer(9)).await?;
                let item = service_item(1, SERVICE_A);
                let reference = DeliveryItemRef {
                    item: item.id(),
                    digest: item.digest().clone(),
                };
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .enqueue(item, peer(3), Submitter::Machine(peer(9)), now())
                        .await
                        .map_err(storage_error)?,
                    OutboxAdmission::Queued { .. }
                ));
                query::<Turso>("DELETE FROM test_sender_bindings")
                    .execute(scope.executor())
                    .await?;
                Ok(reference)
            })
        })
        .await
        .unwrap();
    let before = counts(&network.mailbox.store).await;
    let answer = network
        .routes
        .dispatch(
            SubmitCall::<TestTypes>::KIND,
            serde_json::to_value(SubmitCall::<TestTypes>::new(peer(3), peer(2))).unwrap(),
            serde_json::to_value(reference).unwrap(),
            call("submit"),
            now(),
        )
        .await;
    assert!(
        matches!(answer,Err(durable_steps::CallFailure::Retryable {reason}) if reason.code=="senderBindingUnknown")
    );
    assert_eq!(counts(&network.mailbox.store).await, before);
    assert_eq!(network.routes.settlements.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn missing_cancel_or_relay_binding_admits_no_submission_removal_or_custody_work() {
    let fixture = fixture().await;
    let before = counts(&fixture.store).await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let cancel = DeliveryCancel::<TestTypes>::new(
                    item_id(2),
                    format!("svc:{SERVICE_A}|session"),
                    Recipient {
                        peer: peer(2),
                        address: "inbox".into(),
                    },
                    None,
                    CancelBody { target: item_id(1) },
                )
                .unwrap();
                let (answer, admitted) =
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .enqueue_cancel_and_admit::<SubmitTask, RecordRemoval, _>(
                            cancel,
                            peer(3),
                            Submitter::Machine(peer(9)),
                            now(),
                            &TestConsumer,
                        )
                        .await
                        .map_err(storage_error)?;
                assert!(matches!(
                    answer,
                    OutboxAdmission::BindingUnavailable {
                        reason: SenderBindingFailure::Unknown
                    }
                ));
                assert!(admitted.is_none());
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .submit_relay::<PushTask, RecordRemoval>(
                            Submitter::Machine(peer(9)),
                            DeliveryEnvelope::Item(service_item(1, SERVICE_A)),
                            peer(3),
                            "route".into(),
                            now()
                        )
                        .await
                        .map_err(storage_error)?,
                    SubmitResult::BindingUnavailable {
                        reason: SenderBindingFailure::Unknown
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(counts(&fixture.store).await, before);
}

#[tokio::test]
async fn unavailable_host_directory_is_a_retryable_lookup_failure_and_never_sender_forgery() {
    let fixture = Fixture::new().await; // The host's directory is unavailable.
    let before = counts(&fixture.store).await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .enqueue(
                            service_item(1, SERVICE_A),
                            peer(3),
                            Submitter::Machine(peer(9)),
                            now()
                        )
                        .await
                        .map_err(storage_error)?,
                    OutboxAdmission::BindingUnavailable {
                        reason: SenderBindingFailure::Unavailable
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(counts(&fixture.store).await, before);
}
