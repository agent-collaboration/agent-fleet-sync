use super::support::{
    network::{DeliverTask, PublishTask},
    *,
};
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
type PushTask = PushItem<TestStore, Turso, TestTypes, PublishTask, FixtureBindings>;

fn reference(identity: u64, body: &str) -> DeliveryItemRef {
    DeliveryItemRef {
        item: item_id(identity),
        digest: item(identity, body).digest().clone(),
    }
}
async fn admit(
    store: &TestStore,
    identity: u64,
    body: &str,
    submitter: u128,
    recipient: u128,
) -> SubmitResult<Acceptance> {
    let body = body.to_owned();
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let envelope = DeliveryEnvelope::Item(
                    DeliveryItem::<TestTypes>::new(
                        item_id(identity),
                        if submitter == 1 {
                            "sender".to_owned()
                        } else {
                            owned_sender(Submitter::Machine(peer(submitter)), "session")
                        },
                        Recipient {
                            peer: peer(recipient),
                            address: "inbox".into(),
                        },
                        None,
                        body,
                    )
                    .unwrap(),
                );
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .submit_relay::<PushTask, RecordRemoval>(
                        fleet_sync_delivery::Submitter::Machine(peer(submitter)),
                        envelope,
                        peer(9),
                        format!("route-{submitter}"),
                        SyncTime::from_unix_millis(11),
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}

async fn snapshot(store: &TestStore) -> Vec<String> {
    store.transaction(|scope|Box::pin(async move {
        query::<Turso>("SELECT json_array(item_id,digest,status_kind,status_version,last_report_seq,payload,handed_off_at,updated_at) AS state FROM _fleet_sync_outbox_items UNION ALL SELECT json_array(item_id,target_kind,status_route,version,status_kind,status_acceptance,cancel_outcome,submitter_kind,submitter_id) AS state FROM _fleet_sync_status_owed UNION ALL SELECT json_array(submitter_kind,submitter_id,cursor,item_id,version,status_kind,status_acceptance,cancel_outcome) AS state FROM _fleet_sync_status_events ORDER BY state")
            .fetch_all(scope.executor()).await?.iter().map(|row|row.try_get("state")).collect::<Result<Vec<_>,_>>().map_err(StoreError::backend)
    })).await.unwrap()
}

async fn receipt(
    store: &TestStore,
    caller: u128,
    identity: u64,
    body: &str,
) -> StatusPublicationResult {
    let reference = if identity == 2 {
        DeliveryItemRef {
            item: item_id(identity),
            digest: DeliveryItem::<TestTypes>::new(
                item_id(identity),
                owned_sender(Submitter::Machine(peer(3)), "session"),
                Recipient {
                    peer: peer(3),
                    address: "inbox".into(),
                },
                None,
                body.to_owned(),
            )
            .unwrap()
            .digest()
            .clone(),
        }
    } else {
        reference(identity, body)
    };
    store
        .transaction(move |scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .receipt::<PublishTask>(
                        peer(caller),
                        &reference,
                        PushResult::Accepted {
                            accepted_at: SyncTime::from_unix_millis(18),
                        },
                        SyncTime::from_unix_millis(20),
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}
async fn report(
    store: &TestStore,
    caller: u128,
    identity: u64,
    sequence: u64,
    outcome: RecipientOutcome<Acceptance>,
) -> ReportResult {
    store
        .transaction(move |scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .recipient_report::<PublishTask>(
                        peer(caller),
                        RecipientReport {
                            item: item_id(identity),
                            report_seq: ReportSeq::new(sequence),
                            outcome,
                        },
                        SyncTime::from_unix_millis(25),
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn recipient_authority_is_per_item_before_and_after_release_and_reports_never_regress() {
    let Fixture {
        store,
        host,
        directory,
    } = Fixture::new().await;
    assert!(
        matches!(admit(&store,1,"body-a",1,2).await,SubmitResult::Accepted {status:DeliveryStatus::Sent,version,..} if version.value()==1)
    );
    admit(&store, 2, "body-c", 3, 3).await;
    let original = snapshot(&store).await;
    assert!(matches!(
        receipt(&store, 3, 1, "body-a").await,
        StatusPublicationResult::Refused {
            reason: DeliveryRefusal::NotRecipient
        }
    ));
    assert!(matches!(
        report(&store, 3, 1, 1, RecipientOutcome::Held).await,
        ReportResult::Refused {
            reason: DeliveryRefusal::NotRecipient
        }
    ));
    assert_eq!(snapshot(&store).await, original);
    assert!(matches!(
        receipt(&store, 2, 1, "body-a").await,
        StatusPublicationResult::Applied
    ));
    let released = snapshot(&store).await;
    assert!(matches!(
        receipt(&store, 2, 1, "body-a").await,
        StatusPublicationResult::Applied
    ));
    assert_eq!(
        snapshot(&store).await,
        released,
        "duplicate receipt rewrote retained state"
    );
    assert!(matches!(
        receipt(&store, 3, 1, "body-a").await,
        StatusPublicationResult::Refused {
            reason: DeliveryRefusal::NotRecipient
        }
    ));
    assert!(matches!(
        report(&store, 3, 1, 1, RecipientOutcome::Held).await,
        ReportResult::Refused {
            reason: DeliveryRefusal::NotRecipient
        }
    ));
    assert_eq!(snapshot(&store).await, released);
    assert!(
        matches!(report(&store,2,1,1,RecipientOutcome::Held).await,ReportResult::Ack {report_seq} if report_seq.value()==1)
    );
    assert!(
        matches!(report(&store,2,1,2,RecipientOutcome::Delivered {acceptance:Acceptance {receipt:"one final receipt".into()}}).await,ReportResult::Ack {report_seq} if report_seq.value()==2)
    );
    let final_state = snapshot(&store).await;
    drop(store);
    let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
        .await
        .unwrap();
    assert!(
        matches!(report(&reopened,2,1,2,RecipientOutcome::Delivered {acceptance:Acceptance {receipt:"one final receipt".into()}}).await,ReportResult::Replayed {report_seq} if report_seq.value()==2)
    );
    assert!(
        matches!(report(&reopened,2,1,1,RecipientOutcome::Held).await,ReportResult::Replayed {report_seq} if report_seq.value()==1)
    );
    assert_eq!(snapshot(&reopened).await, final_state);
    assert!(
        matches!(admit(&reopened,1,"body-a",1,2).await,SubmitResult::Replayed {status:DeliveryStatus::Delivered {acceptance},version,..} if acceptance.receipt=="one final receipt"&&version.value()==4)
    );
    assert!(matches!(
        admit(&reopened, 1, "different", 1, 2).await,
        SubmitResult::Refused {
            reason: DeliveryRefusal::ItemConflict { .. }
        }
    ));
    assert_eq!(snapshot(&reopened).await, final_state);
    assert!(matches!(
        receipt(&reopened, 3, 2, "body-c").await,
        StatusPublicationResult::Applied
    ));
    reopened.transaction(|scope|Box::pin(async move {
        let row=query::<Turso>("SELECT payload,status_kind,last_report_seq,created_at,admitted_at,handed_off_at FROM _fleet_sync_outbox_items WHERE item_id=?1").bind(item_id(1).as_uuid().to_string()).fetch_one(scope.executor()).await?;
        assert!(row.try_get::<Option<String>,_>("payload")?.is_none());
        assert_eq!(row.try_get::<String,_>("status_kind")?,"delivered");
        assert_eq!(row.try_get::<i64,_>("last_report_seq")?,2);
        assert_eq!(row.try_get::<i64,_>("created_at")?,1_717_986_918_400);
        assert_eq!(row.try_get::<i64,_>("admitted_at")?,11);
        assert_eq!(row.try_get::<i64,_>("handed_off_at")?,20);
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn rolled_back_next_holder_acceptance_keeps_content_and_receipt_release_is_atomic_with_status_debt()
 {
    let sender = Fixture::new().await;
    let fleet = Fixture::new().await;
    let recipient = Fixture::new().await;
    sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(original) = item(1, "body-a") else {
                    return Err(StoreError::backend("original"));
                };
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .enqueue(
                        original,
                        peer(9),
                        fleet_sync_delivery::Submitter::Machine(peer(1)),
                        SyncTime::from_unix_millis(10),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let rolled_back = fleet
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .submit_relay::<PushTask, RecordRemoval>(
                        fleet_sync_delivery::Submitter::Machine(peer(1)),
                        item(1, "body-a"),
                        peer(9),
                        "route-1".into(),
                        SyncTime::from_unix_millis(11),
                    )
                    .await
                    .map_err(storage_error)?;
                Err::<(), _>(StoreError::backend("rollback Fleet admission"))
            })
        })
        .await;
    assert!(rolled_back.is_err());
    sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .resolve_submit(&reference(1, "body-a"))
                        .await
                        .map_err(storage_error)?
                        .item
                        .id()
                        == item_id(1)
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    let fleet_answer = admit(&fleet.store, 1, "body-a", 1, 2).await;
    sender
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .mark_submitted(
                        &SubmitInput {
                            item: item_id(1),
                            digest: item(1, "body-a").digest().clone(),
                            relay: peer(9),
                            mailbox: peer(2),
                        },
                        fleet_answer,
                        SyncTime::from_unix_millis(12),
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    let before_recipient = snapshot(&fleet.store).await;
    let aborted = recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                    .accept_push::<DeliverTask,super::support::network::ReportTask,RecordRemoval,_>(fleet_sync_delivery::Submitter::Machine(peer(1)),
                        item(1, "body-a"),
                        peer(9),
                        &TestConsumer,
                        SyncTime::from_unix_millis(18),
                    )
                    .await
                    .map_err(storage_error)?;
                Err::<(), _>(StoreError::backend("rollback B acceptance"))
            })
        })
        .await;
    assert!(aborted.is_err());
    assert_eq!(snapshot(&fleet.store).await, before_recipient);
    let answer = recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                    .accept_push::<DeliverTask,super::support::network::ReportTask,RecordRemoval,_>(fleet_sync_delivery::Submitter::Machine(peer(1)),
                        item(1, "body-a"),
                        peer(9),
                        &TestConsumer,
                        SyncTime::from_unix_millis(18),
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    let aborted = fleet
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .receipt::<PublishTask>(
                        peer(2),
                        &reference(1, "body-a"),
                        answer,
                        SyncTime::from_unix_millis(20),
                    )
                    .await
                    .map_err(storage_error)?;
                Err::<(), _>(StoreError::backend("rollback receipt after status debt"))
            })
        })
        .await;
    assert!(aborted.is_err());
    assert_eq!(snapshot(&fleet.store).await, before_recipient);
    assert!(matches!(
        receipt(&fleet.store, 2, 1, "body-a").await,
        StatusPublicationResult::Applied
    ));
    fleet.store.transaction(|scope|Box::pin(async move {
        let row=query::<Turso>("SELECT payload,status_kind,(SELECT count(*) FROM _fleet_sync_status_owed) AS owed FROM _fleet_sync_outbox_items WHERE item_id=?1").bind(item_id(1).as_uuid().to_string()).fetch_one(scope.executor()).await?;
        assert!(row.try_get::<Option<String>,_>("payload")?.is_none());
        assert_eq!(row.try_get::<String,_>("status_kind")?,"atRecipient");
        assert_eq!(row.try_get::<i64,_>("owed")?,1);
        Ok(())
    })).await.unwrap();
}
