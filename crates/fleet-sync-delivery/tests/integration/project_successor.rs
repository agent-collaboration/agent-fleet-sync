use super::support::{network::*, *};
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;

fn project() -> DatabaseId {
    DatabaseId::new(peer(1).as_uuid()).unwrap()
}
fn now() -> SyncTime {
    SyncTime::from_unix_millis(0)
}
fn envelope() -> DeliveryEnvelope<TestTypes> {
    DeliveryEnvelope::Item(
        DeliveryItem::new(
            item_id(1),
            owned_sender(Submitter::Project(project()), "project source"),
            Recipient {
                peer: peer(2),
                address: "inbox".into(),
            },
            None,
            "successor body".into(),
        )
        .unwrap(),
    )
}
fn reference() -> DeliveryItemRef {
    DeliveryItemRef {
        item: item_id(1),
        digest: envelope().digest().clone(),
    }
}
async fn queue(store: &TestStore) {
    store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(item) = envelope() else {
                    return Err(StoreError::backend("item fixture"));
                };
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .enqueue(item, peer(3), Submitter::Project(project()), now())
                        .await
                        .map_err(storage_error)?,
                    OutboxAdmission::Queued { .. }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}
async fn state(store: &TestStore) -> OutboxStatusState<Acceptance> {
    store
        .transaction(|scope| {
            Box::pin(async move {
                Ok(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .status(item_id(1))
                        .await
                        .map_err(storage_error)?
                        .state,
                )
            })
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn successor_resend_delivers_through_push_report_and_current_project_holder_publication() {
    let network = NetworkFixture::new().await;
    let successor = Fixture::new().await;
    queue(&network.sender.store).await;
    queue(&successor.store).await;
    network.routes.publications.register_project_stream(
        project(),
        "fixture-route",
        &network.status.store,
    );
    network
        .routes
        .publications
        .set_project_holder(project(), peer(1), &network.sender.store);
    // The same sealed item is retried after ownership moves. Its source and id
    // remain unchanged; the transport holder is an independent real PeerId.
    network
        .routes
        .publications
        .set_project_holder(project(), peer(9), &successor.store);
    let answer = network
        .relay
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .submit_relay::<PushTask, RecordRemoval>(
                        Submitter::Project(project()),
                        envelope(),
                        peer(3),
                        "fixture-route".into(),
                        now(),
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
        .unwrap();
    assert!(matches!(answer, SubmitResult::Accepted { .. }));
    successor
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
                outbox
                    .mark_submit_attempted(item_id(1), now())
                    .await
                    .map_err(storage_error)?;
                outbox
                    .mark_submitted(
                        &SubmitInput {
                            item: item_id(1),
                            digest: reference().digest,
                            relay: peer(3),
                            mailbox: peer(2),
                        },
                        answer,
                        now(),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    drain(&network.engine(&network.relay)).await;
    drain(&network.engine(&network.recipient)).await;
    drain(&network.engine(&network.relay)).await;
    drain(&network.engine(&network.status)).await;
    assert!(matches!(
        state(&successor.store).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Delivered { .. },
            ..
        }
    ));
    assert!(
        matches!(state(&network.sender.store).await,OutboxStatusState::Known {status:DeliveryStatus::Queued,version,..} if version.value()==0)
    );
    let publications = network
        .routes
        .publications
        .project_deliveries
        .lock()
        .unwrap();
    assert!(!publications.is_empty());
    assert!(
        publications
            .iter()
            .all(|target| *target == (project(), peer(9)))
    );
    assert_eq!(
        network
            .routes
            .local_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

async fn snapshot(store: &TestStore) -> Vec<String> {
    store.transaction(|scope|Box::pin(async move {
        query::<Turso>("SELECT json_array('custody',item_id,digest,submitter_kind,submitter_id,consumer_sender,payload,status_kind,status_version,last_report_seq,handed_off_at,updated_at) AS value FROM _fleet_sync_outbox_items UNION ALL SELECT json_array('owed',id,item_id,submitter_kind,submitter_id,target_kind,version,status_kind,updated_at) FROM _fleet_sync_status_owed UNION ALL SELECT json_array('journal',operation_id,kind,request_key,state,next_attempt_at,attempt,updated_at) FROM _fleet_sync_operations ORDER BY value").fetch_all(scope.executor()).await?.iter().map(|row|row.try_get("value").map_err(StoreError::backend)).collect()
    })).await.unwrap()
}

#[tokio::test]
async fn compacted_source_owner_mismatch_refuses_status_reads_without_writes() {
    source_mismatch(false).await;
}
#[tokio::test]
async fn compacted_source_owner_mismatch_refuses_recipient_reports_without_writes() {
    source_mismatch(true).await;
}
async fn source_mismatch(report: bool) {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
                outbox
                    .submit_relay::<PushTask, RecordRemoval>(
                        Submitter::Project(project()),
                        envelope(),
                        peer(3),
                        "fixture-route".into(),
                        now(),
                    )
                    .await
                    .map_err(storage_error)?;
                outbox
                    .receipt::<PublishTask>(
                        peer(2),
                        &reference(),
                        PushResult::Accepted { accepted_at: now() },
                        now(),
                    )
                    .await
                    .map_err(storage_error)?;
                query::<Turso>(
                    "UPDATE _fleet_sync_outbox_items SET consumer_sender=?1 WHERE item_id=?2",
                )
                .bind(serde_json::to_string("sender").unwrap())
                .bind(item_id(1).as_uuid().to_string())
                .execute(scope.executor())
                .await?;
                let row =
                    query::<Turso>("SELECT payload FROM _fleet_sync_outbox_items WHERE item_id=?1")
                        .bind(item_id(1).as_uuid().to_string())
                        .fetch_one(scope.executor())
                        .await?;
                assert!(row.try_get::<Option<String>, _>("payload")?.is_none());
                Ok(())
            })
        })
        .await
        .unwrap();
    let before = snapshot(&fixture.store).await;
    fixture.store.transaction(move |scope|Box::pin(async move {
            let mut outbox=Outbox::<_,TestTypes, _>::new(scope,limits(), &FixtureBindings);
            let error=if report {
                outbox.recipient_report::<PublishTask>(peer(2),RecipientReport::<TestTypes> {item:item_id(1),report_seq:ReportSeq::new(1),outcome:RecipientOutcome::Delivered {acceptance:Acceptance {receipt:"actual B receipt".into()}}},now()).await.err()
            } else {outbox.status(item_id(1)).await.err()};
            assert!(matches!(error,Some(DeliveryError::CorruptRow {table:"_fleet_sync_outbox_items",key,field:"submitter"}) if key==item_id(1).as_uuid().to_string()));
            Ok(())
        })).await.unwrap();
    assert_eq!(snapshot(&fixture.store).await, before);
}
