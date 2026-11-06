use super::support::{
    network::{DeliverTask, PublishTask, PushTask, ReportTask},
    *,
};
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;

fn owners() -> [Submitter; 2] {
    let raw = durable_steps::Uuid::from_u128(0x019eef00000070008000000000000001);
    [
        Submitter::Machine(PeerId::new(raw).unwrap()),
        Submitter::Project(DatabaseId::new(raw).unwrap()),
    ]
}
fn now() -> SyncTime {
    SyncTime::from_unix_millis(0)
}
fn envelope(owner: Submitter, identity: u64) -> DeliveryEnvelope<TestTypes> {
    DeliveryEnvelope::Item(
        DeliveryItem::new(
            item_id(identity),
            owned_sender(owner, "session/exact"),
            Recipient {
                peer: peer(2),
                address: "inbox".into(),
            },
            None,
            "body".into(),
        )
        .unwrap(),
    )
}
fn kind(owner: Submitter) -> &'static str {
    match owner {
        Submitter::Machine(_) => "machine",
        Submitter::Project(_) => "project",
        Submitter::Fleet(_) => "fleet",
    }
}
async fn counts(store: &TestStore) -> (i64, i64, i64, i64) {
    store.transaction(|scope| Box::pin(async move {
        let row=query::<Turso>("SELECT (SELECT count(*) FROM _fleet_sync_outbox_items) AS outbox,(SELECT count(*) FROM _fleet_sync_inbox_items) AS inbox,(SELECT count(*) FROM _fleet_sync_operations) AS operations,(SELECT count(*) FROM _fleet_sync_delivery_backfills) AS markers")
            .fetch_one(scope.executor()).await?;
        Ok((row.try_get("outbox")?,row.try_get("inbox")?,row.try_get("operations")?,row.try_get("markers")?))
    })).await.unwrap()
}
async fn assert_stored(store: &TestStore, table: &'static str, owner: Submitter, identity: u64) {
    store.transaction(move |scope|Box::pin(async move {
        let sql=format!("SELECT submitter_kind,submitter_id,consumer_sender FROM {table} WHERE item_id=?1");
        let row=query::<Turso>(sqlx_core::sql_str::AssertSqlSafe(sql)).bind(item_id(identity).as_uuid().to_string()).fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<String,_>("submitter_kind")?,kind(owner));
        assert_eq!(row.try_get::<String,_>("submitter_id")?,owner.as_uuid().to_string());
        let sender: String=serde_json::from_str(&row.try_get::<String,_>("consumer_sender")?).map_err(StoreError::backend)?;
        assert_eq!(sender,owned_sender(owner,"session/exact"));
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn forged_source_claims_are_refused_at_each_intake_without_writes() {
    let fixture = Fixture::new().await;
    let [machine, project] = owners();
    let before = counts(&fixture.store).await;
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(item) = envelope(project, 10) else {
                    return Err(StoreError::backend("fixture item"));
                };
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .enqueue(item, peer(3), machine, now())
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
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .submit_relay::<PushTask, RecordRemoval>(
                            machine,
                            envelope(project, 10),
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
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(counts(&fixture.store).await, before);
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                assert!(matches!(
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                        .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                            machine,
                            envelope(project, 10),
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
async fn both_owner_kinds_survive_three_custody_records_replay_and_payload_release() {
    for owner in owners() {
        let sender = Fixture::new().await;
        let relay = Fixture::new().await;
        let recipient = Fixture::new().await;
        sender
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let DeliveryEnvelope::Item(item) = envelope(owner, 11) else {
                        return Err(StoreError::backend("fixture item"));
                    };
                    assert!(matches!(
                        Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                            .enqueue(item, peer(3), owner, now())
                            .await
                            .map_err(storage_error)?,
                        OutboxAdmission::Queued { .. }
                    ));
                    Ok(())
                })
            })
            .await
            .unwrap();
        relay
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let mut outbox =
                        Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
                    assert_eq!(
                        outbox
                            .relay_submitter(item_id(999))
                            .await
                            .map_err(storage_error)?,
                        None
                    );
                    assert!(matches!(
                        outbox
                            .submit_relay::<PushTask, RecordRemoval>(
                                owner,
                                envelope(owner, 11),
                                peer(3),
                                "route".into(),
                                now()
                            )
                            .await
                            .map_err(storage_error)?,
                        SubmitResult::Accepted { .. }
                    ));
                    assert_eq!(
                        outbox
                            .relay_submitter(item_id(11))
                            .await
                            .map_err(storage_error)?,
                        Some(owner)
                    );
                    assert!(matches!(
                        outbox
                            .submit_relay::<PushTask, RecordRemoval>(
                                owner,
                                envelope(owner, 11),
                                peer(3),
                                "route".into(),
                                now()
                            )
                            .await
                            .map_err(storage_error)?,
                        SubmitResult::Replayed { .. }
                    ));
                    Ok(())
                })
            })
            .await
            .unwrap();
        let pushed = relay
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .resolve_push(
                            &DeliveryItemRef {
                                item: item_id(11),
                                digest: envelope(owner, 11).digest().clone(),
                            },
                            fleet_sync_protocol::SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)
                })
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pushed.submitter, owner);
        recipient
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    assert!(matches!(
                        Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                            .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                                pushed.submitter,
                                pushed.item,
                                peer(3),
                                &TestConsumer,
                                now()
                            )
                            .await
                            .map_err(storage_error)?,
                        PushResult::Accepted { .. }
                    ));
                    Ok(())
                })
            })
            .await
            .unwrap();
        for (store, table) in [
            (&sender.store, "_fleet_sync_outbox_items"),
            (&relay.store, "_fleet_sync_outbox_items"),
            (&recipient.store, "_fleet_sync_inbox_items"),
        ] {
            assert_stored(store, table, owner, 11).await;
        }
        relay
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let mut outbox =
                        Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
                    let reference = DeliveryItemRef {
                        item: item_id(11),
                        digest: envelope(owner, 11).digest().clone(),
                    };
                    outbox
                        .receipt::<PublishTask>(
                            peer(2),
                            &reference,
                            PushResult::Accepted { accepted_at: now() },
                            now(),
                        )
                        .await
                        .map_err(storage_error)?;
                    assert_eq!(
                        outbox
                            .relay_submitter(item_id(11))
                            .await
                            .map_err(storage_error)?,
                        Some(owner)
                    );
                    assert!(matches!(
                        outbox
                            .relay_query(owner, item_id(11))
                            .await
                            .map_err(storage_error)?
                            .state,
                        ItemStatusState::Known {
                            status: DeliveryStatus::AtRecipient,
                            ..
                        }
                    ));
                    let other = match owner {
                        Submitter::Machine(_) => owners()[1],
                        Submitter::Project(_) => owners()[0],
                        Submitter::Fleet(_) => owners()[0],
                    };
                    assert!(matches!(
                        outbox.relay_query(other, item_id(11)).await,
                        Err(DeliveryError::Refused {
                            reason: DeliveryRefusal::NotSubmitter
                        })
                    ));
                    assert!(matches!(
                        outbox
                            .relay_settle::<RecordRemoval>(
                                other,
                                item_id(11),
                                reference.digest.clone(),
                                now()
                            )
                            .await
                            .map_err(storage_error)?,
                        SettleResult::ItemConflict { .. }
                    ));
                    assert!(matches!(
                        outbox
                            .submit_relay::<PushTask, RecordRemoval>(
                                owner,
                                envelope(owner, 11),
                                peer(3),
                                "route".into(),
                                now()
                            )
                            .await
                            .map_err(storage_error)?,
                        SubmitResult::Replayed { .. }
                    ));
                    Ok(())
                })
            })
            .await
            .unwrap();
        assert_stored(&relay.store, "_fleet_sync_outbox_items", owner, 11).await;
    }
}

#[tokio::test]
async fn matching_uuid_bits_still_have_separate_cursors_acknowledgements_and_replays() {
    let fixture = Fixture::new().await;
    let [machine, project] = owners();
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let publication = StatusPublication::<Acceptance> {
                    item: item_id(20),
                    status: DeliveryStatus::Sent,
                    version: StatusVersion::new(1),
                    cancel: None,
                };
                for owner in [machine, project] {
                    let mut stream =
                        StatusStream::<_, TestTypes>::new(scope, owner, "same route".into());
                    let (event, reference) = stream
                        .append(&publication, now())
                        .await
                        .map_err(storage_error)?;
                    assert_eq!(event.cursor.value(), 1);
                    assert!(match (owner, reference.unwrap().target) {
                        (Submitter::Machine(expected), PublicationTarget::Peer { peer, .. }) =>
                            expected == peer,
                        (
                            Submitter::Project(expected),
                            PublicationTarget::Project { project, .. },
                        ) => expected == project,
                        _ => false,
                    });
                    let (replayed, reference) = stream
                        .append(&publication, now())
                        .await
                        .map_err(storage_error)?;
                    assert_eq!(replayed.cursor.value(), 1);
                    assert!(reference.is_none());
                }
                StatusStream::<_, TestTypes>::new(scope, machine, "same route".into())
                    .acknowledge(StatusCursor::new(1), now())
                    .await
                    .map_err(storage_error)?;
                let machine_events =
                    StatusStream::<_, TestTypes>::new(scope, machine, "same route".into())
                        .fetch(machine, StatusCursor::new(0), 10)
                        .await
                        .map_err(storage_error)?;
                let project_events =
                    StatusStream::<_, TestTypes>::new(scope, project, "same route".into())
                        .fetch(project, StatusCursor::new(0), 10)
                        .await
                        .map_err(storage_error)?;
                assert!(machine_events.events.is_empty());
                assert_eq!(project_events.events.len(), 1);
                assert_eq!(project_events.events.first().unwrap().cursor.value(), 1);
                let next = StatusPublication {
                    item: item_id(21),
                    ..publication
                };
                assert_eq!(
                    StatusStream::<_, TestTypes>::new(scope, machine, "same route".into())
                        .append(&next, now())
                        .await
                        .map_err(storage_error)?
                        .0
                        .cursor
                        .value(),
                    2
                );
                assert_eq!(
                    StatusStream::<_, TestTypes>::new(scope, project, "same route".into())
                        .append(&next, now())
                        .await
                        .map_err(storage_error)?
                        .0
                        .cursor
                        .value(),
                    2
                );
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn project_publication_waits_without_a_holder_then_resolves_the_current_machine() {
    let fixture = super::support::network::NetworkFixture::new().await;
    let owner = owners()[1];
    let Submitter::Project(project) = owner else {
        panic!("fixture owner must be a project")
    };
    fixture
        .sender
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(item) = envelope(owner, 30) else {
                    return Err(StoreError::backend("fixture item"));
                };
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
                outbox
                    .enqueue(item, peer(3), owner, now())
                    .await
                    .map_err(storage_error)?;
                outbox
                    .mark_submitted(
                        &SubmitInput {
                            item: item_id(30),
                            digest: envelope(owner, 30).digest().clone(),
                            relay: peer(3),
                            mailbox: peer(2),
                        },
                        SubmitResult::Accepted {
                            status: DeliveryStatus::Sent,
                            version: StatusVersion::new(1),
                            cancel: None,
                        },
                        now(),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    fixture
        .relay
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                StatusStream::<_, TestTypes>::new(scope, owner, "project route".into())
                    .append_and_admit::<PublishTask>(
                        &StatusPublication {
                            item: item_id(30),
                            status: DeliveryStatus::AtRecipient,
                            version: StatusVersion::new(2),
                            cancel: None,
                        },
                        now(),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let engine = fixture.engine(&fixture.relay);
    let first = engine.drive_due().await.unwrap();
    assert!(first.failed.is_empty());
    let owed = fixture
        .relay
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = query::<Turso>("SELECT count(*) AS count FROM _fleet_sync_status_owed")
                    .fetch_one(scope.executor())
                    .await?;
                Ok(row.try_get::<i64, _>("count")?)
            })
        })
        .await
        .unwrap();
    assert_eq!(owed, 1, "no-holder retry must retain the publication");
    assert!(
        fixture
            .routes
            .publications
            .project_deliveries
            .lock()
            .unwrap()
            .is_empty()
    );
    fixture
        .routes
        .publications
        .set_project_holder(project, peer(9), &fixture.sender.store);
    fixture.advance();
    let second = engine.drive_due().await.unwrap();
    assert!(second.failed.is_empty());
    assert_eq!(
        *fixture
            .routes
            .publications
            .project_deliveries
            .lock()
            .unwrap(),
        vec![(project, peer(9))]
    );
    fixture
        .sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let status = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .status(item_id(30))
                    .await
                    .map_err(storage_error)?;
                assert!(matches!(
                    status.state,
                    OutboxStatusState::Known {
                        status: DeliveryStatus::AtRecipient,
                        ..
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}

fn fleet_id() -> FleetId {
    FleetId::new(peer(1).as_uuid()).unwrap()
}

struct FleetBindings;
impl<S: TransactionScope<Database = Turso>> SenderBinding<S, TestTypes> for FleetBindings {
    async fn resolve_submitter<'a>(
        &'a self,
        _scope: &'a mut S,
        sender: &'a String,
    ) -> Result<Submitter, SenderBindingFailure> {
        match sender.as_str() {
            "fleet" => Ok(Submitter::Fleet(fleet_id())),
            "sender" => Ok(Submitter::Machine(peer(1))),
            _ => Err(SenderBindingFailure::Unknown),
        }
    }
}

fn fleet_envelope(identity: u64) -> DeliveryEnvelope<TestTypes> {
    DeliveryEnvelope::Item(
        DeliveryItem::new(
            item_id(identity),
            "fleet".into(),
            Recipient {
                peer: peer(2),
                address: "inbox".into(),
            },
            None,
            "Fleet command".into(),
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn fleet_submitter_survives_internal_admission_push_payload_release_and_reopen() {
    let fixture = Fixture::new().await;
    let recipient = Fixture::new().await;
    let pushed = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &FleetBindings);
                assert!(matches!(
                    outbox
                        .submit_fleet::<PushTask, RecordRemoval>(
                            fleet_id(),
                            fleet_envelope(40),
                            peer(3),
                            "fleet route".into(),
                            now()
                        )
                        .await
                        .map_err(storage_error)?,
                    SubmitResult::Accepted { .. }
                ));
                let reference = DeliveryItemRef {
                    item: item_id(40),
                    digest: fleet_envelope(40).digest().clone(),
                };
                let pushed = outbox
                    .resolve_push(&reference, now())
                    .await
                    .map_err(storage_error)?
                    .unwrap();
                assert_eq!(pushed.submitter, Submitter::Fleet(fleet_id()));
                assert!(matches!(
                    outbox
                        .submit_fleet::<PushTask, RecordRemoval>(
                            fleet_id(),
                            fleet_envelope(40),
                            peer(3),
                            "fleet route".into(),
                            now()
                        )
                        .await
                        .map_err(storage_error)?,
                    SubmitResult::Replayed { .. }
                ));
                Ok(pushed)
            })
        })
        .await
        .unwrap();
    recipient
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                assert!(matches!(
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FleetBindings)
                        .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                            pushed.submitter,
                            pushed.item,
                            peer(3),
                            &TestConsumer,
                            now()
                        )
                        .await
                        .map_err(storage_error)?,
                    PushResult::Accepted { .. }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let reference = DeliveryItemRef {
                    item: item_id(40),
                    digest: fleet_envelope(40).digest().clone(),
                };
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FleetBindings)
                        .receipt::<PublishTask>(
                            peer(2),
                            &reference,
                            PushResult::Accepted { accepted_at: now() },
                            now()
                        )
                        .await
                        .map_err(storage_error)?,
                    StatusPublicationResult::Applied
                ));
                Ok(())
            })
        })
        .await
        .unwrap();

    let Fixture {
        store,
        host,
        directory,
    } = fixture;
    drop(store);
    let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
        .await
        .unwrap();
    reopened.transaction(|scope| Box::pin(async move {
        assert_eq!(backfill_submitters::<TestTypes, _, _>(scope, DeliveryHostRole::Relay, &FleetBindings, now()).await.map_err(storage_error)?, 0);
        let row = query::<Turso>("SELECT payload,submitter_kind,submitter_id,consumer_sender FROM _fleet_sync_outbox_items WHERE item_id=?1")
            .bind(item_id(40).as_uuid().to_string()).fetch_one(scope.executor()).await?;
        assert!(row.try_get::<Option<String>, _>("payload")?.is_none());
        assert_eq!(row.try_get::<String, _>("submitter_kind")?, "fleet");
        assert_eq!(row.try_get::<String, _>("submitter_id")?, fleet_id().to_string());
        assert_eq!(row.try_get::<String, _>("consumer_sender")?, "\"fleet\"");
        let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &FleetBindings);
        assert_eq!(outbox.relay_submitter(item_id(40)).await.map_err(storage_error)?, Some(Submitter::Fleet(fleet_id())));
        assert!(matches!(outbox.relay_query(Submitter::Fleet(fleet_id()), item_id(40)).await.map_err(storage_error)?.state,
            ItemStatusState::Known { status: DeliveryStatus::AtRecipient, .. }));
        assert!(outbox.resolve_push(&DeliveryItemRef { item: item_id(40), digest: fleet_envelope(40).digest().clone() }, now()).await.map_err(storage_error)?.is_none());
        assert!(matches!(outbox.submit_fleet::<PushTask, RecordRemoval>(fleet_id(), fleet_envelope(40), peer(3), "fleet route".into(), now()).await.map_err(storage_error)?, SubmitResult::Replayed { .. }));
        let publication = resolve_publication::<_, TestTypes>(scope, PublicationRef {
            item: item_id(40), version: StatusVersion::new(2),
            target: PublicationTarget::StatusStream { sender: Submitter::Fleet(fleet_id()), route: "fleet route".into() },
        }).await.map_err(storage_error)?.unwrap();
        assert!(matches!(publication.status, DeliveryStatus::AtRecipient));
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn machine_submit_refuses_fleet_submitters_without_writes() {
    let fixture = Fixture::new().await;
    let before = counts(&fixture.store).await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &FleetBindings);
                assert!(matches!(
                    outbox
                        .submit_relay::<PushTask, RecordRemoval>(
                            Submitter::Fleet(fleet_id()),
                            fleet_envelope(41),
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
                let DeliveryEnvelope::Item(item) = fleet_envelope(41) else {
                    panic!("fixture item")
                };
                assert!(matches!(
                    outbox
                        .enqueue(item, peer(3), Submitter::Fleet(fleet_id()), now())
                        .await
                        .map_err(storage_error)?,
                    OutboxAdmission::Refused {
                        reason: DeliveryRefusal::NotSubmitter
                    }
                ));
                let cancel = DeliveryCancel::new(
                    item_id(42),
                    "fleet".into(),
                    Recipient {
                        peer: peer(2),
                        address: "inbox".into(),
                    },
                    None,
                    CancelBody {
                        target: item_id(41),
                    },
                )
                .unwrap();
                assert!(matches!(
                    outbox
                        .enqueue_cancel(
                            cancel,
                            peer(3),
                            Submitter::Fleet(fleet_id()),
                            now(),
                            &TestConsumer
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
async fn internal_fleet_admission_refuses_other_bound_owners_without_writes() {
    let fixture = Fixture::new().await;
    let before = counts(&fixture.store).await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &FleetBindings);
                for (fleet, envelope) in [
                    (fleet_id(), item(43, "machine source")),
                    (FleetId::new(peer(9).as_uuid()).unwrap(), fleet_envelope(43)),
                ] {
                    assert!(matches!(
                        outbox
                            .submit_fleet::<PushTask, RecordRemoval>(
                                fleet,
                                envelope,
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
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(counts(&fixture.store).await, before);
}
