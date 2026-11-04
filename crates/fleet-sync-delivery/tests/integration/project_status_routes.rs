use super::support::{network::*, *};
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::Arc;

fn project_owner() -> Submitter {
    Submitter::Project(DatabaseId::new(peer(1).as_uuid()).unwrap())
}

fn project_envelope(identity: u64) -> DeliveryEnvelope<TestTypes> {
    let sender = owned_sender(project_owner(), "project agent");
    let recipient = Recipient {
        peer: peer(2),
        address: "inbox".into(),
    };
    if identity == 1 {
        DeliveryEnvelope::Item(
            DeliveryItem::new(item_id(1), sender, recipient, None, "inherited body".into())
                .unwrap(),
        )
    } else {
        DeliveryEnvelope::Cancel(
            DeliveryCancel::new(
                item_id(2),
                sender,
                recipient,
                None,
                CancelBody { target: item_id(1) },
            )
            .unwrap(),
        )
    }
}

async fn admit_project(store: &TestStore, identity: u64, route: &'static str) {
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let answer = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .submit_relay::<PushTask, RecordRemoval>(
                        project_owner(),
                        project_envelope(identity),
                        peer(9),
                        route.into(),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                assert!(matches!(
                    answer,
                    SubmitResult::Accepted {
                        status: DeliveryStatus::Sent,
                        ..
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}

async fn report_project(store: &TestStore, identity: u64) -> Result<ReportResult, StoreError> {
    store
        .transaction(move |scope| {
            Box::pin(async move {
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .recipient_report::<PublishTask>(
                        peer(2),
                        RecipientReport {
                            item: item_id(identity),
                            report_seq: ReportSeq::new(1),
                            outcome: if identity == 1 {
                                RecipientOutcome::Held
                            } else {
                                RecipientOutcome::Prevented { cancel: item_id(2) }
                            },
                        },
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)
            })
        })
        .await
}

async fn frozen_first_publication(store: &TestStore) -> (Vec<String>, Vec<String>) {
    store.transaction(|scope| Box::pin(async move {
        let rows = query::<Turso>("SELECT json_array(id,submitter_kind,submitter_id,item_id,target_kind,status_route,version,status_kind,status_acceptance,created_at,updated_at) AS frozen FROM _fleet_sync_status_owed WHERE item_id=?1")
            .bind(item_id(1).as_uuid().to_string()).fetch_all(scope.executor()).await?;
        let owed = rows.iter().map(|row| row.try_get("frozen")).collect::<Result<Vec<String>, _>>()?;
        let rows = query::<Turso>("SELECT json_array(operation_id,kind,kind_version,request_key,hex(input),hex(input_digest)) AS frozen FROM _fleet_sync_operations WHERE kind='fleet-sync.delivery.publish-status' AND json_extract(input,'$.item')=?1")
            .bind(item_id(1).as_uuid().to_string()).fetch_all(scope.executor()).await?;
        let journal = rows.iter().map(|row| row.try_get("frozen")).collect::<Result<Vec<String>, _>>()?;
        assert_eq!(owed.len(), 1);
        assert_eq!(journal.len(), 1);
        Ok((owed, journal))
    })).await.unwrap()
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

#[tokio::test]
async fn project_control_on_successor_route_reports_into_one_durable_stream_without_rekeying() {
    let fixture = Fixture::new().await;
    admit_project(&fixture.store, 1, "installation-a").await;
    assert!(
        matches!(report_project(&fixture.store, 1).await.unwrap(), ReportResult::Ack { report_seq } if report_seq.value() == 1)
    );
    let frozen = frozen_first_publication(&fixture.store).await;

    admit_project(&fixture.store, 2, "installation-b").await;
    // The successor's self-Prevented control Report uses its own admitted route.
    let answer = report_project(&fixture.store, 2)
        .await
        .expect("a Project's second installation route must not corrupt its status stream");
    assert!(matches!(answer, ReportResult::Ack { report_seq } if report_seq.value() == 1));
    assert_eq!(frozen_first_publication(&fixture.store).await, frozen);

    fixture.store.transaction(|scope| Box::pin(async move {
        let page = StatusStream::<_, TestTypes>::new(scope, project_owner(), "installation-b".into())
            .fetch(project_owner(), StatusCursor::new(0), 10).await.map_err(storage_error)?;
        assert_eq!(page.events.iter().map(|event| (event.cursor.value(), event.item, event.version.value())).collect::<Vec<_>>(), vec![(1, item_id(1), 2), (2, item_id(2), 2)]);
        assert!(matches!(page.events[0].status, DeliveryStatus::Held));
        assert!(matches!(page.events[1].status, DeliveryStatus::Cancelled));
        assert_eq!(page.events[1].cancel, Some(CancelResult::Prevented));
        let row = query::<Turso>("SELECT count(*) AS streams,min(owner_route) AS route FROM _fleet_sync_status_state").fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<i64, _>("streams")?, 1);
        assert_eq!(row.try_get::<String, _>("route")?, "\"installation-a\"");
        Ok(())
    })).await.unwrap();

    let fixture = reopen(fixture).await;
    for identity in [1, 2] {
        assert!(
            matches!(report_project(&fixture.store, identity).await.unwrap(), ReportResult::Replayed { report_seq } if report_seq.value() == 1)
        );
    }
    assert_eq!(frozen_first_publication(&fixture.store).await, frozen);
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let answer = StatusStream::<_, TestTypes>::new(
                    scope,
                    project_owner(),
                    "installation-b".into(),
                )
                .acknowledge(StatusCursor::new(1), SyncTime::from_unix_millis(0))
                .await
                .map_err(storage_error)?;
                assert_eq!(
                    answer,
                    StatusAckResult::Acknowledged {
                        floor: StatusCursor::new(1)
                    }
                );
                Ok(())
            })
        })
        .await
        .unwrap();

    let fixture = reopen(fixture).await;
    assert_eq!(frozen_first_publication(&fixture.store).await, frozen);
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let page = StatusStream::<_, TestTypes>::new(
                    scope,
                    project_owner(),
                    "installation-c".into(),
                )
                .fetch(project_owner(), StatusCursor::new(0), 10)
                .await
                .map_err(storage_error)?;
                assert_eq!(page.events.len(), 1);
                assert_eq!(page.events[0].cursor.value(), 2);
                assert_eq!(page.events[0].item, item_id(2));
                for identity in [1, 2] {
                    let project = DatabaseId::new(peer(1).as_uuid()).unwrap();
                    let reference = PublicationRef {
                        item: item_id(identity),
                        version: StatusVersion::new(2),
                        target: PublicationTarget::Project {
                            project,
                            route: "installation-a".to_owned(),
                        },
                    };
                    let publication = resolve_publication::<_, TestTypes>(scope, reference.clone())
                        .await
                        .map_err(storage_error)?
                        .unwrap();
                    assert_eq!(publication.item, item_id(identity));
                    assert_eq!(publication.version.value(), 2);
                    if identity == 1 {
                        assert!(matches!(publication.status, DeliveryStatus::Held));
                    } else {
                        assert!(matches!(publication.status, DeliveryStatus::Cancelled));
                        assert_eq!(publication.cancel, Some(CancelResult::Prevented));
                    }
                    retire_publication::<_, TestTypes>(scope, reference.clone())
                        .await
                        .map_err(storage_error)?;
                    assert!(
                        resolve_publication::<_, TestTypes>(scope, reference)
                            .await
                            .map_err(storage_error)?
                            .is_none()
                    );
                }
                Ok(())
            })
        })
        .await
        .unwrap();
}

fn held_publication() -> StatusPublication<Acceptance> {
    StatusPublication {
        item: item_id(8),
        status: DeliveryStatus::Held,
        version: StatusVersion::new(1),
        cancel: None,
    }
}

#[tokio::test]
async fn machine_route_stays_pinned_and_same_uuid_project_remains_separate() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let machine = Submitter::Machine(peer(1));
                StatusStream::<_, TestTypes>::new(scope, machine, "installation-a".into())
                    .append(&held_publication(), SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?;
                let wrong_route =
                    StatusStream::<_, TestTypes>::new(scope, machine, "installation-b".into())
                        .fetch(machine, StatusCursor::new(0), 10)
                        .await;
                assert!(matches!(
                    wrong_route,
                    Err(DeliveryError::Corrupt {
                        field: "status stream owner or cursor"
                    })
                ));
                let empty_project = StatusStream::<_, TestTypes>::new(
                    scope,
                    project_owner(),
                    "installation-b".into(),
                )
                .fetch(project_owner(), StatusCursor::new(0), 10)
                .await
                .map_err(storage_error)?;
                assert!(empty_project.events.is_empty());
                let (project_event, _) = StatusStream::<_, TestTypes>::new(
                    scope,
                    project_owner(),
                    "installation-b".into(),
                )
                .append(&held_publication(), SyncTime::from_unix_millis(0))
                .await
                .map_err(storage_error)?;
                assert_eq!(project_event.cursor.value(), 1);
                let machine_page =
                    StatusStream::<_, TestTypes>::new(scope, machine, "installation-a".into())
                        .fetch(machine, StatusCursor::new(0), 10)
                        .await
                        .map_err(storage_error)?;
                assert_eq!(machine_page.events.len(), 1);
                let row =
                    query::<Turso>("SELECT count(*) AS streams FROM _fleet_sync_status_state")
                        .fetch_one(scope.executor())
                        .await?;
                assert_eq!(row.try_get::<i64, _>("streams")?, 2);
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn project_route_flexibility_still_rejects_malformed_stored_route_and_cursor() {
    for statement in [
        "UPDATE _fleet_sync_status_state SET owner_route='{'",
        "UPDATE _fleet_sync_status_state SET owner_route=json_quote(printf('%063d',0))",
        "UPDATE _fleet_sync_status_state SET floor=head+1",
    ] {
        let fixture = Fixture::new().await;
        fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    StatusStream::<_, TestTypes>::new(
                        scope,
                        project_owner(),
                        "installation-a".into(),
                    )
                    .append(&held_publication(), SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?;
                    query::<Turso>(statement).execute(scope.executor()).await?;
                    let result = StatusStream::<_, TestTypes>::new(
                        scope,
                        project_owner(),
                        "installation-b".into(),
                    )
                    .fetch(project_owner(), StatusCursor::new(0), 10)
                    .await;
                    assert!(
                        matches!(result, Err(DeliveryError::Corrupt { .. })),
                        "malformed Project state accepted: {statement}"
                    );
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
}
