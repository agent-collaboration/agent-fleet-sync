#![allow(clippy::unwrap_used)]
use super::support::*;
use durable_steps::{AtomicStore, SyncTime, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;

#[tokio::test]
async fn oversized_route_is_refused_before_admission_and_cannot_replace_a_live_target() {
    let fixture = Fixture::new().await;
    fixture.store.transaction(|scope|Box::pin(async move {
        assert!(matches!(Outbox::<_,TestTypes,_>::new(scope,limits(),&FixtureBindings)
            .submit_relay::<FixturePush<TestTypes>,RecordRemoval>(Submitter::Machine(peer(1)),item(1,"body"),peer(3),"x".repeat(63),SyncTime::from_unix_millis(0)).await,
            Err(DeliveryError::Refused {reason:DeliveryRefusal::TooLarge {field:DeliverySizeField::StatusRoute}})));
        let row=query::<Turso>("SELECT count(*) AS count FROM _fleet_sync_outbox_items").fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<i64,_>("count")?,0);
        assert!(matches!(Outbox::<_,TestTypes,_>::new(scope,limits(),&FixtureBindings)
            .submit_relay::<FixturePush<TestTypes>,RecordRemoval>(Submitter::Machine(peer(1)),item(1,"body"),peer(3),"valid".into(),SyncTime::from_unix_millis(0)).await.map_err(storage_error)?,SubmitResult::Accepted {..}));
        let before=query::<Turso>("SELECT json_array(item_id,status_route,status_version,payload,settlement_bytes,created_at,updated_at) AS state FROM _fleet_sync_outbox_items").fetch_one(scope.executor()).await?.try_get::<String,_>("state")?;
        assert!(matches!(Outbox::<_,TestTypes,_>::new(scope,limits(),&FixtureBindings)
            .submit_relay::<FixturePush<TestTypes>,RecordRemoval>(Submitter::Machine(peer(1)),item(1,"body"),peer(3),"x".repeat(63),SyncTime::from_unix_millis(0)).await,
            Err(DeliveryError::Refused {reason:DeliveryRefusal::TooLarge {field:DeliverySizeField::StatusRoute}})));
        let after=query::<Turso>("SELECT json_array(item_id,status_route,status_version,payload,settlement_bytes,created_at,updated_at) AS state FROM _fleet_sync_outbox_items").fetch_one(scope.executor()).await?.try_get::<String,_>("state")?;
        assert_eq!(after,before);
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn authoritative_route_bounds_reject_corrupt_relay_publication_and_stream_state() {
    for owner in ["relay", "publication", "stream"] {
        let fixture = Fixture::new().await;
        fixture
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .submit_relay::<FixturePush<TestTypes>, RecordRemoval>(
                            Submitter::Machine(peer(1)),
                            item(1, "body"),
                            peer(3),
                            "valid".into(),
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)?;
                    StatusStream::<_, TestTypes>::new(
                        scope,
                        Submitter::Machine(peer(1)),
                        "valid".into(),
                    )
                    .append(
                        &StatusPublication {
                            item: item_id(1),
                            status: DeliveryStatus::Sent,
                            version: StatusVersion::new(1),
                            cancel: None,
                        },
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                    let statement = match owner {
                        "relay" => {
                            "UPDATE _fleet_sync_outbox_items SET status_route=json_quote(?1)"
                        }
                        "publication" => {
                            "UPDATE _fleet_sync_status_owed SET status_route=json_quote(?1)"
                        }
                        _ => "UPDATE _fleet_sync_status_state SET owner_route=json_quote(?1)",
                    };
                    query::<Turso>(statement)
                        .bind("x".repeat(63))
                        .execute(scope.executor())
                        .await?;
                    let result = match owner {
                        "relay" => {
                            Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                                .relay_query(Submitter::Machine(peer(1)), item_id(1))
                                .await
                                .map(|_| ())
                        }
                        "publication" => {
                            admit_publications::<_, TestTypes, FixturePublication<TestTypes>>(scope)
                                .await
                                .map(|_| ())
                        }
                        _ => StatusStream::<_, TestTypes>::new(
                            scope,
                            Submitter::Machine(peer(1)),
                            "valid".into(),
                        )
                        .fetch(Submitter::Machine(peer(1)), StatusCursor::new(0), 10)
                        .await
                        .map(|_| ()),
                    };
                    assert!(
                        matches!(result, Err(DeliveryError::Corrupt { .. })),
                        "{owner} accepted a corrupt route"
                    );
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
}
