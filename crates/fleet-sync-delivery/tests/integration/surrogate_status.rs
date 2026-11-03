use super::support::*;
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;

async fn snapshots(store: &TestStore) -> Vec<String> {
    store.transaction(|scope|Box::pin(async move {
        query::<Turso>("SELECT 'state:' || json_array(id,machine_id,owner_route,head,floor,created_at,updated_at) AS value FROM _fleet_sync_status_state UNION ALL SELECT 'event:' || json_array(id,machine_id,cursor,item_id,version,status_kind,status_acceptance,created_at,updated_at) FROM _fleet_sync_status_events UNION ALL SELECT 'owed:' || json_array(id,item_id,target_kind,machine_id,status_route,version,status_kind,status_acceptance,created_at,updated_at) FROM _fleet_sync_status_owed UNION ALL SELECT 'cursor:' || json_array(id,singleton,cursor,created_at,updated_at) FROM _fleet_sync_outbox_status ORDER BY value").fetch_all(scope.executor()).await?.into_iter().map(|row|row.try_get("value").map_err(StoreError::backend)).collect()
    })).await.unwrap()
}
async fn corrupt_case(kind: &'static str) {
    let fixture = Fixture::new().await;
    let reference = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(original) = item(1, "content") else {
                    return Err(StoreError::backend("item"));
                };
                Outbox::<_, TestTypes>::new(scope, limits())
                    .enqueue(original, peer(9), peer(1), SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?;
                let publication = StatusPublication {
                    item: item_id(1),
                    version: StatusVersion::new(1),
                    status: DeliveryStatus::AtRecipient,
                    cancel: None,
                };
                let (_, reference) =
                    StatusStream::<_, TestTypes>::new(scope, peer(1), "route".into())
                        .append(&publication)
                        .await
                        .map_err(storage_error)?;
                let batch = StatusStream::<_, TestTypes>::new(scope, peer(1), "route".into())
                    .fetch(peer(1), StatusCursor::new(0), 10)
                    .await
                    .map_err(storage_error)?;
                Outbox::<_, TestTypes>::new(scope, limits())
                    .apply_status_batch(StatusCursor::new(0), batch, SyncTime::from_unix_millis(1))
                    .await
                    .map_err(storage_error)?;
                Ok(reference.unwrap())
            })
        })
        .await
        .unwrap();
    fixture.store.transaction(move|scope|Box::pin(async move {
        let statement=match kind {
            "state"=>"UPDATE _fleet_sync_status_state SET id='019eef00-0000-4000-8000-000000000099'",
            "event"=>"UPDATE _fleet_sync_status_events SET id='garbage'",
            "owed"=>"UPDATE _fleet_sync_status_owed SET id='019eef00-0000-4000-8000-000000000099'",
            _=>"UPDATE _fleet_sync_outbox_status SET id='garbage'",
        };
        query::<Turso>(statement).execute(scope.executor()).await?;Ok(())
    })).await.unwrap();
    let before = snapshots(&fixture.store).await;
    let refused = fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Ok(match kind {
                    "state" | "event" => {
                        StatusStream::<_, TestTypes>::new(scope, peer(1), "route".into())
                            .fetch(peer(1), StatusCursor::new(0), 10)
                            .await
                            .is_err()
                    }
                    "owed" => resolve_publication::<_, TestTypes>(scope, reference)
                        .await
                        .is_err(),
                    _ => Outbox::<_, TestTypes>::new(scope, limits())
                        .status_cursor()
                        .await
                        .is_err(),
                })
            })
        })
        .await
        .unwrap();
    assert!(refused, "{kind} accepted an invalid surrogate id");
    assert_eq!(snapshots(&fixture.store).await, before);
}
#[tokio::test]
async fn status_state_surrogate_id_is_refused_without_repair() {
    corrupt_case("state").await;
}
#[tokio::test]
async fn status_event_surrogate_id_is_refused_without_repair() {
    corrupt_case("event").await;
}
#[tokio::test]
async fn status_debt_surrogate_id_is_refused_without_repair() {
    corrupt_case("owed").await;
}
#[tokio::test]
async fn outbox_cursor_surrogate_id_is_refused_without_repair() {
    corrupt_case("cursor").await;
}

#[tokio::test]
async fn corrupt_surrogate_read_rolls_back_business_write_and_keeps_custody_corrupt_classification()
{
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let publication = StatusPublication {
                    item: item_id(1),
                    version: StatusVersion::new(1),
                    status: DeliveryStatus::AtRecipient,
                    cancel: None,
                };
                StatusStream::<_, TestTypes>::new(scope, peer(1), "route".into())
                    .append(&publication)
                    .await
                    .map_err(storage_error)?;
                query::<Turso>("CREATE TABLE before_corrupt_read(value INTEGER NOT NULL)")
                    .execute(scope.executor())
                    .await?;
                query::<Turso>("UPDATE _fleet_sync_status_state SET id='garbage'")
                    .execute(scope.executor())
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let result = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                query::<Turso>("INSERT INTO before_corrupt_read VALUES(1)")
                    .execute(scope.executor())
                    .await?;
                StatusStream::<_, TestTypes>::new(scope, peer(1), "route".into())
                    .fetch(peer(1), StatusCursor::new(0), 10)
                    .await
                    .map_err(delivery_rollback)?;
                Ok(())
            })
        })
        .await;
    assert!(
        matches!(result.map_err(classify_delivery_store_error),Err(DeliveryTransactionFailure::Corrupt {field}) if field.contains("_fleet_sync_status_state"))
    );
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert_eq!(
                    query::<Turso>("SELECT count(*) AS count FROM before_corrupt_read")
                        .fetch_one(scope.executor())
                        .await?
                        .try_get::<i64, _>("count")?,
                    0
                );
                Ok(())
            })
        })
        .await
        .unwrap();
}
