use super::support::{legacy_submitters::*, sender_binding::*, *};
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;

const SERVICE_A: &str = "11111111-1111-4111-8111-111111111111";
const SERVICE_B: &str = "22222222-2222-4222-8222-222222222222";
fn source(identity: u64, _: Submitter) -> String {
    format!(
        "svc:{}|legacy/session",
        if identity % 2 == 1 {
            SERVICE_A
        } else {
            SERVICE_B
        }
    )
}
async fn fixture() -> Fixture {
    let fixture = legacy_store_with_sender(0, source).await;
    fixture
        .store
        .transaction(|scope| Box::pin(async move { create_binding_directory(scope).await }))
        .await
        .unwrap();
    fixture
}
async fn payloads(store: &TestStore) -> Vec<String> {
    store.transaction(|scope| Box::pin(async move {
        query::<Turso>("SELECT json_array(item_id,consumer_sender,digest,payload) AS row FROM _fleet_sync_outbox_items UNION ALL SELECT json_array(item_id,consumer_sender,digest,payload) FROM _fleet_sync_inbox_items ORDER BY row")
        .fetch_all(scope.executor()).await?.iter().map(|row|row.try_get("row").map_err(StoreError::backend)).collect()
    })).await.unwrap()
}
#[tokio::test]
async fn released_live_service_sources_backfill_from_host_mapping_without_changing_sealed_bytes() {
    let fixture = fixture().await;
    let before = payloads(&fixture.store).await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                insert_binding(scope, 90, SERVICE_A, peer(9)).await?;
                insert_binding(scope, 91, SERVICE_B, peer(8)).await?;
                assert_eq!(
                    backfill_submitters::<TestTypes, _, _>(scope, &FixtureBindings, startup_time())
                        .await
                        .map_err(storage_error)?,
                    4
                );
                for (identity, expected, inbox) in [
                    (1, peer(9), false),
                    (2, peer(8), false),
                    (3, peer(9), true),
                    (4, peer(8), true),
                ] {
                    let sql = if inbox {
                        "SELECT submitter_id FROM _fleet_sync_inbox_items WHERE item_id=?1"
                    } else {
                        "SELECT submitter_id FROM _fleet_sync_outbox_items WHERE item_id=?1"
                    };
                    let owner: String = query::<Turso>(sqlx_core::sql_str::AssertSqlSafe(sql))
                        .bind(item_id(identity).as_uuid().to_string())
                        .fetch_one(scope.executor())
                        .await?
                        .try_get("submitter_id")?;
                    assert_eq!(owner, expected.as_uuid().to_string());
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(payloads(&fixture.store).await, before);
}
#[tokio::test]
async fn missing_backfill_binding_names_the_row_and_commits_neither_prior_bindings_nor_marker() {
    unavailable_backfill(false).await;
}
#[tokio::test]
async fn conflicting_backfill_binding_names_the_row_and_commits_neither_prior_bindings_nor_marker()
{
    unavailable_backfill(true).await;
}
async fn unavailable_backfill(conflict: bool) {
    let fixture = fixture().await;
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                insert_binding(scope, 90, SERVICE_A, peer(9)).await?;
                if conflict {
                    insert_binding(scope, 91, SERVICE_B, peer(8)).await?;
                    insert_binding(scope, 92, SERVICE_B, peer(7)).await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    let before = snapshot(&fixture.store).await;
    let result = fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                Ok(
                    backfill_submitters::<TestTypes, _, _>(scope, &FixtureBindings, startup_time())
                        .await,
                )
            })
        })
        .await
        .unwrap();
    let expected = if conflict {
        SenderBindingFailure::Conflict
    } else {
        SenderBindingFailure::Unknown
    };
    assert!(
        matches!(result,Err(DeliveryError::SenderBindingUnavailable {row:CustodyRow::Inbox(item), reason}) if item==item_id(4) && reason==expected)
    );
    assert_eq!(snapshot(&fixture.store).await, before);
}
