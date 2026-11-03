#![allow(clippy::unwrap_used)]
use crate::custody_fixture as support;

use super::*;
use durable_steps::AtomicStore;
use support::*;

#[tokio::test]
async fn loader_rejects_a_stored_v4_primary_identity_with_v7_control() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(original) = item(80, "identity") else {
                    return Err(durable_steps::StoreError::backend("original fixture"));
                };
                Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                    .enqueue_local(original, &TestConsumer, SyncTime::from_unix_millis(11))
                    .await
                    .map_err(storage_error)?;
                let valid = queries::read_inbox_item(scope, item_key(item_id(80)))
                    .await?
                    .unwrap();
                assert!(
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                        .decode_item(valid, None)
                        .await
                        .is_ok()
                );
                let invalid = "01900000-0000-4000-8000-000000000050";
                sqlx_core::query::query::<sqlx_turso::Turso>(
                    "UPDATE _fleet_sync_inbox_items SET item_id=?1",
                )
                .bind(invalid.to_owned())
                .execute(scope.executor())
                .await?;
                let row = queries::read_inbox_item(scope, invalid.to_owned())
                    .await?
                    .unwrap();
                assert!(matches!(
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                        .decode_item(row, None)
                        .await,
                    Err(DeliveryError::Corrupt { .. })
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}
