use super::support::{network::*, sender_binding::*, *};
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::Arc;
const SERVICE: &str = "11111111-1111-4111-8111-111111111111";
fn now() -> SyncTime {
    SyncTime::from_unix_millis(0)
}
#[tokio::test]
async fn compacted_custody_reopens_offline_using_persisted_host_truth_and_never_its_stored_claim() {
    let fixture = Fixture::new().await;
    let raw = serde_json::to_string(&format!("svc:{SERVICE}|session")).unwrap();
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                create_binding_directory(scope).await?;
                insert_binding(scope, 90, SERVICE, peer(9)).await?;
                let item = DeliveryItem::<TestTypes>::new(
                    item_id(1),
                    format!("svc:{SERVICE}|session"),
                    Recipient {
                        peer: peer(2),
                        address: "inbox".into(),
                    },
                    None,
                    "sealed body".into(),
                )
                .unwrap();
                let reference = DeliveryItemRef {
                    item: item.id(),
                    digest: item.digest().clone(),
                };
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
                outbox
                    .submit_relay::<PushTask, RecordRemoval>(
                        Submitter::Machine(peer(9)),
                        DeliveryEnvelope::Item(item),
                        peer(3),
                        "fixture-route".into(),
                        now(),
                    )
                    .await
                    .map_err(storage_error)?;
                outbox
                    .receipt::<PublishTask>(
                        peer(2),
                        &reference,
                        PushResult::Accepted { accepted_at: now() },
                        now(),
                    )
                    .await
                    .map_err(storage_error)?;
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
    drop(store); // The reopened host has no network routes or Fleet connection.
    let reopened = Arc::new(
        TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
            .await
            .unwrap(),
    );
    reopened.transaction(move|scope|Box::pin(async move {
        let row=query::<Turso>("SELECT payload,consumer_sender,submitter_id FROM _fleet_sync_outbox_items").fetch_one(scope.executor()).await?;
        assert!(row.try_get::<Option<String>,_>("payload")?.is_none());
        assert_eq!(row.try_get::<String,_>("consumer_sender")?,raw);
        let status=Outbox::<_,TestTypes,_>::new(scope,limits(),&FixtureBindings).status(item_id(1)).await.map_err(storage_error)?;
        assert!(matches!(status.state,OutboxStatusState::Known {status:DeliveryStatus::AtRecipient,..}));
        query::<Turso>("DELETE FROM test_sender_bindings").execute(scope.executor()).await?;
        assert!(matches!(Outbox::<_,TestTypes,_>::new(scope,limits(),&FixtureBindings).status(item_id(1)).await,Err(DeliveryError::SenderBindingUnavailable {row:CustodyRow::Outbox(item),reason:SenderBindingFailure::Unknown}) if item==item_id(1)));
        Ok(())
    })).await.unwrap();
}
