#![allow(clippy::unwrap_used)]
use super::support;
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use support::*;

#[tokio::test]
async fn admission_rejects_corrupt_accounting_in_other_custody_rows() {
    for hop in ["outbox", "relay", "inbox"] {
        for corrupted in ["not-a-number", "-1"] {
            let fixture = Fixture::new().await;
            fixture
                .store
                .transaction(move |scope| {
                    Box::pin(async move {
                        admit(scope, hop, 1).await.unwrap();
                        let statement = match hop {
                            "outbox" => "UPDATE _fleet_sync_outbox_items SET payload_bytes=?1",
                            "relay" => "UPDATE _fleet_sync_outbox_items SET payload_bytes=?1",
                            _ => "UPDATE _fleet_sync_inbox_items SET payload_bytes=?1",
                        };
                        sqlx_core::query::query::<sqlx_turso::Turso>(statement)
                            .bind(corrupted)
                            .execute(scope.executor())
                            .await
                            .map_err(durable_steps::StoreError::backend)?;
                        let answer = admit(scope, hop, 2).await;
                        assert!(
                            matches!(answer, Err(DeliveryError::Corrupt { .. })),
                            "{hop} accepted corrupt byte accounting {corrupted}: {answer:?}"
                        );
                        Ok(())
                    })
                })
                .await
                .unwrap();
        }
    }
}
async fn admit(
    scope: &mut durable_steps::turso_store::TursoScope<'_, TestHost>,
    hop: &str,
    id: u64,
) -> Result<(), DeliveryError> {
    let DeliveryEnvelope::Item(original) = item(id, "body") else {
        return Err(DeliveryError::InvalidBatch);
    };
    match hop {
        "outbox" => {
            Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                .enqueue(
                    original,
                    peer(3),
                    fleet_sync_delivery::Submitter::Machine(peer(1)),
                    SyncTime::from_unix_millis(0),
                )
                .await?;
        }
        "relay" => {
            Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                .submit_relay::<super::support::network::PushTask, RecordRemoval>(
                    Submitter::Machine(peer(1)),
                    DeliveryEnvelope::Item(original),
                    peer(3),
                    "fixture-route".to_owned(),
                    SyncTime::from_unix_millis(0),
                )
                .await?;
        }
        _ => {
            Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                .enqueue_local(original, &TestConsumer, SyncTime::from_unix_millis(0))
                .await?;
        }
    }
    Ok(())
}
