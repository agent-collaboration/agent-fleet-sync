use super::support::*;
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::Arc;

#[tokio::test]
async fn wire_project_claim_settles_an_unknown_item_and_survives_reopen_and_query() {
    let fixture = Fixture::new().await;
    let project = Submitter::Project(DatabaseId::try_from(peer(7).as_uuid().to_string()).unwrap());
    let successor = peer(9);
    let digest = ItemDigest::try_from("a".repeat(64)).unwrap();
    let request = DeliveryRequest::<TestTypes>::Settle {
        call: call("project settle"),
        mailbox: peer(2),
        submitter: project,
        item: item_id(1),
        digest: digest.clone(),
    };
    let bytes = serde_json::to_vec(&request).unwrap();
    let decoded: DeliveryRequest<TestTypes> = serde_json::from_slice(&bytes).unwrap();
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let DeliveryRequest::Settle {
                    submitter,
                    item,
                    digest,
                    ..
                } = decoded
                else {
                    return Err(durable_steps::StoreError::backend(
                        "fixture settlement missing",
                    ));
                };
                // Lease authorization belongs to the host. The SDK must keep
                // its Project claim instead of binding Machine(successor).
                assert_ne!(submitter, Submitter::Machine(successor));
                let answer = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .relay_settle::<RecordRemoval>(
                        submitter,
                        item,
                        digest,
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                assert!(matches!(answer, SettleResult::Settled {
                    reason: DeliveryFailureKind::CustodyUnresolvable, version,
                } if version.value() == 1));
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
    let store = Arc::new(
        TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
            .await
            .unwrap(),
    );
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut outbox = Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings);
                assert_eq!(outbox.relay_submitter(item_id(1)).await.map_err(storage_error)?, Some(project));
                let status = outbox.relay_query(project, item_id(1)).await.map_err(storage_error)?;
                assert!(matches!(status.state, ItemStatusState::Known {
                    status: DeliveryStatus::Refused {
                        reason: DeliveryRefusal::ProcessingFailed {
                            kind: DeliveryFailureKind::CustodyUnresolvable,
                        },
                    }, version, ..
                } if version.value() == 1));
                // Different peers and matching UUID bits must both respect the
                // Project/Machine namespace distinction.
                for machine in [successor, peer(7)] {
                    assert!(matches!(outbox.relay_query(Submitter::Machine(machine), item_id(1)).await,
                        Err(DeliveryError::Refused { reason: DeliveryRefusal::NotSubmitter })));
                    let mismatch = outbox
                        .relay_settle::<RecordRemoval>(Submitter::Machine(machine), item_id(1), digest.clone(), SyncTime::from_unix_millis(0))
                        .await
                        .map_err(storage_error)?;
                    assert!(matches!(mismatch, SettleResult::ItemConflict { .. }));
                }
                let row = query::<Turso>("SELECT submitter_kind,submitter_id FROM _fleet_sync_outbox_items WHERE item_id=?1")
                    .bind(item_id(1).as_uuid().to_string())
                    .fetch_one(scope.executor())
                    .await?;
                assert_eq!(row.try_get::<String, _>("submitter_kind")?, "project");
                assert_eq!(row.try_get::<String, _>("submitter_id")?, project.as_uuid().to_string());
                Ok(())
            })
        })
        .await
        .unwrap();
}
