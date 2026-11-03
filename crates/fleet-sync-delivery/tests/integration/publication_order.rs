use super::support::{network::*, publication_order::*, *};
use durable_steps::{AtomicStore, Engine, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

#[derive(Clone, Copy)]
enum AppendOrder {
    InOrder,
    LowerAfterUnsentHigher,
    LowerAfterAppliedHigher,
}
const BODY: &str = "publication order proof";
async fn mailbox_state(network: &NetworkFixture) -> (String, i64) {
    network.mailbox.store.transaction(|scope|Box::pin(async move {
        let row=query::<Turso>("SELECT json_extract(record,'$.value.status.kind') AS status,json_extract(record,'$.value.version') AS version FROM _fleet_sync_mail_items WHERE item_id=?1")
            .bind(item_id(1).as_uuid().to_string()).fetch_one(scope.executor()).await?;
        Ok((row.try_get("status")?,row.try_get("version")?))
    })).await.unwrap()
}
async fn stream_state(network: &NetworkFixture) -> (Vec<i64>, Vec<i64>) {
    network.status.store.transaction(|scope|Box::pin(async move {
        let events=query::<Turso>("SELECT version FROM _fleet_sync_status_events WHERE item_id=?1 ORDER BY cursor").bind(item_id(1).as_uuid().to_string()).fetch_all(scope.executor()).await?;
        let debt=query::<Turso>("SELECT version FROM _fleet_sync_status_owed WHERE item_id=?1").bind(item_id(1).as_uuid().to_string()).fetch_all(scope.executor()).await?;
        Ok((events.iter().map(|row|row.try_get("version")).collect::<Result<Vec<_>,_>>()?,debt.iter().map(|row|row.try_get("version")).collect::<Result<Vec<_>,_>>()?))
    })).await.unwrap()
}
async fn deliver_and_report(network: &NetworkFixture) {
    let engine = network.engine(&network.recipient);
    for _ in 0..8 {
        let driven = engine.drive_due().await.unwrap();
        assert!(driven.failed.is_empty());
        if mailbox_state(network).await == ("delivered".into(), 3) {
            return;
        }
    }
    assert_eq!(mailbox_state(network).await, ("delivered".into(), 3));
}
async fn scenario(order: AppendOrder) {
    let network = NetworkFixture::new().await;
    network.enqueue(1, BODY).await;
    drain(&network.engine(&network.sender)).await;
    drain(&network.engine(&network.mailbox)).await;
    drain(&network.engine(&network.status)).await;
    network.receive(DeliveryStream::Items).await;
    let received = network
        .engine(&network.recipient)
        .drive_due()
        .await
        .unwrap();
    assert_eq!(received.completed.len(), 1);
    assert!(received.failed.is_empty());
    let gate = Arc::new(PublicationAppendGate::new());
    let mut held_engine = Engine::new(network.mailbox.store.clone(), network.mailbox.host.clone());
    held_engine
        .register(PausePublicationAppend::new(gate.clone()))
        .unwrap();
    let held = tokio::spawn(async move { held_engine.drive_due().await });
    gate.wait_until_held().await;
    assert_eq!(mailbox_state(&network).await, ("atRecipient".into(), 2));
    assert_eq!(
        stream_state(&network).await.0,
        vec![1],
        "the held v2 must not have appended yet"
    );
    match order {
        AppendOrder::InOrder => {
            gate.resume_append();
            assert!(held.await.unwrap().unwrap().failed.is_empty());
            deliver_and_report(&network).await;
            drain(&network.engine(&network.mailbox)).await;
        }
        AppendOrder::LowerAfterUnsentHigher | AppendOrder::LowerAfterAppliedHigher => {
            deliver_and_report(&network).await;
            let higher = network.engine(&network.mailbox).drive_due().await.unwrap();
            assert!(higher.failed.is_empty());
            assert_eq!(stream_state(&network).await, (vec![1, 3], vec![3]));
            if matches!(order, AppendOrder::LowerAfterAppliedHigher) {
                drain(&network.engine(&network.status)).await;
            }
            gate.resume_append();
            assert!(held.await.unwrap().unwrap().failed.is_empty());
        }
    }
    let mut failures = Vec::new();
    for _ in 0..6 {
        failures.extend(
            network
                .engine(&network.status)
                .drive_due()
                .await
                .unwrap()
                .failed,
        );
        network.advance();
    }
    let actual = sender_status(&network, 1).await;
    let observed = serde_json::to_value(&actual).unwrap();
    assert!(
        matches!(actual,OutboxStatusState::Known {status:DeliveryStatus::Delivered {acceptance},version,..} if version.value()==3&&acceptance.receipt==BODY),
        "public sender must receive Delivered@3; observed {observed}"
    );
    assert_eq!(mailbox_state(&network).await, ("delivered".into(), 3));
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    assert!(failures.is_empty(), "publication failures: {failures:?}");
    assert!(stream_state(&network).await.1.is_empty());
}
#[tokio::test]
async fn unseen_lower_revision_cannot_replace_unsent_higher_debt_or_lose_sender_delivery() {
    for _ in 0..3 {
        scenario(AppendOrder::LowerAfterUnsentHigher).await;
    }
}
#[tokio::test]
async fn ordered_publications_deliver_the_latest_revision() {
    scenario(AppendOrder::InOrder).await;
}
#[tokio::test]
async fn late_lower_revision_does_not_regress_an_already_delivered_sender() {
    scenario(AppendOrder::LowerAfterAppliedHigher).await;
}

#[tokio::test]
async fn same_revision_with_another_payload_still_conflicts_without_stream_changes() {
    let fixture = Fixture::new().await;
    fixture
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let owner = Submitter::Machine(peer(1));
                let mut stream =
                    StatusStream::<_, TestTypes>::new(scope, owner, "fixture-route".into());
                let publication = StatusPublication {
                    item: item_id(1),
                    status: DeliveryStatus::Delivered {
                        acceptance: Acceptance {
                            receipt: "first".into(),
                        },
                    },
                    version: StatusVersion::new(3),
                    cancel: None,
                };
                stream
                    .append(&publication, SyncTime::from_unix_millis(0))
                    .await
                    .map_err(storage_error)?;
                let before = stream
                    .fetch(owner, StatusCursor::new(0), 10)
                    .await
                    .map_err(storage_error)?;
                let changed = StatusPublication {
                    status: DeliveryStatus::Delivered {
                        acceptance: Acceptance {
                            receipt: "different".into(),
                        },
                    },
                    ..publication
                };
                assert!(matches!(
                    stream.append(&changed, SyncTime::from_unix_millis(0)).await,
                    Err(DeliveryError::CallConflict)
                ));
                let after = stream
                    .fetch(owner, StatusCursor::new(0), 10)
                    .await
                    .map_err(storage_error)?;
                assert_eq!(
                    serde_json::to_value(before).map_err(durable_steps::StoreError::backend)?,
                    serde_json::to_value(after).map_err(durable_steps::StoreError::backend)?
                );
                Ok(())
            })
        })
        .await
        .unwrap();
}
