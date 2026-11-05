use super::support::{network::*, *};
use durable_steps::AtomicStore;
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::{sync::Arc, time::Duration};

#[derive(Clone, Copy)]
enum AppendOrder {
    InOrder,
    LowerAfterUnsentHigher,
    LowerAfterAppliedHigher,
}
const BODY: &str = "publication order proof";
fn canonical_publication(version: u64) -> StatusPublication<Acceptance> {
    StatusPublication {
        item: item_id(1),
        version: StatusVersion::new(version),
        cancel: None,
        status: match version {
            1 => DeliveryStatus::Sent,
            2 => DeliveryStatus::AtRecipient,
            _ => DeliveryStatus::Delivered {
                acceptance: Acceptance {
                    receipt: BODY.into(),
                },
            },
        },
    }
}
async fn append_status(store: Arc<TestStore>, publication: StatusPublication<Acceptance>) {
    store
        .transaction(move |scope| {
            Box::pin(async move {
                StatusStream::<_, TestTypes>::new(
                    scope,
                    Submitter::Machine(peer(1)),
                    "fixture-route".into(),
                )
                .append_and_admit::<PublishTask>(&publication, SyncTime::from_unix_millis(0))
                .await
                .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
}
async fn stream_state(network: &NetworkFixture) -> (Vec<i64>, Vec<i64>) {
    use durable_steps::storage::TransactionScope;
    network.status.store.transaction(|scope|Box::pin(async move {
        let events=query::<Turso>("SELECT version FROM _fleet_sync_status_events WHERE item_id=?1 ORDER BY cursor").bind(item_id(1).as_uuid().to_string()).fetch_all(scope.executor()).await?;
        let debt=query::<Turso>("SELECT version FROM _fleet_sync_status_owed WHERE item_id=?1").bind(item_id(1).as_uuid().to_string()).fetch_all(scope.executor()).await?;
        Ok((events.iter().map(|row|row.try_get("version")).collect::<Result<Vec<_>,_>>()?,debt.iter().map(|row|row.try_get("version")).collect::<Result<Vec<_>,_>>()?))
    })).await.unwrap()
}
async fn scenario(order: AppendOrder) {
    let network = NetworkFixture::new().await;
    network
        .sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(original) = item(1, BODY) else {
                    return Err(durable_steps::StoreError::backend("fixture item"));
                };
                Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                    .enqueue(
                        original,
                        peer(3),
                        Submitter::Machine(peer(1)),
                        SyncTime::from_unix_millis(0),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    append_status(network.status.store.clone(), canonical_publication(1)).await;
    drain(&network.engine(&network.status)).await;
    let reached = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let store = network.status.store.clone();
    let ready = reached.clone();
    let release = resume.clone();
    // The caller prepares v2, then waits before its first real append. No
    // storage lock is held, so the independent v3 caller can commit first.
    let pending = canonical_publication(2);
    let held = tokio::spawn(async move {
        ready.notify_one();
        release.notified().await;
        append_status(store, pending).await;
    });
    tokio::time::timeout(Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    assert_eq!(stream_state(&network).await.0, vec![1]);
    match order {
        AppendOrder::InOrder => {
            resume.notify_one();
            held.await.unwrap();
            append_status(network.status.store.clone(), canonical_publication(3)).await;
        }
        AppendOrder::LowerAfterUnsentHigher | AppendOrder::LowerAfterAppliedHigher => {
            append_status(network.status.store.clone(), canonical_publication(3)).await;
            assert_eq!(stream_state(&network).await, (vec![1, 3], vec![3]));
            if matches!(order, AppendOrder::LowerAfterAppliedHigher) {
                drain(&network.engine(&network.status)).await;
            }
            resume.notify_one();
            held.await.unwrap();
        }
    }
    drain(&network.engine(&network.status)).await;
    let actual = sender_status(&network, 1).await;
    let observed = serde_json::to_value(&actual).unwrap();
    assert!(
        matches!(actual,OutboxStatusState::Known {status:DeliveryStatus::Delivered {acceptance},version,..} if version.value()==3&&acceptance.receipt==BODY),
        "sender ledger lost the latest publication: {observed}"
    );
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
