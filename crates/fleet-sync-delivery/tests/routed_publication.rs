#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, SyncTime};
use fleet_sync_delivery::*;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use support::{network::*, publication_network::PublicationBarrier, *};

const SUCCESSOR_ROUTE: &str = "6b99f3e1-a002-4f42-9101-338578c72102";

async fn stream_events(fixture: &Fixture, sender: PeerId, route: &str) -> serde_json::Value {
    let route = route.to_owned();
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let events = StatusStream::<_, TestTypes>::new(scope, sender, route)
                    .fetch(sender, StatusCursor::new(0), 20)
                    .await
                    .map_err(storage_error)?;
                Ok(serde_json::to_value(events).unwrap())
            })
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn held_old_publish_keeps_its_route_and_stream_restart_delivers_both_frozen_hops() {
    let mut network = NetworkFixture::new().await;
    let successor = Fixture::new().await;
    let successor_stream = Fixture::new().await;
    for fixture in [&successor, &successor_stream] {
        assert!(fixture.host.routes.set(network.routes.clone()).is_ok());
    }
    network.routes.publications.register(
        SUCCESSOR_ROUTE,
        peer(3),
        &successor.store,
        &successor_stream.store,
    );
    network.enqueue(2, "unrelated old item").await;
    drain(&network.engine(&network.sender)).await;
    drain(&network.engine(&network.mailbox)).await;
    drain(&network.engine(&network.status)).await;
    let unrelated = stream_events(&network.status, peer(1), "fixture-route").await;
    assert_eq!(unrelated["events"].as_array().unwrap().len(), 1);

    network.enqueue(1, "inherited item").await;
    drain(&network.engine(&network.sender)).await;
    successor
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Item(item) = item(1, "inherited item") else {
                    return Err(durable_steps::StoreError::backend("fixture item"));
                };
                Outbox::<_, TestTypes>::new(scope, limits())
                    .enqueue(item, peer(3))
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();

    let reached = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *network.routes.publications.after_append.lock().unwrap() = Some(PublicationBarrier {
        item: item_id(1),
        reached: reached.clone(),
        release: release.clone(),
    });
    let mailbox_engine = network.engine(&network.mailbox);
    let held = tokio::spawn(async move { mailbox_engine.drive_due().await.unwrap() });
    tokio::time::timeout(Duration::from_secs(10), reached.notified())
        .await
        .unwrap();
    // The first-hop transaction is committed; the old call has not answered.
    assert_eq!(
        stream_events(&network.status, peer(1), "fixture-route").await["through"],
        2
    );
    network
        .mailbox
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let mut mailbox = Mailbox::<_, TestTypes>::new(scope, peer(2), limits());
                assert!(matches!(
                    mailbox
                        .submit(
                            peer(3),
                            item(1, "inherited item"),
                            SyncTime::from_unix_millis(0),
                            SUCCESSOR_ROUTE.into(),
                            Some(2)
                        )
                        .await
                        .map_err(storage_error)?,
                    SubmitResult::Replayed { .. }
                ));
                mailbox
                    .report(
                        peer(2),
                        RecipientReport {
                            item: item_id(1),
                            report_seq: ReportSeq::new(1),
                            outcome: RecipientOutcome::Held,
                        },
                    )
                    .await
                    .map_err(storage_error)?;
                admit_publications::<_, TestTypes, PublishTask>(scope)
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    release.notify_one();
    let completed = tokio::time::timeout(Duration::from_secs(10), held)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.completed.len(), 1);
    assert!(completed.failed.is_empty());
    assert!(completed.retried.is_empty());
    network
        .mailbox
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let newer = PublicationRef {
                    item: item_id(1),
                    version: StatusVersion::new(2),
                    target: PublicationTarget::StatusStream {
                        sender: peer(3),
                        route: SUCCESSOR_ROUTE.to_owned(),
                    },
                };
                assert!(matches!(
                    resolve_publication::<_, TestTypes>(scope, newer)
                        .await
                        .map_err(storage_error)?
                        .unwrap()
                        .status,
                    DeliveryStatus::Held
                ));
                Ok(())
            })
        })
        .await
        .unwrap();

    // Lose a later committed first-hop answer, then retry its actual durable call.
    network
        .routes
        .lose_publication
        .store(true, Ordering::SeqCst);
    let lost = network.engine(&network.mailbox).drive_due().await.unwrap();
    assert_eq!(lost.retried.len(), 1);
    assert!(lost.failed.is_empty());
    network.advance();
    drain(&network.engine(&network.mailbox)).await;
    let calls = network.routes.publications.calls.lock().unwrap().clone();
    let successor_calls: Vec<_> = calls.iter().filter(|(_, target, publication)| matches!(target, PublicationTarget::StatusStream { route, .. } if route == SUCCESSOR_ROUTE) && publication["item"] == serde_json::json!(item_id(1))).collect();
    assert_eq!(successor_calls.len(), 2);
    assert_eq!(
        successor_calls.first(),
        successor_calls.get(1),
        "same CallId must keep its committed target and request"
    );

    // Close and reopen the real stream store between append and its Peer task.
    let Fixture {
        store,
        host,
        directory,
    } = network.status;
    assert_eq!(Arc::strong_count(&store), 1);
    drop(store);
    let store = Arc::new(
        TestStore::open_and_migrate(directory.path().join("delivery.db"), host.clone())
            .await
            .unwrap(),
    );
    network
        .routes
        .publications
        .register("fixture-route", peer(1), &network.sender.store, &store);
    network.status = Fixture {
        store,
        host,
        directory,
    };
    drain(&network.engine(&network.status)).await;
    drain(&network.engine(&successor_stream)).await;
    let old_events = stream_events(&network.status, peer(1), "fixture-route").await;
    assert_eq!(old_events["events"].as_array().unwrap().len(), 2);
    assert_eq!(old_events["events"][0], unrelated["events"][0]);
    assert_eq!(old_events["through"], 2);
    let new_events = stream_events(&successor_stream, peer(3), SUCCESSOR_ROUTE).await;
    assert_eq!(new_events["events"].as_array().unwrap().len(), 1);
    assert_eq!(new_events["through"], 1);
    assert!(matches!(
        sender_status(&network, 1).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Sent { .. },
            ..
        }
    ));
    assert!(matches!(
        sender_status(&network, 2).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Sent { .. },
            ..
        }
    ));
    successor
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, TestTypes>::new(scope, limits())
                        .status(item_id(1))
                        .await
                        .map_err(storage_error)?
                        .state,
                    OutboxStatusState::Known {
                        status: DeliveryStatus::Held,
                        ..
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    let calls = network.routes.publications.calls.lock().unwrap();
    assert!(calls.iter().any(|(_, target, publication)| matches!(target, PublicationTarget::Peer { peer:owner, route } if *owner==peer(1) && route=="fixture-route") && publication["item"] == serde_json::json!(item_id(1))));
    assert!(calls.iter().any(|(_, target, publication)| matches!(target, PublicationTarget::Peer { peer:owner, route } if *owner==peer(3) && route==SUCCESSOR_ROUTE) && publication["item"] == serde_json::json!(item_id(1))));
}
