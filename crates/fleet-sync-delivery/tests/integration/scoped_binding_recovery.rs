use super::support::{network::*, push_expiry::push_operation, sender_binding::*, *};
use durable_steps::{
    AtomicStore,
    storage::{CallReach, JournalStorage, StepState, TransactionScope},
};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
const SERVICE: &str = "11111111-1111-4111-8111-111111111111";
fn now() -> SyncTime {
    SyncTime::from_unix_millis(0)
}
fn envelope() -> DeliveryItem<TestTypes> {
    DeliveryItem::new(
        item_id(1),
        format!("svc:{SERVICE}|session"),
        Recipient {
            peer: peer(2),
            address: "inbox".into(),
        },
        None,
        "sealed body".into(),
    )
    .unwrap()
}

#[tokio::test]
async fn cold_recipient_signals_refresh_outside_custody_before_any_timer_then_retries_real_push_once()
 {
    let network = NetworkFixture::new().await;
    *network.routes.authenticated_submitter.lock().unwrap() = Submitter::Machine(peer(9));
    network.routes.publications.register(
        "fixture-route",
        peer(9),
        &network.sender.store,
        &network.status.store,
    );
    for fixture in [&network.sender, &network.relay, &network.recipient] {
        fixture
            .store
            .transaction(|scope| Box::pin(async move { create_binding_directory(scope).await }))
            .await
            .unwrap();
    }
    for fixture in [&network.sender, &network.relay] {
        fixture
            .store
            .transaction(|scope| {
                Box::pin(async move { insert_binding(scope, 90, SERVICE, peer(9)).await })
            })
            .await
            .unwrap();
    }
    network
        .sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let (answer, admitted) =
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .enqueue_and_admit::<SubmitTask, RecordRemoval>(
                            envelope(),
                            peer(3),
                            Submitter::Machine(peer(9)),
                            now(),
                        )
                        .await
                        .map_err(storage_error)?;
                assert!(matches!(answer, OutboxAdmission::Queued { .. }));
                assert!(admitted.is_some());
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(
        network
            .engine(&network.sender)
            .drive_due()
            .await
            .unwrap()
            .completed
            .len(),
        1
    );
    let requested = Arc::new(tokio::sync::Notify::new());
    *network.routes.binding_refresh.lock().unwrap() = Some(requested.clone());
    let recipient = network.recipient.store.clone();
    let refresh = tokio::spawn(async move {
        // The host's authenticated lookup returns the independently known peer.
        // A fresh transaction proves the custody transaction released its scope.
        requested.notified().await;
        recipient
            .transaction(|scope| {
                Box::pin(async move { insert_binding(scope, 90, SERVICE, peer(9)).await })
            })
            .await
            .unwrap();
    });
    let relay_engine = network.engine(&network.relay);
    let first = relay_engine.drive_due().await.unwrap();
    assert_eq!(first.retried.len(), 1);
    assert!(first.failed.is_empty());
    tokio::time::timeout(Duration::from_secs(5), refresh)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        network.recipient.host.clock.load(Ordering::SeqCst),
        0,
        "refresh must not depend on a timer tick"
    );
    let operation = push_operation(&network.relay.store).await;
    network.relay.store.transaction(move|scope|Box::pin(async move {
        assert!(matches!(scope.step(operation,0).await?.unwrap().state,StepState::CallPending {attempt,..} if attempt.reach==CallReach::NoEffect));
        let row=query::<Turso>("SELECT payload,status_kind FROM _fleet_sync_outbox_items").fetch_one(scope.executor()).await?;
        assert!(row.try_get::<Option<String>,_>("payload")?.is_some()); assert_eq!(row.try_get::<String,_>("status_kind")?,"sent"); Ok(())
    })).await.unwrap();
    assert_eq!(
        super::support::push_expiry::recipient_rows(&network).await,
        (0, 0)
    );
    network.advance();
    assert_eq!(relay_engine.drive_due().await.unwrap().completed.len(), 1);
    drain(&network.engine(&network.recipient)).await;
    drain(&relay_engine).await;
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert_eq!(
                    query::<Turso>("SELECT count(*) AS count FROM _fleet_sync_inbox_items")
                        .fetch_one(scope.executor())
                        .await?
                        .try_get::<i64, _>("count")?,
                    1
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    assert_eq!(network.routes.push_bodies.load(Ordering::SeqCst), 2);
}
