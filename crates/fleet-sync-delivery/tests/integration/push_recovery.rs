use super::support::{network::*, *};
use durable_steps::{AtomicStore, Engine, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

fn engine(fixture: &Fixture) -> Engine<TestStore, TestHost> {
    let mut engine = Engine::new(fixture.store.clone(), fixture.host.clone());
    engine
        .register(SubmitTask::new(limits(), Arc::new(TestConsumer)))
        .unwrap();
    engine.register(PushTask::new(limits())).unwrap();
    engine.register(PublishTask::new()).unwrap();
    engine
        .register(DeliverTask::new(
            config().local,
            Arc::new(TestConsumer),
            fixture.host.clone(),
        ))
        .unwrap();
    engine.register(ReportTask::new(config())).unwrap();
    engine
}
async fn reopened(fixture: Fixture) -> Fixture {
    let Fixture {
        store,
        host,
        directory,
    } = fixture;
    assert_eq!(
        Arc::strong_count(&store),
        1,
        "an active engine still owns the database"
    );
    drop(store);
    let store = Arc::new(
        TestStore::open_and_migrate(directory.path().join("delivery.db"), host.clone())
            .await
            .unwrap(),
    );
    Fixture {
        store,
        host,
        directory,
    }
}
async fn payload(store: &TestStore, table: &str) -> Option<String> {
    let statement = match table {
        "outbox" => "SELECT payload FROM _fleet_sync_outbox_items WHERE item_id=?1",
        _ => "SELECT payload FROM _fleet_sync_inbox_items WHERE item_id=?1",
    };
    store
        .transaction(move |scope| {
            Box::pin(async move {
                Ok(query::<Turso>(statement)
                    .bind(item_id(1).as_uuid().to_string())
                    .fetch_one(scope.executor())
                    .await?
                    .try_get("payload")?)
            })
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn lost_submit_and_push_answers_reopen_each_holder_and_keep_one_delivery() {
    let network = NetworkFixture::new().await;
    network.routes.fleet_push.store(true, Ordering::SeqCst);
    network.routes.lose_submit.store(true, Ordering::SeqCst);
    network.enqueue(1, "one durable body").await;
    let NetworkFixture {
        mut sender,
        mut mailbox,
        mut recipient,
        status,
        routes,
    } = network;
    let first = engine(&sender).drive_due().await.unwrap();
    assert_eq!(first.retried.len(), 1);
    assert!(payload(&sender.store, "outbox").await.is_some());
    assert!(payload(&mailbox.store, "outbox").await.is_some());
    sender = reopened(sender).await;
    *routes.sender.lock().unwrap() = Arc::downgrade(&sender.store);
    routes
        .publications
        .register("fixture-route", peer(1), &sender.store, &status.store);
    sender.host.clock.store(100_000, Ordering::SeqCst);
    assert_eq!(
        engine(&sender).drive_due().await.unwrap().completed.len(),
        1
    );
    assert!(payload(&sender.store, "outbox").await.is_none());
    mailbox = reopened(mailbox).await;
    *routes.mailbox.lock().unwrap() = Arc::downgrade(&mailbox.store);
    let reached = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *routes.push_barrier.lock().unwrap() = Some(FetchBarrier {
        reached: reached.clone(),
        release,
    });
    let driver = tokio::spawn({
        let engine = engine(&mailbox);
        async move { engine.drive_due().await }
    });
    tokio::time::timeout(Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    assert!(payload(&recipient.store, "inbox").await.is_some());
    assert!(payload(&mailbox.store, "outbox").await.is_some());
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());
    recipient = reopened(recipient).await;
    *routes.recipient.lock().unwrap() = Arc::downgrade(&recipient.store);
    mailbox = reopened(mailbox).await;
    *routes.mailbox.lock().unwrap() = Arc::downgrade(&mailbox.store);
    mailbox.host.clock.store(100_000, Ordering::SeqCst);
    assert_eq!(
        engine(&mailbox).drive_due().await.unwrap().completed.len(),
        1
    );
    assert!(payload(&mailbox.store, "outbox").await.is_none());
    mailbox.store.transaction(|scope|Box::pin(async move {
        let row=query::<Turso>("SELECT status_kind,(SELECT count(*) FROM _fleet_sync_status_owed) AS owed FROM _fleet_sync_outbox_items WHERE item_id=?1").bind(item_id(1).as_uuid().to_string()).fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<String,_>("status_kind")?,"atRecipient");
        assert_eq!(row.try_get::<i64,_>("owed")?,1);
        Ok(())
    })).await.unwrap();
    mailbox = reopened(mailbox).await;
    *routes.mailbox.lock().unwrap() = Arc::downgrade(&mailbox.store);
    routes.lose_publication.store(true, Ordering::SeqCst);
    assert_eq!(engine(&mailbox).drive_due().await.unwrap().retried.len(), 1);
    sender
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
                        status: DeliveryStatus::AtRecipient,
                        ..
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
    mailbox.host.clock.fetch_add(100_000, Ordering::SeqCst);
    engine(&mailbox).drive_due().await.unwrap();
    recipient.host.clock.store(100_000, Ordering::SeqCst);
    routes.lose_delivery.store(true, Ordering::SeqCst);
    assert_eq!(
        engine(&recipient).drive_due().await.unwrap().failed.len(),
        1
    );
    recipient = reopened(recipient).await;
    *routes.recipient.lock().unwrap() = Arc::downgrade(&recipient.store);
    recipient.host.clock.fetch_add(100_000, Ordering::SeqCst);
    for _ in 0..3 {
        engine(&recipient).drive_due().await.unwrap();
    }
    for _ in 0..3 {
        engine(&mailbox).drive_due().await.unwrap();
    }
    assert_eq!(routes.local_calls.load(Ordering::SeqCst), 1);
    sender
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
                        status: DeliveryStatus::Uncertain,
                        ..
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}
