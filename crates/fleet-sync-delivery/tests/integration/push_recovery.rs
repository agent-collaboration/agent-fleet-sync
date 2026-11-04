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
        .register(SubmitTask::new(
            limits(),
            Arc::new(TestConsumer),
            fixture.host.clone(),
            std::sync::Arc::new(FixtureBindings),
        ))
        .unwrap();
    engine
        .register(PushTask::new(
            limits(),
            fixture.host.clone(),
            std::sync::Arc::new(FixtureBindings),
        ))
        .unwrap();
    engine.register(PublishTask::new()).unwrap();
    engine
        .register(DeliverTask::new(
            config().local,
            Arc::new(TestConsumer),
            fixture.host.clone(),
            std::sync::Arc::new(FixtureBindings),
        ))
        .unwrap();
    engine
        .register(ReportTask::new(
            config(),
            fixture.host.clone(),
            std::sync::Arc::new(FixtureBindings),
        ))
        .unwrap();
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
    network.routes.lose_submit.store(true, Ordering::SeqCst);
    network.enqueue(1, "one durable body").await;
    let NetworkFixture {
        mut sender,
        mut relay,
        mut recipient,
        status,
        routes,
    } = network;
    let first = engine(&sender).drive_due().await.unwrap();
    assert_eq!(first.retried.len(), 1);
    assert!(payload(&sender.store, "outbox").await.is_some());
    assert!(payload(&relay.store, "outbox").await.is_some());
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
    relay = reopened(relay).await;
    *routes.relay.lock().unwrap() = Arc::downgrade(&relay.store);
    let reached = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *routes.push_barrier.lock().unwrap() = Some(PushBarrier {
        reached: reached.clone(),
        release,
    });
    let driver = tokio::spawn({
        let engine = engine(&relay);
        async move { engine.drive_due().await }
    });
    tokio::time::timeout(Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    assert!(payload(&recipient.store, "inbox").await.is_some());
    assert!(payload(&relay.store, "outbox").await.is_some());
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());
    recipient = reopened(recipient).await;
    *routes.recipient.lock().unwrap() = Arc::downgrade(&recipient.store);
    relay = reopened(relay).await;
    *routes.relay.lock().unwrap() = Arc::downgrade(&relay.store);
    relay.host.clock.store(100_000, Ordering::SeqCst);
    assert_eq!(engine(&relay).drive_due().await.unwrap().completed.len(), 1);
    assert!(payload(&relay.store, "outbox").await.is_none());
    relay.store.transaction(|scope|Box::pin(async move {
        let row=query::<Turso>("SELECT status_kind,(SELECT count(*) FROM _fleet_sync_status_owed) AS owed FROM _fleet_sync_outbox_items WHERE item_id=?1").bind(item_id(1).as_uuid().to_string()).fetch_one(scope.executor()).await?;
        assert_eq!(row.try_get::<String,_>("status_kind")?,"atRecipient");
        assert_eq!(row.try_get::<i64,_>("owed")?,1);
        Ok(())
    })).await.unwrap();
    relay = reopened(relay).await;
    *routes.relay.lock().unwrap() = Arc::downgrade(&relay.store);
    routes.lose_publication.store(true, Ordering::SeqCst);
    assert_eq!(engine(&relay).drive_due().await.unwrap().retried.len(), 1);
    sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
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
    relay.host.clock.fetch_add(100_000, Ordering::SeqCst);
    engine(&relay).drive_due().await.unwrap();
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
        engine(&relay).drive_due().await.unwrap();
    }
    assert_eq!(routes.local_calls.load(Ordering::SeqCst), 1);
    sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
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

#[tokio::test]
async fn no_capacity_push_retries_without_cached_answer_and_lands_once_after_capacity_returns() {
    use durable_steps::{
        OperationId, Task,
        storage::{JournalStorage, StepState},
    };
    let network = NetworkFixture::new().await;
    let fill = network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let original = DeliveryItem::<TestTypes>::new(
                    item_id(999),
                    "sender".into(),
                    Recipient {
                        peer: peer(2),
                        address: "inbox".into(),
                    },
                    None,
                    "f".repeat(
                        usize::try_from(
                            limits().inbox_bytes()
                                - u64::from(limits().settlement_allowance())
                                - 1024,
                        )
                        .unwrap(),
                    ),
                )
                .unwrap();
                let admission =
                    Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                        .enqueue_local(original, &TestConsumer, SyncTime::from_unix_millis(0))
                        .await
                        .map_err(storage_error)?;
                require_local_attempt(admission)
            })
        })
        .await
        .unwrap();
    network.enqueue(1, "wait for real receiving room").await;
    assert_eq!(
        engine(&network.sender)
            .drive_due()
            .await
            .unwrap()
            .completed
            .len(),
        1
    );
    let push = network
        .relay
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row =
                    query::<Turso>("SELECT operation_id FROM _fleet_sync_operations WHERE kind=?1")
                        .bind(PushTask::KIND)
                        .fetch_one(scope.executor())
                        .await?;
                let raw: String = row.try_get("operation_id")?;
                let item = ItemId::try_from(raw).map_err(durable_steps::StoreError::backend)?;
                OperationId::new(item.as_uuid()).map_err(durable_steps::StoreError::backend)
            })
        })
        .await
        .unwrap();
    let first = engine(&network.relay).drive_due().await.unwrap();
    assert_eq!(
        first.retried,
        vec![push],
        "no-capacity Push was cached or failed terminally"
    );
    assert!(first.completed.is_empty() && first.failed.is_empty());
    network.relay.store.transaction(move|scope|Box::pin(async move {
        let checkpoint=scope.step(push,0).await?.ok_or(durable_steps::StoreError::backend("missing push checkpoint"))?;
        assert!(matches!(checkpoint.state,StepState::CallPending {attempt,..} if attempt.retry_reason.as_ref().is_some_and(|reason|reason.code=="noCapacity")),"no-capacity answer was cached as CallDone");
        Ok(())
    })).await.unwrap();
    assert!(payload(&network.relay.store, "outbox").await.is_some());
    network
        .recipient
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                let count = query::<Turso>(
                    "SELECT count(*) AS count FROM _fleet_sync_inbox_items WHERE item_id=?1",
                )
                .bind(item_id(1).as_uuid().to_string())
                .fetch_one(scope.executor())
                .await?
                .try_get::<i64, _>("count")?;
                assert_eq!(count, 0);
                Inbox::<_, TestTypes, _>::new(scope, peer(2), limits(), &FixtureBindings)
                    .record_failure(
                        fill,
                        DeliveryFailureKind::ExecutionFailed,
                        &TestConsumer,
                        SyncTime::from_unix_millis(1),
                    )
                    .await
                    .map_err(storage_error)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    network.advance();
    assert_eq!(
        engine(&network.relay).drive_due().await.unwrap().completed,
        vec![push]
    );
    assert!(payload(&network.relay.store, "outbox").await.is_none());
    assert_eq!(
        engine(&network.recipient)
            .drive_due()
            .await
            .unwrap()
            .completed
            .len(),
        1
    );
    engine(&network.recipient).drive_due().await.unwrap();
    engine(&network.relay).drive_due().await.unwrap();
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    network
        .recipient
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = query::<Turso>(
                    "SELECT count(*) AS count FROM _fleet_sync_inbox_items WHERE item_id=?1",
                )
                .bind(item_id(1).as_uuid().to_string())
                .fetch_one(scope.executor())
                .await?;
                assert_eq!(row.try_get::<i64, _>("count")?, 1);
                Ok(())
            })
        })
        .await
        .unwrap();
    network
        .sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                assert!(matches!(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .status(item_id(1))
                        .await
                        .map_err(storage_error)?
                        .state,
                    OutboxStatusState::Known {
                        status: DeliveryStatus::Delivered { .. },
                        ..
                    }
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}
