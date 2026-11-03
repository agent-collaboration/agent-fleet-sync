use super::support::{
    network::{DeliverTask, ReportTask},
    *,
};
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Default)]
struct ReceiveProjection {
    reject: AtomicBool,
    calls: AtomicUsize,
}
impl<S: TransactionScope<Database = Turso>> DeliveryConsumer<S> for ReceiveProjection {
    type Protocol = TestProtocol;
    async fn on_received(
        &self,
        scope: &mut S,
        item: &DeliveryItem<TestTypes>,
    ) -> Result<(), DeliveryError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        query::<Turso>("INSERT INTO test_push_projection(item_id,body) VALUES(?1,?2)")
            .bind(item.id().as_uuid().to_string())
            .bind(item.body().clone())
            .execute(scope.executor())
            .await
            .map_err(|error| DeliveryError::Storage(error.to_string()))?;
        if self.reject.load(Ordering::SeqCst) {
            return Err(DeliveryError::Storage(
                "injected receive projection failure".into(),
            ));
        }
        Ok(())
    }
    async fn release_gate(
        &self,
        _scope: &mut S,
        _item: &DeliveryItem<TestTypes>,
    ) -> Result<GateDecision, DeliveryError> {
        Ok(GateDecision::Release)
    }
    async fn deliver_locally(
        &self,
        _item: &DeliveryItem<TestTypes>,
        _call: CallId,
    ) -> LocalDelivery<TestTypes> {
        LocalDelivery {
            outcome: LocalOutcome::Uncertain,
            observation: (),
        }
    }
    fn may_cancel(&self, requester: &String, item: &DeliveryHeader<TestTypes>) -> bool {
        requester == item.sender()
    }
}

async fn fixture() -> Fixture {
    let fixture = Fixture::new().await;
    fixture.store.transaction(|scope|Box::pin(async move {
        query::<Turso>("CREATE TABLE test_push_projection(item_id TEXT PRIMARY KEY NOT NULL,created_at INTEGER NOT NULL DEFAULT 0,updated_at INTEGER NOT NULL DEFAULT 0,body TEXT NOT NULL)")
            .execute(scope.executor()).await?;
        Ok(())
    })).await.unwrap();
    fixture
}

async fn counts(store: &TestStore) -> (i64, i64, i64) {
    store.transaction(|scope|Box::pin(async move {
        let row=query::<Turso>("SELECT (SELECT count(*) FROM _fleet_sync_inbox_items) AS items,(SELECT count(*) FROM test_push_projection) AS projections,(SELECT count(*) FROM _fleet_sync_operations WHERE kind='fleet-sync.delivery.deliver-locally') AS attempts")
            .fetch_one(scope.executor()).await?;
        Ok((row.try_get("items")?,row.try_get("projections")?,row.try_get("attempts")?))
    })).await.unwrap()
}

#[tokio::test]
async fn pushed_acceptance_hook_and_attempt_commit_together_and_replay_is_current_after_reopen() {
    let network = super::support::network::NetworkFixture::new().await;
    network
        .routes
        .local_answers
        .lock()
        .unwrap()
        .push_back(LocalOutcome::Held { until: None });
    let Fixture {
        store,
        host,
        directory,
    } = network.recipient;
    store.transaction(|scope|Box::pin(async move {
        query::<Turso>("CREATE TABLE test_push_projection(item_id TEXT PRIMARY KEY NOT NULL,created_at INTEGER NOT NULL DEFAULT 0,updated_at INTEGER NOT NULL DEFAULT 0,body TEXT NOT NULL)").execute(scope.executor()).await?;
        Ok(())
    })).await.unwrap();
    let consumer = Arc::new(ReceiveProjection::default());
    consumer.reject.store(true, Ordering::SeqCst);
    let result = store
        .transaction({
            let consumer = consumer.clone();
            move |scope| {
                Box::pin(async move {
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .accept_push::<DeliverTask, ReportTask, _>(
                            item(1, "projected once"),
                            peer(9),
                            consumer.as_ref(),
                            SyncTime::from_unix_millis(17),
                        )
                        .await
                        .map_err(storage_error)
                })
            }
        })
        .await;
    assert!(result.is_err());
    assert_eq!(counts(&store).await, (0, 0, 0));
    consumer.reject.store(false, Ordering::SeqCst);
    store.transaction({let consumer=consumer.clone();move|scope|Box::pin(async move {
        let answer=Inbox::<_,TestTypes>::new(scope,peer(2),limits()).accept_push::<DeliverTask,ReportTask,_>(item(1,"projected once"),peer(9),consumer.as_ref(),SyncTime::from_unix_millis(17)).await.map_err(storage_error)?;
        assert!(matches!(answer,PushResult::Accepted {accepted_at} if accepted_at==SyncTime::from_unix_millis(17)));
        Ok(())
    })}).await.unwrap();
    assert_eq!(counts(&store).await, (1, 1, 1));
    host.clock.store(17, Ordering::SeqCst);
    let mut engine = durable_steps::Engine::new(store.clone(), host.clone());
    engine
        .register(DeliverTask::new(
            super::support::network::config().local,
            Arc::new(TestConsumer),
            host.clone(),
        ))
        .unwrap();
    let completed = engine.drive_due().await.unwrap();
    assert_eq!(completed.completed.len(), 1);
    assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
    drop(engine);
    drop(store);
    let reopened = TestStore::open_and_migrate(directory.path().join("delivery.db"), host)
        .await
        .unwrap();
    reopened.transaction({let consumer=consumer.clone();move|scope|Box::pin(async move {
        let mut inbox=Inbox::<_,TestTypes>::new(scope,peer(2),limits());
        assert!(matches!(inbox.accept_push::<DeliverTask,ReportTask,_>(item(1,"projected once"),peer(9),consumer.as_ref(),SyncTime::from_unix_millis(24)).await.map_err(storage_error)?,PushResult::Replayed {status:DeliveryStatus::Held,..}));
        assert!(matches!(inbox.accept_push::<DeliverTask,ReportTask,_>(item(1,"changed content"),peer(9),consumer.as_ref(),SyncTime::from_unix_millis(24)).await.map_err(storage_error)?,PushResult::Conflict {existing} if existing==*item(1,"projected once").digest()));
        Ok(())
    })}).await.unwrap();
    assert_eq!(counts(&reopened).await, (1, 1, 0));
    assert_eq!(consumer.calls.load(Ordering::SeqCst), 2);
    reopened.transaction(|scope|Box::pin(async move {
        let row=query::<Turso>("SELECT payload,accepted_at,relay_machine_id FROM _fleet_sync_inbox_items WHERE item_id=?1").bind(item_id(1).as_uuid().to_string()).fetch_one(scope.executor()).await?;
        let payload:String=row.try_get("payload")?;
        assert_eq!(payload,serde_json::to_string(&item(1,"projected once")).map_err(StoreError::backend)?);
        assert_eq!(row.try_get::<i64,_>("accepted_at")?,17);
        assert_eq!(row.try_get::<String,_>("relay_machine_id")?,peer(9).as_uuid().to_string());
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn wrong_machine_and_full_inbox_pushes_leave_no_custody_projection_or_attempt() {
    let fixture = fixture().await;
    let consumer = Arc::new(ReceiveProjection::default());
    fixture
        .store
        .transaction({
            let consumer = consumer.clone();
            move |scope| {
                Box::pin(async move {
                    assert!(matches!(
                        Inbox::<_, TestTypes>::new(scope, peer(3), limits())
                            .accept_push::<DeliverTask, ReportTask, _>(
                                item(1, "wrong recipient"),
                                peer(9),
                                consumer.as_ref(),
                                SyncTime::from_unix_millis(1)
                            )
                            .await
                            .map_err(storage_error)?,
                        PushResult::Refused {
                            reason: DeliveryRefusal::NotThisMachine
                        }
                    ));
                    let tiny = DeliveryLimits::new::<TestTypes>(
                        100_000,
                        20,
                        100_000,
                        20,
                        1,
                        minimum_settlement_allowance::<TestTypes>().unwrap(),
                    )
                    .map_err(DeliveryError::from)
                    .map_err(storage_error)?;
                    assert!(matches!(
                        Inbox::<_, TestTypes>::new(scope, peer(2), tiny)
                            .accept_push::<DeliverTask, ReportTask, _>(
                                item(1, "no capacity"),
                                peer(9),
                                consumer.as_ref(),
                                SyncTime::from_unix_millis(1)
                            )
                            .await
                            .map_err(storage_error)?,
                        PushResult::Refused {
                            reason: DeliveryRefusal::MailboxFull
                        }
                    ));
                    Ok(())
                })
            }
        })
        .await
        .unwrap();
    assert_eq!(counts(&fixture.store).await, (0, 0, 0));
    assert_eq!(consumer.calls.load(Ordering::SeqCst), 0);
}
