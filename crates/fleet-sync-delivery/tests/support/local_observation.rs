#![allow(clippy::unwrap_used, dead_code)]
use crate::support::{Acceptance, item_id, limits, peer};
use durable_steps::{
    AtomicStore, BoxFuture, CallFailure, CallId, CallResult, CallTarget, Engine, Host,
    OperationContext, OperationFailure, RequestKey, Resume, StepError, StoreError, Task,
    storage::TransactionScope,
    turso_store::{TursoScope, TursoStore},
};
use fleet_sync_delivery::*;
use runtime_primitives::{Clock, IdSource, WakeFailure, WakeScheduler};
use serde::{Serialize, de::DeserializeOwned};
use sqlx_core::row::Row;
use sqlx_turso::Turso;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

pub struct ObservationTypes;
impl DeliveryTypes for ObservationTypes {
    const MAX_SENDER_BYTES: u32 = 64;
    const MAX_ADDRESS_BYTES: u32 = 64;
    const MAX_ACCEPTANCE_BYTES: u32 = 128;
    const MAX_LOCAL_OBSERVATION_BYTES: u32 = 96;
    const MAX_STATUS_ROUTE_BYTES: u32 = 64;
    type Body = String;
    type Sender = String;
    type Address = String;
    type Acceptance = Acceptance;
    type LocalObservation = String;
    type StatusRoute = String;
}
pub struct ObservationProtocol;
impl ConsumerProtocol for ObservationProtocol {
    type Delivery = ObservationTypes;
    type Request = ();
    type Answer = ();
    type ServerRequest = ();
    type ClientAnswer = ();
    type Notice = ();
    type Annotation = ();
}

#[derive(Default)]
pub struct ObservationConsumer {
    pub answers: Mutex<VecDeque<LocalDelivery<ObservationTypes>>>,
    pub refuse_projection: AtomicBool,
}
impl DeliveryConsumer<TursoScope<'_, ObservationHost>> for ObservationConsumer {
    type Protocol = ObservationProtocol;
    async fn release_gate(
        &self,
        _: &mut TursoScope<'_, ObservationHost>,
        _: &DeliveryItem<ObservationTypes>,
    ) -> Result<GateDecision, DeliveryError> {
        Ok(GateDecision::Release)
    }
    async fn deliver_locally(
        &self,
        _: &DeliveryItem<ObservationTypes>,
        _: CallId,
    ) -> LocalDelivery<ObservationTypes> {
        self.answers.lock().unwrap().pop_front().unwrap()
    }
    async fn on_outcome(
        &self,
        scope: &mut TursoScope<'_, ObservationHost>,
        item: ItemId,
        outcome: &RecipientOutcome<Acceptance>,
        observation: Option<&String>,
    ) -> Result<(), DeliveryError> {
        sqlx_core::query::query::<Turso>(
            "INSERT INTO application_observations(item,outcome,observation) VALUES(?1,?2,?3)",
        )
        .bind(item.as_uuid().to_string())
        .bind(serde_json::to_string(outcome).unwrap())
        .bind(observation.cloned())
        .execute(scope.executor())
        .await
        .map_err(|error| DeliveryError::Storage(error.to_string()))?;
        if self.refuse_projection.load(Ordering::SeqCst) {
            return Err(DeliveryError::Storage(
                "observation projection refused".into(),
            ));
        }
        Ok(())
    }
    fn may_cancel(&self, sender: &String, header: &DeliveryHeader<ObservationTypes>) -> bool {
        sender == header.sender()
    }
}

pub type ObservationStore = TursoStore<ObservationHost>;
pub type ObservationTask =
    DeliverLocally<ObservationStore, Turso, ObservationTypes, ObservationConsumer>;
pub struct ObservationHost {
    pub clock: AtomicU64,
    identities: AtomicU64,
    pub calls: AtomicU64,
    pub consumer: Arc<ObservationConsumer>,
    store: OnceLock<Weak<ObservationStore>>,
}
impl Clock for ObservationHost {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(self.clock.load(Ordering::SeqCst) as i64)
    }
}
impl IdSource for ObservationHost {
    fn new_id(&self) -> durable_steps::Uuid {
        durable_steps::Uuid::from_u128(
            0x01900000000070008000000000000100
                + u128::from(self.identities.fetch_add(1, Ordering::SeqCst)),
        )
    }
}
impl WakeScheduler for ObservationHost {
    async fn wake_at(&self, _: SyncTime) -> Result<(), WakeFailure> {
        Ok(())
    }
}
impl Host for ObservationHost {
    async fn dispatch<C: CallTarget>(
        &self,
        _: C,
        request: C::Request,
        call: CallId,
    ) -> Result<C::Answer, CallFailure> {
        assert_eq!(C::KIND, DeliverCall::<ObservationTypes>::KIND);
        let reference: DeliveryItemRef =
            serde_json::from_value(serde_json::to_value(request).unwrap()).unwrap();
        let item = self
            .store
            .get()
            .unwrap()
            .upgrade()
            .unwrap()
            .transaction(move |scope| {
                Box::pin(async move {
                    Inbox::<_, ObservationTypes>::new(scope, peer(2), limits())
                        .resolve_delivery(&reference)
                        .await
                        .map_err(StoreError::backend)
                })
            })
            .await
            .unwrap();
        self.calls.fetch_add(1, Ordering::SeqCst);
        let delivery = self.consumer.deliver_locally(&item, call).await;
        let answer = DeliverCall::<ObservationTypes>::validated_answer(delivery);
        Ok(serde_json::from_value(serde_json::to_value(answer).unwrap()).unwrap())
    }
}

pub struct ObservationFixture {
    pub store: Arc<ObservationStore>,
    pub host: Arc<ObservationHost>,
    pub directory: tempfile::TempDir,
}
impl ObservationFixture {
    pub async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let host = Arc::new(ObservationHost {
            clock: AtomicU64::new(0),
            identities: AtomicU64::new(0),
            calls: AtomicU64::new(0),
            consumer: Arc::new(ObservationConsumer::default()),
            store: OnceLock::new(),
        });
        let store = Arc::new(
            TursoStore::open_and_migrate(directory.path().join("observations.db"), host.clone())
                .await
                .unwrap(),
        );
        assert!(host.store.set(Arc::downgrade(&store)).is_ok());
        store.transaction(|scope| Box::pin(async move {
            let mut migrations = sqlx_core::migrate::Migrator::with_migrations(DELIVERY_MIGRATOR.iter().cloned().collect::<Vec<_>>());
            migrations.dangerous_set_table_name("_fleet_sync_delivery_migrations");
            migrations.run_direct(None,scope.executor(),false).await.map_err(StoreError::backend)?;
            sqlx_core::query::query::<Turso>("CREATE TABLE application_observations(event INTEGER PRIMARY KEY,item TEXT NOT NULL,outcome TEXT NOT NULL,observation TEXT)").execute(scope.executor()).await?;
            Ok(())
        })).await.unwrap();
        Self {
            store,
            host,
            directory,
        }
    }
    pub fn engine(&self) -> Engine<ObservationStore, ObservationHost> {
        let mut engine = Engine::new(self.store.clone(), self.host.clone());
        engine.register(self.task()).unwrap();
        engine
    }
    pub fn task(&self) -> ObservationTask {
        ObservationTask::new(config(), self.host.consumer.clone(), self.host.clone())
    }
    pub async fn enqueue(&self, id: u64, expiry: Option<SyncTime>) {
        self.store
            .transaction(move |scope| {
                Box::pin(async move {
                    let item = DeliveryItem::<ObservationTypes>::new(
                        item_id(id),
                        "sender".into(),
                        Recipient {
                            peer: peer(2),
                            address: "inbox".into(),
                        },
                        expiry,
                        "body".into(),
                    )
                    .unwrap();
                    Inbox::<_, ObservationTypes>::new(scope, peer(2), limits())
                        .enqueue_local_and_admit::<ObservationTask>(
                            item,
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(StoreError::backend)?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
    pub async fn projections(&self) -> Vec<(String, serde_json::Value, Option<String>)> {
        self.store.transaction(|scope| Box::pin(async move {
            let rows=sqlx_core::query::query::<Turso>("SELECT item,outcome,observation FROM application_observations ORDER BY item,event").fetch_all(scope.executor()).await?;
            rows.into_iter().map(|row| Ok((row.try_get("item")?,serde_json::from_str(&row.try_get::<String,_>("outcome")?).unwrap(),row.try_get("observation")?))).collect::<Result<Vec<_>,StoreError>>()
        })).await.unwrap()
    }
}
pub fn config() -> DeliveryTaskConfig {
    DeliveryTaskConfig {
        owner: peer(2),
        relay: peer(3),
        limits: limits(),
        fetch_items: 10,
        fetch_bytes: 100_000,
    }
}

pub struct PausedObservationTask {
    pub inner: ObservationTask,
    pub reached: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}
impl Task for PausedObservationTask {
    const KIND: &'static str = ObservationTask::KIND;
    const VERSION: u32 = ObservationTask::VERSION;
    const KEEP_RESULT: bool = ObservationTask::KEEP_RESULT;
    type Store = ObservationStore;
    type Input = DeliveryAttempt;
    type Output = ();
    async fn run<O: OperationContext<Store = ObservationStore>>(
        &self,
        ctx: &mut O,
        input: &DeliveryAttempt,
    ) -> Result<(), OperationFailure> {
        self.inner
            .run(
                &mut PauseContext {
                    inner: ctx,
                    reached: self.reached.clone(),
                    release: self.release.clone(),
                },
                input,
            )
            .await
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut TursoScope<'t, ObservationHost>,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<()>, StoreError>> {
        self.inner.before_resume(scope, key)
    }
    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut TursoScope<'t, ObservationHost>,
        key: &'s RequestKey,
        failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>> {
        self.inner.on_failed(scope, key, failure)
    }
}
struct PauseContext<'a, O> {
    inner: &'a mut O,
    reached: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
impl<O: OperationContext<Store = ObservationStore>> OperationContext for PauseContext<'_, O> {
    type Store = ObservationStore;
    fn id(&self) -> &OperationId {
        self.inner.id()
    }
    fn now(&self) -> SyncTime {
        self.inner.now()
    }
    async fn local<T, F>(&mut self, step: StepName, run: F) -> Result<T, StepError>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: for<'s, 't> FnOnce(
                &'s mut TursoScope<'t, ObservationHost>,
            ) -> BoxFuture<'s, Result<T, StepError>>
            + Send
            + 'static,
    {
        if step.as_str() == "outcome" {
            self.reached.notify_one();
            self.release.notified().await;
        }
        self.inner.local(step, run).await
    }
    async fn call<C: CallTarget>(
        &mut self,
        step: StepName,
        target: C,
        request: C::Request,
    ) -> Result<C::Answer, StepError> {
        self.inner.call(step, target, request).await
    }
    async fn call_or_give_up<C: CallTarget>(
        &mut self,
        step: StepName,
        target: C,
        request: C::Request,
        unsent_for: Duration,
    ) -> Result<CallResult<C::Answer>, StepError> {
        self.inner
            .call_or_give_up(step, target, request, unsent_for)
            .await
    }
}
