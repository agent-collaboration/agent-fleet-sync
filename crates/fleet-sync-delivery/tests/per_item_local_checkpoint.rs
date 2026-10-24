#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{
    AtomicStore, BoxFuture, CallResult, CallTarget, Engine, JournalScope, OperationContext,
    OperationFailure, RequestKey, Resume, StepError, StepName, StoreError, Task,
    storage::TransactionScope, turso_store::TursoScope,
};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use support::{network::*, *};

struct LocalTypes;
impl DeliveryTypes for LocalTypes {
    const MAX_SENDER_BYTES: u32 = 64;
    const MAX_ADDRESS_BYTES: u32 = 64;
    const MAX_ACCEPTANCE_BYTES: u32 = 128;
    const PER_ITEM_SETTLEMENT: bool = true;
    const MAX_LOCAL_OBSERVATION_BYTES: u32 = 4;
    const MAX_STATUS_ROUTE_BYTES: u32 = 64;
    type Body = String;
    type Sender = String;
    type Address = String;
    type Acceptance = Acceptance;
    type LocalObservation = ();
    type StatusRoute = String;
    fn max_acceptance_bytes_for(body: &String) -> u32 {
        if body == "large" { 128 } else { 64 }
    }
}
struct LocalProtocol;
impl ConsumerProtocol for LocalProtocol {
    type Delivery = LocalTypes;
    type Request = ();
    type Answer = ();
    type ServerRequest = ();
    type ClientAnswer = ();
    type Notice = ();
    type Annotation = ();
}
struct LocalConsumer;
impl DeliveryConsumer<TursoScope<'_, TestHost>> for LocalConsumer {
    type Protocol = LocalProtocol;
    async fn release_gate(
        &self,
        _: &mut TursoScope<'_, TestHost>,
        _: &DeliveryItem<LocalTypes>,
    ) -> Result<GateDecision, DeliveryError> {
        Ok(GateDecision::Release)
    }
    async fn deliver_locally(
        &self,
        item: &DeliveryItem<LocalTypes>,
        _: durable_steps::CallId,
    ) -> LocalDelivery<LocalTypes> {
        LocalDelivery {
            outcome: LocalOutcome::Delivered {
                acceptance: Acceptance {
                    receipt: item.body().clone(),
                },
            },
            observation: (),
        }
    }
    async fn on_outcome(
        &self,
        scope: &mut TursoScope<'_, TestHost>,
        _: ItemId,
        outcome: &RecipientOutcome<Acceptance>,
        observation: Option<&()>,
    ) -> Result<(), DeliveryError> {
        sqlx_core::query::query::<sqlx_turso::Turso>(
            "INSERT INTO local_projection(outcome,observation) VALUES(?1,?2)",
        )
        .bind(serde_json::to_string(outcome).unwrap())
        .bind(observation.map(|v| serde_json::to_string(v).unwrap()))
        .execute(scope.executor())
        .await
        .map_err(|e| DeliveryError::Storage(e.to_string()))?;
        Ok(())
    }
    fn may_cancel(&self, requester: &String, item: &DeliveryHeader<LocalTypes>) -> bool {
        requester == item.sender()
    }
}
type LocalTask = DeliverLocally<TestStore, sqlx_turso::Turso, LocalTypes, LocalConsumer>;
type LocalReport = ReportOutcome<TestStore, sqlx_turso::Turso, LocalTypes>;
struct PausedLocalTask {
    inner: LocalTask,
    reached: Arc<tokio::sync::Notify>,
}
impl Task for PausedLocalTask {
    const KIND: &'static str = LocalTask::KIND;
    const VERSION: u32 = LocalTask::VERSION;
    const KEEP_RESULT: bool = false;
    type Store = TestStore;
    type Input = DeliveryAttempt;
    type Output = ();
    async fn run<O: OperationContext<Store = TestStore>>(
        &self,
        ctx: &mut O,
        input: &DeliveryAttempt,
    ) -> Result<(), OperationFailure> {
        self.inner
            .run(
                &mut PauseContext {
                    inner: ctx,
                    reached: self.reached.clone(),
                },
                input,
            )
            .await
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut TursoScope<'t, TestHost>,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<()>, StoreError>> {
        self.inner.before_resume(scope, key)
    }
    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut TursoScope<'t, TestHost>,
        key: &'s RequestKey,
        failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>> {
        self.inner.on_failed(scope, key, failure)
    }
}
struct PauseContext<'a, O> {
    inner: &'a mut O,
    reached: Arc<tokio::sync::Notify>,
}
impl<O: OperationContext<Store = TestStore>> OperationContext for PauseContext<'_, O> {
    type Store = TestStore;
    fn id(&self) -> &OperationId {
        self.inner.id()
    }
    fn now(&self) -> SyncTime {
        self.inner.now()
    }
    async fn local<T, F>(&mut self, step: StepName, run: F) -> Result<T, StepError>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
        F: for<'s, 't> FnOnce(
                &'s mut TursoScope<'t, TestHost>,
            ) -> BoxFuture<'s, Result<T, StepError>>
            + Send
            + 'static,
    {
        if step.as_str() == "outcome" {
            self.reached.notify_one();
            std::future::pending::<()>().await;
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

#[tokio::test]
async fn stored_item_bound_sanitizes_call_checkpoint_before_reopen_without_resending() {
    for (body, receipt, uncertain) in [
        ("small", "x".repeat(100), true),
        ("small", "ok".into(), false),
        ("large", "x".repeat(100), false),
    ] {
        let mut network = NetworkFixture::new().await;
        network.enqueue(1, body).await;
        drain(&network.engine(&network.sender)).await;
        let batch = network
            .mailbox
            .store
            .transaction(|scope| {
                Box::pin(async move {
                    Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .fetch(
                            peer(2),
                            DeliveryStream::Items,
                            DeliverySeq::new(0),
                            1,
                            10_000,
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)
                })
            })
            .await
            .unwrap();
        let batch: FetchResult<LocalTypes> =
            serde_json::from_value(serde_json::to_value(batch).unwrap()).unwrap();
        let through = batch.through;
        network
            .recipient
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    sqlx_core::query::query::<sqlx_turso::Turso>(
                        "CREATE TABLE local_projection(outcome TEXT NOT NULL,observation TEXT)",
                    )
                    .execute(scope.executor())
                    .await?;
                    let mut inbox = Inbox::<_, LocalTypes>::new(scope, peer(2), limits());
                    let input = inbox
                        .begin_receive(DeliveryStream::Items)
                        .await
                        .map_err(storage_error)?
                        .unwrap();
                    let effects = inbox
                        .store_batch(input, batch, &LocalConsumer, SyncTime::from_unix_millis(0))
                        .await
                        .map_err(storage_error)?;
                    for attempt in effects.attempts {
                        scope
                            .admit::<LocalTask>(
                                &delivery_request_key(&(attempt.item, attempt.attempt)).unwrap(),
                                &attempt,
                            )
                            .await
                            .map_err(StoreError::backend)?;
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
        network
            .mailbox
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .received(
                            peer(2),
                            DeliveryStream::Items,
                            through,
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        network
            .routes
            .local_answers
            .lock()
            .unwrap()
            .push_back(LocalOutcome::Delivered {
                acceptance: Acceptance {
                    receipt: receipt.clone(),
                },
            });
        let reached = Arc::new(tokio::sync::Notify::new());
        let mut engine = Engine::new(
            network.recipient.store.clone(),
            network.recipient.host.clone(),
        );
        engine
            .register(PausedLocalTask {
                inner: LocalTask::new(
                    config().local,
                    Arc::new(LocalConsumer),
                    network.recipient.host.clone(),
                ),
                reached: reached.clone(),
            })
            .unwrap();
        let worker = tokio::spawn(async move { engine.drive_due().await });
        tokio::time::timeout(Duration::from_secs(10), reached.notified())
            .await
            .unwrap();
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
        network
            .recipient
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                        "SELECT output FROM _fleet_sync_steps WHERE step_name='deliver'",
                    )
                    .fetch_one(scope.executor())
                    .await?;
                    let raw: Vec<u8> = row.try_get("output")?;
                    let answer: CallResult<LocalDeliveryOutcome<LocalTypes>> =
                        serde_json::from_slice(&raw).unwrap();
                    if uncertain {
                        assert!(
                            matches!(
                                answer,
                                CallResult::Answered(DispatchOutcome::Answered(
                                    LocalDeliveryAnswer::Uncertain
                                ))
                            ),
                            "invalid per-item receipt must be bounded before journaling"
                        );
                        assert!(!String::from_utf8(raw).unwrap().contains("observation"));
                    } else {
                        assert!(matches!(
                            answer,
                            CallResult::Answered(DispatchOutcome::Answered(
                                LocalDeliveryAnswer::Observed {
                                    outcome: LocalOutcome::Delivered { .. },
                                    ..
                                }
                            ))
                        ));
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
        let path = network.recipient.directory.path().join("delivery.db");
        let replacement = Fixture::new().await;
        let old = std::mem::replace(&mut network.recipient.store, replacement.store.clone());
        let old_store = Arc::downgrade(&old);
        drop(old);
        assert!(
            old_store.upgrade().is_none(),
            "old store must close before reopen"
        );
        network.recipient.store = Arc::new(
            TestStore::open_and_migrate(path, network.recipient.host.clone())
                .await
                .unwrap(),
        );
        *network.routes.recipient.lock().unwrap() = Arc::downgrade(&network.recipient.store);
        network.advance();
        let mut engine = Engine::new(
            network.recipient.store.clone(),
            network.recipient.host.clone(),
        );
        engine
            .register(LocalTask::new(
                config().local,
                Arc::new(LocalConsumer),
                network.recipient.host.clone(),
            ))
            .unwrap();
        engine.register(LocalReport::new(config())).unwrap();
        let report = engine.drive_due().await.unwrap();
        assert!(report.failed.is_empty(), "{report:?}");
        assert!(report.retried.is_empty(), "{report:?}");
        engine.drive_due().await.unwrap();
        assert_eq!(network.routes.local_calls.load(Ordering::SeqCst), 1);
        network
            .recipient
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                        "SELECT outcome,observation FROM local_projection",
                    )
                    .fetch_one(scope.executor())
                    .await?;
                    let value: serde_json::Value =
                        serde_json::from_str(&row.try_get::<String, _>("outcome")?).unwrap();
                    assert_eq!(
                        value["kind"],
                        if uncertain { "uncertain" } else { "delivered" }
                    );
                    assert_eq!(
                        row.try_get::<Option<String>, _>("observation")?,
                        if uncertain { None } else { Some("null".into()) }
                    );
                    let result = Inbox::<_, LocalTypes>::new(scope, peer(2), limits())
                        .local_outcome(item_id(1))
                        .await
                        .map_err(storage_error)?
                        .unwrap();
                    assert_eq!(matches!(result, RecipientOutcome::Uncertain), uncertain);
                    Ok(())
                })
            })
            .await
            .unwrap();
        drain(&network.engine(&network.mailbox)).await;
        drain(&network.engine(&network.status)).await;
        let status = sender_status(&network, 1).await;
        assert_eq!(
            matches!(
                status,
                OutboxStatusState::Known {
                    status: DeliveryStatus::Uncertain,
                    ..
                }
            ),
            uncertain
        );
    }
}

#[test]
fn unbound_opt_in_adapter_cannot_checkpoint_a_delivered_receipt() {
    let answer = DeliverCall::<LocalTypes>::validated_answer(LocalDelivery {
        outcome: LocalOutcome::Delivered {
            acceptance: Acceptance {
                receipt: "ok".into(),
            },
        },
        observation: (),
    });
    assert!(matches!(
        answer,
        DispatchOutcome::Answered(LocalDeliveryAnswer::Uncertain)
    ));
}
