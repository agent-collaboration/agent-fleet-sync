use super::{network::*, *};
use durable_steps::{
    AtomicStore, BoxFuture, CallResult, OperationContext, OperationFailure, RequestKey, Resume,
    StepError, StoreError, Task,
};
use std::sync::atomic::AtomicBool;
use std::time::Duration;

#[derive(Clone)]
pub enum AfterAnswer {
    Release,
    Cancel,
    Checkpoint {
        reached: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    },
    OutcomeCheckpoint {
        reached: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    },
}
pub struct InterleavedDeliver<C = TestConsumer> {
    inner: DeliverLocally<TestStore, sqlx_turso::Turso, TestTypes, C>,
    store: Arc<TestStore>,
    action: AfterAnswer,
    performed: Arc<AtomicBool>,
}
impl InterleavedDeliver {
    pub fn new(store: Arc<TestStore>, host: Arc<TestHost>, action: AfterAnswer) -> Self {
        Self::with_consumer(store, host, action, Arc::new(TestConsumer))
    }
}
impl<C> InterleavedDeliver<C> {
    pub fn with_consumer(
        store: Arc<TestStore>,
        host: Arc<TestHost>,
        action: AfterAnswer,
        consumer: Arc<C>,
    ) -> Self {
        Self {
            inner: DeliverLocally::new(config().local, consumer, host),
            store,
            action,
            performed: Arc::new(AtomicBool::new(false)),
        }
    }
}
impl<C> Task for InterleavedDeliver<C>
where
    C: Send + Sync + 'static,
    for<'t> C: DeliveryConsumer<
            durable_steps::turso_store::TursoScope<'t, TestHost>,
            Protocol = TestProtocol,
        >,
{
    const KIND: &'static str = DeliverTask::KIND;
    const VERSION: u32 = DeliverTask::VERSION;
    const KEEP_RESULT: bool = DeliverTask::KEEP_RESULT;
    type Store = TestStore;
    type Input = DeliveryAttempt;
    type Output = ();
    async fn run<O: OperationContext<Store = TestStore>>(
        &self,
        ctx: &mut O,
        input: &DeliveryAttempt,
    ) -> Result<(), OperationFailure> {
        let mut intercepted = InterceptContext {
            inner: ctx,
            store: self.store.clone(),
            action: self.action.clone(),
            performed: self.performed.clone(),
        };
        self.inner.run(&mut intercepted, input).await
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut <TestStore as AtomicStore>::Scope<'t>,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<()>, StoreError>> {
        self.inner.before_resume(scope, key)
    }
    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut <TestStore as AtomicStore>::Scope<'t>,
        key: &'s RequestKey,
        failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>> {
        self.inner.on_failed(scope, key, failure)
    }
}
struct InterceptContext<'a, O> {
    inner: &'a mut O,
    store: Arc<TestStore>,
    action: AfterAnswer,
    performed: Arc<AtomicBool>,
}
impl<O: OperationContext<Store = TestStore>> InterceptContext<'_, O> {
    async fn after_answer(&mut self) -> Result<(), StepError> {
        if matches!(self.action, AfterAnswer::OutcomeCheckpoint { .. }) {
            return Ok(());
        }
        if self.performed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        match &self.action {
            AfterAnswer::OutcomeCheckpoint { .. } => {}
            AfterAnswer::Checkpoint { reached, resume } => {
                reached.notify_one();
                resume.notified().await;
            }
            AfterAnswer::Release => {
                // Separate commits after the durable call answer and before outcome.
                for _ in 0..3 {
                    self.store
                        .transaction(|scope| {
                            Box::pin(async move {
                                Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                                    .release_held::<DeliverTask>(
                                        item_id(1),
                                        SyncTime::from_unix_millis(0),
                                    )
                                    .await
                                    .map_err(storage_error)?;
                                Ok(())
                            })
                        })
                        .await?;
                }
            }
            AfterAnswer::Cancel => {
                self.store
                    .transaction(|scope| {
                        Box::pin(async move {
                            let mut inbox = Inbox::<_, TestTypes>::new(scope, peer(2), limits());
                            let cycle = inbox
                                .begin_receive(DeliveryStream::Controls)
                                .await
                                .map_err(storage_error)?
                                .unwrap();
                            let effects = inbox
                                .store_batch(
                                    cycle,
                                    FetchResult {
                                        stream: DeliveryStream::Controls,
                                        entries: vec![FetchEntry::Item {
                                            seq: DeliverySeq::new(1),
                                            item: cancel(2, 1),
                                        }],
                                        through: DeliverySeq::new(1),
                                    },
                                    &TestConsumer,
                                    SyncTime::from_unix_millis(0),
                                )
                                .await
                                .map_err(storage_error)?;
                            assert!(effects.attempts.is_empty());
                            assert!(effects.reports.is_empty());
                            Ok(())
                        })
                    })
                    .await?;
            }
        }
        Ok(())
    }
}
impl<O: OperationContext<Store = TestStore>> OperationContext for InterceptContext<'_, O> {
    type Store = TestStore;
    fn id(&self) -> &OperationId {
        self.inner.id()
    }
    fn now(&self) -> SyncTime {
        self.inner.now()
    }
    async fn local<T, F>(&mut self, step: StepName, run: F) -> Result<T, StepError>
    where
        T: Serialize + serde::de::DeserializeOwned + Send + 'static,
        F: for<'s, 't> FnOnce(
                &'s mut <TestStore as AtomicStore>::Scope<'t>,
            ) -> BoxFuture<'s, Result<T, StepError>>
            + Send
            + 'static,
    {
        let is_outcome = step.as_str() == "outcome";
        let result = self.inner.local(step, run).await;
        if result.is_ok()
            && is_outcome
            && let AfterAnswer::OutcomeCheckpoint { reached, resume } = &self.action
            && !self.performed.swap(true, Ordering::SeqCst)
        {
            // The real outcome transaction committed, but Engine::finish has
            // not run. A later item attempt can now be admitted independently.
            reached.notify_one();
            resume.notified().await;
        }
        result
    }
    async fn call<C: CallTarget>(
        &mut self,
        step: StepName,
        target: C,
        request: C::Request,
    ) -> Result<C::Answer, StepError> {
        let answer = self.inner.call(step, target, request).await;
        if C::KIND == DeliverCall::<TestTypes>::KIND {
            self.after_answer().await?;
        }
        answer
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
