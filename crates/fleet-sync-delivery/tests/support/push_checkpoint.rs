//! Pause after the engine commits a push answer and before applying its receipt.
use super::{network::PushTask, *};
use durable_steps::{
    BoxFuture, CallResult, CallTarget, OperationContext, OperationFailure, RequestKey, Resume,
    StepError, StepName, StoreError, Task, turso_store::TursoScope,
};
use std::time::Duration;

pub struct PausedPush {
    pub inner: PushTask,
    pub reached: Arc<tokio::sync::Notify>,
}
impl Task for PausedPush {
    const KIND: &'static str = PushTask::KIND;
    const VERSION: u32 = PushTask::VERSION;
    const KEEP_RESULT: bool = PushTask::KEEP_RESULT;
    type Store = TestStore;
    type Input = PushInput;
    type Output = ();
    async fn run<O: OperationContext<Store = TestStore>>(
        &self,
        ctx: &mut O,
        input: &PushInput,
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
    fn retry_not_later_than<'s, 't>(
        &'s self,
        scope: &'s mut TursoScope<'t, TestHost>,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Option<durable_steps::SyncTime>, StoreError>> {
        self.inner.retry_not_later_than(scope, key)
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut TursoScope<'t, TestHost>,
        operation: &'s durable_steps::OperationId,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<()>, StoreError>> {
        self.inner.before_resume(scope, operation, key)
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
        if step.as_str() == "receipt" {
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
