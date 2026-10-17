use super::*;
use durable_steps::{
    BoxFuture, CallResult, CallTarget, OperationContext, OperationFailure, RequestKey, Resume,
    StepError, Task,
};
use serde::de::DeserializeOwned;
use std::time::Duration;

/// Observation wraps real task/context calls and never changes their values, keys or ownership.
pub(super) struct ObservedTask<T> {
    inner: T,
    changed: Arc<Notify>,
    gate: Arc<FaultGate>,
}
impl<T> ObservedTask<T> {
    pub fn new(inner: T, state: &NodeState) -> Self {
        Self {
            inner,
            changed: state.changed.clone(),
            gate: state.gate.clone(),
        }
    }
}
impl<T: Task<Store = NodeStore>> Task for ObservedTask<T> {
    const KIND: &'static str = T::KIND;
    const VERSION: u32 = T::VERSION;
    const KEEP_RESULT: bool = T::KEEP_RESULT;
    type Store = NodeStore;
    type Input = T::Input;
    type Output = T::Output;
    async fn run<O: OperationContext<Store = NodeStore>>(
        &self,
        ctx: &mut O,
        input: &T::Input,
    ) -> Result<T::Output, OperationFailure> {
        let mut observed = ObservedContext {
            inner: ctx,
            changed: &self.changed,
            gate: &self.gate,
        };
        let result = self.inner.run(&mut observed, input).await;
        self.changed.notify_waiters();
        result
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut <NodeStore as AtomicStore>::Scope<'t>,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<T::Output>, StoreError>> {
        self.inner.before_resume(scope, key)
    }
    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut <NodeStore as AtomicStore>::Scope<'t>,
        key: &'s RequestKey,
        failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>> {
        Box::pin(async move {
            let result = self.inner.on_failed(scope, key, failure).await;
            // Readers serialize through the object's transaction lock and see only the commit.
            self.changed.notify_waiters();
            result
        })
    }
}
struct ObservedContext<'a, O> {
    inner: &'a mut O,
    changed: &'a Notify,
    gate: &'a FaultGate,
}
impl<O: OperationContext<Store = NodeStore>> OperationContext for ObservedContext<'_, O> {
    type Store = NodeStore;
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
                &'s mut <NodeStore as AtomicStore>::Scope<'t>,
            ) -> BoxFuture<'s, Result<T, StepError>>
            + Send
            + 'static,
    {
        let result = self.inner.local(step, run).await;
        self.changed.notify_waiters();
        result
    }
    async fn call<C: CallTarget>(
        &mut self,
        step: StepName,
        target: C,
        request: C::Request,
    ) -> Result<C::Answer, StepError> {
        let result = self.inner.call(step, target, request).await;
        if C::KIND == DeliverCall::<SampleDelivery>::KIND && result.is_ok() {
            self.gate.after("checkpoint").await;
        }
        self.changed.notify_waiters();
        result
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
