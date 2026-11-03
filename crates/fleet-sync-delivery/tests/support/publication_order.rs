//! Hold v2 after its journal read and before its first StatusStream append.
use super::{TestHost, TestStore, TestTypes, network::PublishTask};
use durable_steps::{
    BoxFuture, CallResult, CallTarget, OperationContext, OperationFailure, RequestKey, Resume,
    StepError, StepName, StoreError, Task, turso_store::TursoScope,
};
use fleet_sync_delivery::{PublicationRef, PublishCall, SyncTime};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub struct PublicationAppendGate {
    reached: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    armed: AtomicBool,
}
impl PublicationAppendGate {
    pub fn new() -> Self {
        Self {
            reached: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
            armed: AtomicBool::new(true),
        }
    }
    pub async fn wait_until_held(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.reached.notified())
            .await
            .unwrap();
    }
    pub fn resume_append(&self) {
        self.resume.notify_one();
    }
}
pub struct PausePublicationAppend {
    inner: PublishTask,
    gate: Arc<PublicationAppendGate>,
}
impl PausePublicationAppend {
    pub fn new(gate: Arc<PublicationAppendGate>) -> Self {
        Self {
            inner: PublishTask::new(),
            gate,
        }
    }
}
impl Task for PausePublicationAppend {
    const KIND: &'static str = PublishTask::KIND;
    const VERSION: u32 = PublishTask::VERSION;
    const KEEP_RESULT: bool = PublishTask::KEEP_RESULT;
    type Store = TestStore;
    type Input = PublicationRef<String>;
    type Output = ();
    async fn run<O: OperationContext<Store = TestStore>>(
        &self,
        ctx: &mut O,
        input: &Self::Input,
    ) -> Result<(), OperationFailure> {
        self.inner
            .run(
                &mut PausedContext {
                    inner: ctx,
                    gate: self.gate.clone(),
                },
                input,
            )
            .await
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut TursoScope<'t, TestHost>,
        operation: &'s durable_steps::OperationId,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<()>, StoreError>> {
        self.inner.before_resume(scope, operation, key)
    }
    fn retry_not_later_than<'s, 't>(
        &'s self,
        scope: &'s mut TursoScope<'t, TestHost>,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Option<SyncTime>, StoreError>> {
        self.inner.retry_not_later_than(scope, key)
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
struct PausedContext<'a, O> {
    inner: &'a mut O,
    gate: Arc<PublicationAppendGate>,
}
impl<O: OperationContext<Store = TestStore>> OperationContext for PausedContext<'_, O> {
    type Store = TestStore;
    fn id(&self) -> &durable_steps::OperationId {
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
        self.inner.local(step, run).await
    }
    async fn call<C: CallTarget>(
        &mut self,
        step: StepName,
        target: C,
        request: C::Request,
    ) -> Result<C::Answer, StepError> {
        let target_json = serde_json::to_value(&target).map_err(StoreError::backend)?;
        let body = serde_json::to_value(&request).map_err(StoreError::backend)?;
        if C::KIND == PublishCall::<TestTypes>::KIND
            && target_json
                .pointer("/target/kind")
                .and_then(serde_json::Value::as_str)
                == Some("statusStream")
            && body.get("version").and_then(serde_json::Value::as_u64) == Some(2)
            && self.gate.armed.swap(false, Ordering::SeqCst)
        {
            self.gate.reached.notify_one();
            self.gate.resume.notified().await;
        }
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
