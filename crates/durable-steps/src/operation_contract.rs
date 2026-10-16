use crate::{
    Admitted, CallFailure, CallId, CallResult, OperationFailure, OperationId, RequestKey,
    StartRefusal, StepError, StepName, StoreError, SyncTime, storage::JournalStorage,
};
use runtime_primitives::{Clock, IdSource, WakeScheduler};
use serde::{Serialize, de::DeserializeOwned};
use std::{future::Future, pin::Pin, time::Duration};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait Task: Send + Sync + 'static {
    const KIND: &'static str;
    const VERSION: u32;
    const KEEP_RESULT: bool = true;
    type Store: AtomicStore;
    type Input: Serialize + DeserializeOwned + Send + Sync;
    type Output: Serialize + DeserializeOwned + Send;
    fn run<C: OperationContext<Store = Self::Store>>(
        &self,
        ctx: &mut C,
        input: &Self::Input,
    ) -> impl Future<Output = Result<Self::Output, OperationFailure>> + Send;
    fn on_failed<'s, 't>(
        &'s self,
        _scope: &'s mut <Self::Store as AtomicStore>::Scope<'t>,
        _request_key: &'s RequestKey,
        _failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }
}

pub trait OperationContext: Send {
    type Store: AtomicStore;
    fn id(&self) -> &OperationId;
    /// Read before a step and move the value into its callback. Never branch on
    /// this reading outside a step; replay returns the saved output instead.
    fn now(&self) -> SyncTime;
    fn local<T, F>(
        &mut self,
        step: StepName,
        run: F,
    ) -> impl Future<Output = Result<T, StepError>> + Send
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: for<'s, 't> FnOnce(
                &'s mut <Self::Store as AtomicStore>::Scope<'t>,
            ) -> BoxFuture<'s, Result<T, StepError>>
            + Send
            + 'static;
    fn call<C: CallTarget>(
        &mut self,
        step: StepName,
        target: C,
        request: C::Request,
    ) -> impl Future<Output = Result<C::Answer, StepError>> + Send;
    fn call_or_give_up<C: CallTarget>(
        &mut self,
        step: StepName,
        target: C,
        request: C::Request,
        unsent_for: Duration,
    ) -> impl Future<Output = Result<CallResult<C::Answer>, StepError>> + Send;
}

pub trait AtomicStore: Send + Sync + 'static {
    type Scope<'t>: JournalScope + JournalStorage + Send;
    fn transaction<T, F>(&self, run: F) -> impl Future<Output = Result<T, StoreError>> + Send
    where
        T: Send + 'static,
        F: for<'s, 't> FnOnce(&'s mut Self::Scope<'t>) -> BoxFuture<'s, Result<T, StoreError>>
            + Send
            + 'static;
}

pub trait JournalScope: Send {
    fn admit<T: Task>(
        &mut self,
        request_key: &RequestKey,
        input: &T::Input,
    ) -> impl Future<Output = Result<Admitted, StartRefusal>> + Send;
    fn ensure_wake(&mut self, at: SyncTime) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// A typed boundary call, routed by the host from its serialized value.
pub trait CallTarget: Serialize + DeserializeOwned + Send + Sync + 'static {
    const KIND: &'static str;
    const DEDUPLICATES: bool = true;
    type Request: Serialize + DeserializeOwned + Send;
    type Answer: Serialize + DeserializeOwned + Send;
}

pub trait Host: Clock + IdSource + WakeScheduler + Send + Sync + 'static {
    fn dispatch<C: CallTarget>(
        &self,
        target: C,
        request: C::Request,
        call: CallId,
    ) -> impl Future<Output = Result<C::Answer, CallFailure>> + Send;
}
