//! The same three-step operation runs in native and Durable Object journeys.
use durable_steps::{
    AtomicStore, CallTarget, OperationContext, OperationFailure, StepError, StepName, StoreError,
    Task,
};
use runtime_primitives::SyncTime;
use std::{future::Future, marker::PhantomData};

#[derive(serde::Serialize, serde::Deserialize)]
pub struct JourneyInput {
    pub value: u64,
    pub not_before: SyncTime,
}

pub trait JourneyScope {
    fn increment_effect(
        &mut self,
        name: &str,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;
}
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RemoteCounter;
impl CallTarget for RemoteCounter {
    const KIND: &'static str = "journey-receiver";
    type Request = u64;
    type Answer = u64;
}
pub struct JourneyTask<S>(pub PhantomData<fn() -> S>);
impl<S: AtomicStore> Task for JourneyTask<S>
where
    for<'t> S::Scope<'t>: JourneyScope,
{
    const KIND: &'static str = "three-step-journey";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = JourneyInput;
    type Output = u64;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        ctx: &mut C,
        input: &JourneyInput,
    ) -> Result<u64, OperationFailure> {
        ctx.local(
            StepName::new("first")
                .map_err(|error| StepError::Storage(StoreError::backend(error)))?,
            |scope| Box::pin(async move { Ok(scope.increment_effect("first").await?) }),
        )
        .await?;
        let answer = ctx
            .call(
                StepName::new("remote")
                    .map_err(|error| StepError::Storage(StoreError::backend(error)))?,
                RemoteCounter,
                input.value,
            )
            .await?;
        let now = ctx.now();
        let not_before = input.not_before;
        ctx.local(
            StepName::new("last")
                .map_err(|error| StepError::Storage(StoreError::backend(error)))?,
            move |scope| {
                Box::pin(async move {
                    if now < not_before {
                        let remaining = not_before.unix_millis().saturating_sub(now.unix_millis());
                        return Err(StepError::Retry {
                            after: std::time::Duration::from_millis(
                                u64::try_from(remaining).map_err(StoreError::backend)?,
                            ),
                        });
                    }
                    Ok(scope.increment_effect("last").await?)
                })
            },
        )
        .await?;
        Ok(answer)
    }
}
