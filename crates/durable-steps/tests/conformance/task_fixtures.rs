use super::{BusinessScope, key, step};
use durable_steps::{
    AtomicStore, BoxFuture, CallResult, CallTarget, JournalScope, OperationContext,
    OperationFailure, RequestKey, StepError, StoreError, Task,
};
use std::{
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

pub struct Child<S>(pub PhantomData<fn() -> S>);
impl<S: AtomicStore> Task for Child<S> {
    const KIND: &'static str = "suite-child";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        _ctx: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        Ok(*input)
    }
}
pub struct Parent<S> {
    pub fail_once: Arc<AtomicBool>,
    pub invocations: Arc<AtomicU64>,
    pub store: PhantomData<fn() -> S>,
}
impl<S: AtomicStore> Task for Parent<S>
where
    for<'t> S::Scope<'t>: BusinessScope,
{
    const KIND: &'static str = "suite-parent";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        ctx: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        let value = *input;
        let fail = Arc::clone(&self.fail_once);
        let invocations = Arc::clone(&self.invocations);
        let result = ctx
            .local(step("business-child"), move |scope| {
                invocations.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    let count = scope.read_counter("parent").await?.unwrap_or(0) + 1;
                    scope.write_counter("parent", count).await?;
                    scope
                        .admit::<Child<S>>(&key("parent-child"), &value)
                        .await?;
                    if fail.swap(false, Ordering::SeqCst) {
                        return Err(StepError::Retry {
                            after: Duration::ZERO,
                        });
                    }
                    Ok(count)
                })
            })
            .await?;
        ctx.call(step("wait-answer"), Double::<true>, value).await?;
        Ok(result)
    }
}
pub struct OldVersion<S>(pub PhantomData<fn() -> S>);
impl<S: AtomicStore> Task for OldVersion<S> {
    const KIND: &'static str = "suite-version";
    const VERSION: u32 = 0;
    type Store = S;
    type Input = String;
    type Output = ();
    async fn run<C: OperationContext<Store = S>>(
        &self,
        _ctx: &mut C,
        _input: &String,
    ) -> Result<(), OperationFailure> {
        Ok(())
    }
}
pub struct NewVersion<S> {
    pub fail_hook: Arc<AtomicBool>,
    pub store: PhantomData<fn() -> S>,
}
impl<S: AtomicStore> Task for NewVersion<S>
where
    for<'t> S::Scope<'t>: BusinessScope,
{
    const KIND: &'static str = "suite-version";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = u64;
    type Output = ();
    async fn run<C: OperationContext<Store = S>>(
        &self,
        _ctx: &mut C,
        _input: &u64,
    ) -> Result<(), OperationFailure> {
        Err(OperationFailure::Refused {
            code: "must_not_decode_or_run".into(),
            detail: String::new(),
        })
    }
    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        request: &'s RequestKey,
        failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>> {
        Box::pin(async move {
            super::check(
                request.as_str() == "old-version"
                    && matches!(
                        failure,
                        OperationFailure::VersionMismatch {
                            recorded: 0,
                            registered: 1
                        }
                    ),
                "failure hook receives key and version failure without input decode",
            )?;
            scope.write_counter("domain-failed", 1).await?;
            scope
                .admit::<Child<S>>(&key("owed-child"), &17)
                .await
                .map_err(StoreError::backend)?;
            if self.fail_hook.swap(false, Ordering::SeqCst) {
                return Err(StoreError::Aborted {
                    reason: "hook rollback".into(),
                });
            }
            Ok(())
        })
    }
}
pub struct DeleteAtEnd<S>(pub PhantomData<fn() -> S>);
impl<S: AtomicStore> Task for DeleteAtEnd<S> {
    const KIND: &'static str = "suite-delete";
    const VERSION: u32 = 1;
    const KEEP_RESULT: bool = false;
    type Store = S;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        ctx: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        let value = *input;
        Ok(ctx
            .local(step("save"), move |_| Box::pin(async move { Ok(value) }))
            .await?)
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Double<const DEDUP: bool>;
impl<const DEDUP: bool> CallTarget for Double<DEDUP> {
    const KIND: &'static str = "double";
    const DEDUPLICATES: bool = DEDUP;
    type Request = u64;
    type Answer = u64;
}
pub struct CallTask<S, const BOUND: bool, const DEDUP: bool> {
    pub wait_once: Arc<AtomicBool>,
    pub store: PhantomData<fn() -> S>,
}
impl<S: AtomicStore, const BOUND: bool, const DEDUP: bool> Task for CallTask<S, BOUND, DEDUP> {
    const KIND: &'static str = match (BOUND, DEDUP) {
        (true, true) => "suite-bound",
        (true, false) => "suite-bound-nondup",
        (false, true) => "suite-call",
        (false, false) => "suite-call-nondup",
    };
    const VERSION: u32 = 1;
    type Store = S;
    type Input = u64;
    type Output = CallResult<u64>;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        ctx: &mut C,
        input: &u64,
    ) -> Result<Self::Output, OperationFailure> {
        let result = if BOUND {
            ctx.call_or_give_up(
                step("double"),
                Double::<DEDUP>,
                *input,
                Duration::from_millis(20),
            )
            .await?
        } else {
            CallResult::Answered(ctx.call(step("double"), Double::<DEDUP>, *input).await?)
        };
        let wait = Arc::clone(&self.wait_once);
        ctx.local(step("after-call"), move |_| {
            Box::pin(async move {
                if wait.swap(false, Ordering::SeqCst) {
                    Err(StepError::Retry {
                        after: Duration::ZERO,
                    })
                } else {
                    Ok(())
                }
            })
        })
        .await?;
        Ok(result)
    }
}
