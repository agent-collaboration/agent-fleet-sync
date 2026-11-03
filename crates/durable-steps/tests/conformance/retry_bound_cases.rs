//! Real scheduling on each conformance backend, with an injected clock.
use super::{check, key, step};
use crate::support::TestHost;
use durable_steps::{
    AtomicStore, BoxFuture, Engine, OperationContext, OperationFailure, OperationState, RequestKey,
    StepError, StoreError, SyncTime, Task,
};
use runtime_primitives::Clock;
use std::{marker::PhantomData, sync::Arc, time::Duration};

struct DefaultRetry<S, const STORAGE: bool>(PhantomData<fn() -> S>);
impl<S: AtomicStore, const STORAGE: bool> Task for DefaultRetry<S, STORAGE> {
    const KIND: &'static str = if STORAGE {
        "retry-bound.default-storage"
    } else {
        "retry-bound.default-retry"
    };
    const VERSION: u32 = 1;
    type Store = S;
    type Input = SyncTime;
    type Output = ();
    async fn run<O: OperationContext<Store = S>>(
        &self,
        ctx: &mut O,
        until: &SyncTime,
    ) -> Result<(), OperationFailure> {
        let now = ctx.now();
        let until = *until;
        ctx.local(step("wait"), move |_| {
            Box::pin(async move {
                if now < until {
                    if STORAGE {
                        return Err(StepError::Storage(StoreError::backend(
                            "injected retryable storage failure",
                        )));
                    }
                    return Err(StepError::Retry {
                        after: Duration::ZERO,
                    });
                }
                Ok(())
            })
        })
        .await?;
        Ok(())
    }
}
struct BoundedRetry<S, const STORAGE: bool> {
    bound: SyncTime,
    store: PhantomData<fn() -> S>,
}
impl<S: AtomicStore, const STORAGE: bool> Task for BoundedRetry<S, STORAGE> {
    const KIND: &'static str = if STORAGE {
        "retry-bound.bounded-storage"
    } else {
        "retry-bound.bounded-retry"
    };
    const VERSION: u32 = 1;
    type Store = S;
    type Input = SyncTime;
    type Output = ();
    async fn run<O: OperationContext<Store = S>>(
        &self,
        ctx: &mut O,
        input: &SyncTime,
    ) -> Result<(), OperationFailure> {
        DefaultRetry::<S, STORAGE>(PhantomData)
            .run(ctx, input)
            .await
    }
    fn retry_not_later_than<'s, 't>(
        &'s self,
        _scope: &'s mut S::Scope<'t>,
        _key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Option<SyncTime>, StoreError>> {
        Box::pin(async move { Ok(Some(self.bound)) })
    }
}
async fn case<S: AtomicStore, const STORAGE: bool>(
    store: Arc<S>,
    host: Arc<TestHost>,
    past: bool,
) -> Result<(), StoreError> {
    let now = host.now();
    let until = SyncTime::from_unix_millis(now.unix_millis() + 3);
    let bound = if past {
        SyncTime::from_unix_millis(now.unix_millis() - 1)
    } else {
        until
    };
    let mut engine = Engine::new(store, host.clone());
    engine.register(DefaultRetry::<S, STORAGE>(PhantomData))?;
    engine.register(BoundedRetry::<S, STORAGE> {
        bound,
        store: PhantomData,
    })?;
    let default = engine
        .start::<DefaultRetry<S, STORAGE>>(
            &key(&format!("retry-bound-default-{STORAGE}-{past}")),
            until,
        )
        .await
        .map_err(StoreError::backend)?;
    let bounded = engine
        .start::<BoundedRetry<S, STORAGE>>(
            &key(&format!("retry-bound-limited-{STORAGE}-{past}")),
            until,
        )
        .await
        .map_err(StoreError::backend)?;
    check(
        matches!(engine.status(&default).await?.state,OperationState::Running {next_attempt_at,..} if next_attempt_at.unix_millis()==now.unix_millis()+10),
        "default Task hook must retain the established 10ms initial backoff",
    )?;
    check(
        matches!(engine.status(&bounded).await?.state,OperationState::Running {next_attempt_at,..} if next_attempt_at==bound.max(now)),
        "retry must be due at the domain bound, clamped to now",
    )?;
    host.advance(3);
    let due = engine.drive_due().await?;
    check(
        due.completed == vec![bounded] && due.failed.is_empty(),
        "bounded work must complete before default backoff is due",
    )?;
    host.advance(7);
    let due = engine.drive_due().await?;
    check(
        due.completed == vec![default] && due.failed.is_empty(),
        "default work must complete at its unchanged backoff",
    )
}
pub async fn retry_bounds<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError> {
    for past in [false, true] {
        case::<S, false>(store.clone(), host.clone(), past).await?;
        case::<S, true>(store.clone(), host.clone(), past).await?;
    }
    Ok(())
}
