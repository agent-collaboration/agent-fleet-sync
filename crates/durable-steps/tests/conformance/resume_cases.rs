use super::{BusinessScope, check, key, step, task_fixtures::Double};
use crate::support::TestHost;
use durable_steps::{
    AtomicStore, BoxFuture, Engine, OperationContext, OperationFailure, OperationId,
    OperationState, RequestKey, Resume, StoreError, Task,
    storage::{JournalStorage, OperationExecution},
};
use std::{
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub(super) struct Resumable<S, const KEEP: bool> {
    pub hooks: Arc<AtomicU64>,
    pub runs: Arc<AtomicU64>,
    pub store: PhantomData<fn() -> S>,
}
impl<S: AtomicStore, const KEEP: bool> Task for Resumable<S, KEEP>
where
    for<'t> S::Scope<'t>: BusinessScope,
{
    const KIND: &'static str = if KEEP {
        "resume-kept"
    } else {
        "resume-deleted"
    };
    const VERSION: u32 = 1;
    const KEEP_RESULT: bool = KEEP;
    type Store = S;
    type Input = u64;
    type Output = u64;

    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        request: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<u64>, StoreError>> {
        // Synchronous invocation is observable even if a later token failure
        // rolls back SQL writes or the returned future is never polled.
        self.hooks.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let stop_key = format!("stop:{}", request.as_str());
            if let Some(output) = scope.read_counter(&stop_key).await? {
                scope
                    .write_counter(&format!("hook-finished:{}", request.as_str()), 1)
                    .await?;
                Ok(Resume::Finish(output))
            } else {
                Ok(Resume::Continue)
            }
        })
    }
    async fn run<C: OperationContext<Store = S>>(
        &self,
        ctx: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        let first = effect_key("first", *ctx.id());
        ctx.local(step("resume-first"), move |scope| {
            Box::pin(async move {
                let next = scope.read_counter(&first).await?.unwrap_or(0) + 1;
                scope.write_counter(&first, next).await?;
                Ok(next)
            })
        })
        .await?;
        let answer = ctx
            .call(step("resume-call"), Double::<true>, *input)
            .await?;
        let last = effect_key("last", *ctx.id());
        ctx.local(step("resume-last"), move |scope| {
            Box::pin(async move {
                let next = scope.read_counter(&last).await?.unwrap_or(0) + 1;
                scope.write_counter(&last, next).await?;
                Ok(next)
            })
        })
        .await?;
        Ok(answer)
    }
}
fn effect_key(stage: &str, id: OperationId) -> String {
    format!("resume-{stage}:{}", id.0)
}
fn engine<S: AtomicStore, const KEEP: bool>(
    store: Arc<S>,
    host: Arc<TestHost>,
    hooks: Arc<AtomicU64>,
    runs: Arc<AtomicU64>,
) -> Result<Engine<S, TestHost>, StoreError>
where
    for<'t> S::Scope<'t>: BusinessScope,
{
    let mut engine = Engine::new(store, host);
    engine.register(Resumable::<S, KEEP> {
        hooks,
        runs,
        store: PhantomData,
    })?;
    Ok(engine)
}

pub async fn finish_unanswered<S>(store: Arc<S>, host: Arc<TestHost>) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    finish_one::<S, true>(Arc::clone(&store), Arc::clone(&host), "resume-finish-kept").await?;
    finish_one::<S, false>(store, host, "resume-finish-deleted").await
}
async fn finish_one<S: AtomicStore, const KEEP: bool>(
    store: Arc<S>,
    host: Arc<TestHost>,
    request: &'static str,
) -> Result<(), StoreError>
where
    for<'t> S::Scope<'t>: BusinessScope,
{
    let hooks = Arc::new(AtomicU64::new(0));
    let runs = Arc::new(AtomicU64::new(0));
    let first = engine::<S, KEEP>(
        Arc::clone(&store),
        Arc::clone(&host),
        Arc::clone(&hooks),
        Arc::clone(&runs),
    )?;
    host.retry_calls.store(true, Ordering::SeqCst);
    let id = first
        .start::<Resumable<S, KEEP>>(&key(request), 7)
        .await
        .map_err(StoreError::backend)?;
    let deliveries = host.deliveries.load(Ordering::SeqCst);
    store
        .transaction(move |scope| {
            Box::pin(async move {
                check(
                    scope.step_count(id).await? == 2,
                    "unanswered call retains first output and call intent",
                )?;
                scope.write_counter(&format!("stop:{request}"), 73).await
            })
        })
        .await?;
    // A new Engine is the restart/alarm caller; the hook's domain read must
    // happen before replay can dispatch the unanswered call again.
    let resumed = engine::<S, KEEP>(
        Arc::clone(&store),
        Arc::clone(&host),
        Arc::clone(&hooks),
        Arc::clone(&runs),
    )?;
    host.advance(11);
    let report = resumed.drive_due().await?;
    check(
        report.completed.contains(&id),
        "Resume::Finish must complete the unanswered operation",
    )?;
    check(
        host.deliveries.load(Ordering::SeqCst) == deliveries && runs.load(Ordering::SeqCst) == 1,
        "Finish precedes all task replay and further dispatch",
    )?;
    check(
        hooks.load(Ordering::SeqCst) == 2,
        "hook runs once on each claimed drive",
    )?;
    store.transaction(move |scope| Box::pin(async move {
        check(scope.read_counter(&effect_key("first", id)).await? == Some(1), "first local effect stays once")?;
        check(scope.read_counter(&effect_key("last", id)).await?.is_none(), "Finish skips the later local step")?;
        check(scope.read_counter(&format!("hook-finished:{request}")).await? == Some(1), "hook domain writes commit with completion")?;
        check(scope.step_count(id).await? == 0, "hook completion deletes step history")?;
        match scope.operation(id).await? {
            Some(row) if KEEP => check(matches!(row.execution, OperationExecution::Completed { result, .. } if result == b"73"), "hook completion keeps the normal compact output"),
            None if !KEEP => Ok(()),
            _ => check(false, "hook completion obeys KEEP_RESULT"),
        }
    })).await?;
    host.retry_calls.store(false, Ordering::SeqCst);
    Ok(())
}

pub async fn continue_replays<S>(store: Arc<S>, host: Arc<TestHost>) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let hooks = Arc::new(AtomicU64::new(0));
    let runs = Arc::new(AtomicU64::new(0));
    let engine = engine::<S, true>(
        Arc::clone(&store),
        Arc::clone(&host),
        Arc::clone(&hooks),
        Arc::clone(&runs),
    )?;
    host.retry_calls.store(true, Ordering::SeqCst);
    let id = engine
        .start::<Resumable<S, true>>(&key("resume-continue"), 7)
        .await
        .map_err(StoreError::backend)?;
    host.retry_calls.store(false, Ordering::SeqCst);
    host.advance(11);
    engine.drive_due().await?;
    check(
        matches!(engine.status(&id).await?.state, OperationState::Completed { result } if result == b"14"),
        "Continue preserves the task's own output",
    )?;
    check(
        hooks.load(Ordering::SeqCst) == 2 && runs.load(Ordering::SeqCst) == 2,
        "Continue resumes normal execution",
    )?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                check(
                    scope.read_counter(&effect_key("first", id)).await? == Some(1),
                    "Continue still replays saved local output",
                )?;
                check(
                    scope.read_counter(&effect_key("last", id)).await? == Some(1),
                    "Continue executes the unfinished local step",
                )
            })
        })
        .await
}
