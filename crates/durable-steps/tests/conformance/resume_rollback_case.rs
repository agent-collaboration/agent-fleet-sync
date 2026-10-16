//! Inject a commit failure after Finish has changed the real transaction.
use super::{BusinessScope, check, key, resume_cases::Resumable};
use crate::support::TestHost;
use durable_steps::{
    AtomicStore, BoxFuture, Engine, JournalScope, OperationId, StoreError,
    storage::{JournalStorage, OperationExecution},
};
use std::{
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

struct FinishRollbackStore<S> {
    inner: Arc<S>,
    target: OperationId,
    fail_finish: Arc<AtomicBool>,
}
impl<S: AtomicStore> AtomicStore for FinishRollbackStore<S> {
    type Scope<'t> = S::Scope<'t>;
    async fn transaction<T, F>(&self, run: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: for<'s, 't> FnOnce(&'s mut Self::Scope<'t>) -> BoxFuture<'s, Result<T, StoreError>>
            + Send
            + 'static,
    {
        let id = self.target;
        let fail_finish = Arc::clone(&self.fail_finish);
        self.inner
            .transaction(move |scope| {
                Box::pin(async move {
                    let output = run(scope).await?;
                    if fail_finish.load(Ordering::SeqCst)
                        && matches!(
                            scope.operation(id).await?.map(|row| row.execution),
                            Some(OperationExecution::Completed { .. })
                        )
                        && fail_finish.swap(false, Ordering::SeqCst)
                    {
                        return Err(StoreError::Backend {
                            message: "injected Finish commit failure".into(),
                        });
                    }
                    Ok(output)
                })
            })
            .await
    }
}

pub async fn finish_rolls_back_with_hook_writes<S>(
    inner: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let id = inner
        .transaction(|scope| {
            Box::pin(async move {
                Ok(scope
                    .admit::<Resumable<FinishRollbackStore<S>, true>>(&key("resume-rollback"), &7)
                    .await
                    .map_err(StoreError::backend)?
                    .id())
            })
        })
        .await?;
    let fail_finish = Arc::new(AtomicBool::new(false));
    let store = Arc::new(FinishRollbackStore {
        inner: Arc::clone(&inner),
        target: id,
        fail_finish: Arc::clone(&fail_finish),
    });
    let hooks = Arc::new(AtomicU64::new(0));
    let runs = Arc::new(AtomicU64::new(0));
    let mut engine = Engine::new(store, Arc::clone(&host));
    engine.register(Resumable::<FinishRollbackStore<S>, true> {
        hooks: Arc::clone(&hooks),
        runs: Arc::clone(&runs),
        store: PhantomData,
    })?;
    host.retry_calls.store(true, Ordering::SeqCst);
    engine.drive_due().await?;
    let deliveries = host.deliveries.load(Ordering::SeqCst);
    inner
        .transaction(move |scope| {
            Box::pin(async move {
                check(
                    scope.step_count(id).await? == 2,
                    "rollback fixture has an unanswered call",
                )?;
                scope.write_counter("stop:resume-rollback", 73).await
            })
        })
        .await?;
    fail_finish.store(true, Ordering::SeqCst);
    host.advance(11);
    let report = engine.drive_due().await?;
    check(
        report.retried.contains(&id) && !fail_finish.load(Ordering::SeqCst),
        "injected commit failure is reached and scheduled for retry",
    )?;
    inner
        .transaction(move |scope| {
            Box::pin(async move {
                check(
                    scope
                        .read_counter("hook-finished:resume-rollback")
                        .await?
                        .is_none(),
                    "hook business write rolls back when Finish cannot commit",
                )?;
                check(
                    scope.step_count(id).await? == 2,
                    "Finish rollback restores the pending call and saved local step",
                )?;
                check(
                    matches!(
                        scope.operation(id).await?.map(|row| row.execution),
                        Some(OperationExecution::Running { .. })
                    ),
                    "Finish rollback leaves the operation Running",
                )
            })
        })
        .await?;
    check(
        host.deliveries.load(Ordering::SeqCst) == deliveries && runs.load(Ordering::SeqCst) == 1,
        "failed Finish does not resume or redispatch",
    )?;
    host.advance(21);
    let report = engine.drive_due().await?;
    check(
        report.completed.contains(&id) && hooks.load(Ordering::SeqCst) == 3,
        "successful retry re-reads domain state and finishes",
    )?;
    check(
        host.deliveries.load(Ordering::SeqCst) == deliveries && runs.load(Ordering::SeqCst) == 1,
        "successful retry still avoids task replay and dispatch",
    )?;
    host.retry_calls.store(false, Ordering::SeqCst);
    inner
        .transaction(move |scope| {
            Box::pin(async move {
                check(
                    scope.read_counter("hook-finished:resume-rollback").await? == Some(1),
                    "successful Finish commits its business write",
                )?;
                check(
                    scope.step_count(id).await? == 0,
                    "successful Finish compacts history",
                )?;
                check(
                    matches!(scope.operation(id).await?.map(|row| row.execution),
            Some(OperationExecution::Completed { result, .. }) if result == b"73"),
                    "successful Finish retains the supplied output",
                )
            })
        })
        .await
}
