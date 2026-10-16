//! Force replacement after claim commits but before the resume transaction.
//! Scheduling is controlled; both claims and the hook use the real store.
use super::{BusinessScope, check, key, resume_cases::Resumable};
use crate::support::TestHost;
use durable_steps::{
    AtomicStore, BoxFuture, DriveLease, Engine, JournalScope, OperationId, StoreError, Uuid,
    storage::{JournalStorage, OperationExecution},
};
use runtime_primitives::{Clock, IdSource};
use std::{
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

struct ReplacedStore<S> {
    inner: Arc<S>,
    host: Arc<TestHost>,
    target: OperationId,
    replacement: Uuid,
    armed: AtomicBool,
}
impl<S: AtomicStore> AtomicStore for ReplacedStore<S> {
    type Scope<'t> = S::Scope<'t>;
    async fn transaction<T, F>(&self, run: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: for<'s, 't> FnOnce(&'s mut Self::Scope<'t>) -> BoxFuture<'s, Result<T, StoreError>>
            + Send
            + 'static,
    {
        let output = self.inner.transaction(run).await?;
        if self.armed.load(Ordering::SeqCst) {
            let id = self.target;
            let lease = self
                .inner
                .transaction(move |scope| {
                    Box::pin(async move {
                        Ok(match scope.operation(id).await?.map(|r| r.execution) {
                            Some(OperationExecution::Running { drive, .. }) => drive,
                            _ => None,
                        })
                    })
                })
                .await?;
            if let Some(lease) = lease
                && self.armed.swap(false, Ordering::SeqCst)
            {
                self.host
                    .clock
                    .store(lease.until.unix_millis(), Ordering::SeqCst);
                let now = self.host.now();
                let replacement = self.replacement;
                self.inner
                    .transaction(move |scope| {
                        Box::pin(async move {
                            check(
                                scope
                                    .claim(
                                        id,
                                        now,
                                        DriveLease {
                                            token: replacement,
                                            until: now.after(Duration::from_secs(30)),
                                        },
                                    )
                                    .await?
                                    .is_some(),
                                "controlled competing drive replaced the expired claim",
                            )?;
                            if let Some(at) = scope.earliest().await? {
                                scope.ensure_wake(at).await?;
                            }
                            Ok(())
                        })
                    })
                    .await?;
            }
        }
        Ok(output)
    }
}

pub async fn stale_hook_is_not_invoked<S>(
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
                    .admit::<Resumable<ReplacedStore<S>, true>>(&key("resume-stale"), &7)
                    .await
                    .map_err(StoreError::backend)?
                    .id())
            })
        })
        .await?;
    let store = Arc::new(ReplacedStore {
        inner: Arc::clone(&inner),
        host: Arc::clone(&host),
        target: id,
        replacement: host.new_id(),
        armed: AtomicBool::new(true),
    });
    let hooks = Arc::new(AtomicU64::new(0));
    let runs = Arc::new(AtomicU64::new(0));
    let mut engine = Engine::new(store, host);
    engine.register(Resumable::<ReplacedStore<S>, true> {
        hooks: Arc::clone(&hooks),
        runs: Arc::clone(&runs),
        store: PhantomData,
    })?;
    let report = engine.drive_due().await?;
    check(
        report.yielded.contains(&id),
        "replaced drive yields before its hook",
    )?;
    check(
        hooks.load(Ordering::SeqCst) == 0 && runs.load(Ordering::SeqCst) == 0,
        "stale drive invokes neither the hook nor the task",
    )?;
    inner
        .transaction(move |scope| {
            Box::pin(async move {
                check(
                    scope.step_count(id).await? == 0,
                    "stale drive saves no step",
                )?;
                check(
                    matches!(
                        scope.operation(id).await?.map(|r| r.execution),
                        Some(OperationExecution::Running { .. })
                    ),
                    "replacement retains the running operation",
                )
            })
        })
        .await
}
