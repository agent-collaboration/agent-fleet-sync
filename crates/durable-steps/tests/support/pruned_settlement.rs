use durable_steps::{
    AtomicStore, BoxFuture, CallFailure, CallId, CallTarget, Host, OperationId, StoreError,
    SyncTime, Uuid, WakeFailure, storage::JournalStorage,
};
use futures_util::lock::Mutex;
use runtime_primitives::{Clock, IdSource, WakeScheduler};
use std::sync::{
    Arc, Mutex as StateMutex,
    atomic::{AtomicBool, Ordering},
};

pub struct WakeBarrier {
    pub hold: Mutex<()>,
    pub entered: AtomicBool,
    once: AtomicBool,
}
impl WakeBarrier {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            hold: Mutex::new(()),
            entered: AtomicBool::new(false),
            once: AtomicBool::new(true),
        })
    }
}

pub struct SettlementHost {
    pub base: Arc<crate::support::TestHost>,
    pub wake: StateMutex<Option<Arc<WakeBarrier>>>,
}
impl SettlementHost {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            base: crate::support::TestHost::new(),
            wake: StateMutex::new(None),
        })
    }
}
impl Clock for SettlementHost {
    fn now(&self) -> SyncTime {
        self.base.now()
    }
}
impl IdSource for SettlementHost {
    fn new_id(&self) -> Uuid {
        self.base.new_id()
    }
}
impl Host for SettlementHost {
    async fn dispatch<C: CallTarget>(
        &self,
        target: C,
        request: C::Request,
        call: CallId,
    ) -> Result<C::Answer, CallFailure> {
        self.base.dispatch(target, request, call).await
    }
}
impl WakeScheduler for SettlementHost {
    async fn wake_at(&self, at: SyncTime) -> Result<(), WakeFailure> {
        self.base.wake_at(at).await?;
        let barrier = self
            .wake
            .lock()
            .map_err(|error| WakeFailure::Unavailable {
                message: error.to_string(),
            })?
            .clone();
        if let Some(barrier) = barrier
            && barrier.once.swap(false, Ordering::SeqCst)
        {
            barrier.entered.store(true, Ordering::SeqCst);
            let _released = barrier.hold.lock().await;
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct StoreFaults {
    pub rollback_next: AtomicBool,
    pub tracked: StateMutex<Option<OperationId>>,
    pub fail_after_prune: AtomicBool,
    fail_next: AtomicBool,
}

pub struct FaultStore<S> {
    pub inner: Arc<S>,
    pub faults: Arc<StoreFaults>,
}
impl<S> FaultStore<S> {
    pub fn new(inner: Arc<S>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            faults: Arc::new(StoreFaults::default()),
        })
    }
}
impl<S: AtomicStore> AtomicStore for FaultStore<S> {
    type Scope<'transaction> = S::Scope<'transaction>;

    async fn transaction<T, F>(&self, run: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: for<'scope, 'transaction> FnOnce(
                &'scope mut Self::Scope<'transaction>,
            )
                -> BoxFuture<'scope, Result<T, StoreError>>
            + Send
            + 'static,
    {
        if self.faults.fail_next.swap(false, Ordering::SeqCst) {
            return Err(StoreError::Backend {
                message: "post-commit rearm".into(),
            });
        }
        let faults = Arc::clone(&self.faults);
        let (output, pruned) = self
            .inner
            .transaction(move |scope| {
                Box::pin(async move {
                    let output = run(scope).await?;
                    if faults.rollback_next.swap(false, Ordering::SeqCst) {
                        return Err(StoreError::Aborted {
                            reason: "rollback after admission attachment".into(),
                        });
                    }
                    let tracked = *faults.tracked.lock().map_err(StoreError::backend)?;
                    let pruned = if faults.fail_after_prune.load(Ordering::SeqCst) {
                        if let Some(id) = tracked {
                            scope.operation(id).await?.is_none()
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    Ok((output, pruned))
                })
            })
            .await?;
        // Fault the next scheduling read only after the real prune committed.
        if pruned && self.faults.fail_after_prune.swap(false, Ordering::SeqCst) {
            self.faults.fail_next.store(true, Ordering::SeqCst);
        }
        Ok(output)
    }
}
