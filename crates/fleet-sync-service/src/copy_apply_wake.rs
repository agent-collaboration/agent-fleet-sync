//! Fault only the committed-apply hint, while delegating to the real service host.
use crate::ServiceHost;
use runtime_primitives::{SyncTime, WakeFailure, WakeScheduler};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

pub(crate) struct CopyApplyWake {
    host: Arc<ServiceHost>,
    fail_next: AtomicBool,
    failures: AtomicU64,
}
impl CopyApplyWake {
    pub fn new(host: Arc<ServiceHost>) -> Self {
        Self {
            host,
            fail_next: false.into(),
            failures: 0.into(),
        }
    }
    pub fn arm(&self) {
        self.fail_next.store(true, Ordering::SeqCst);
    }
    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::SeqCst)
    }
}
impl WakeScheduler for CopyApplyWake {
    async fn wake_at(&self, at: SyncTime) -> Result<(), WakeFailure> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            self.failures.fetch_add(1, Ordering::SeqCst);
            return Err(WakeFailure::Unavailable {
                message: "injected post-apply wake failure".into(),
            });
        }
        self.host.wake_at(at).await
    }
}
