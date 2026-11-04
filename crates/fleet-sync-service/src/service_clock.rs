use runtime_primitives::{Clock, SyncTime};
use std::sync::Arc;

/// The reference host shares the caller's clock across every owned object.
#[derive(Clone)]
pub struct ServiceClock(pub(crate) Arc<dyn Clock>);
impl Clock for ServiceClock {
    fn now(&self) -> SyncTime {
        self.0.now()
    }
}
