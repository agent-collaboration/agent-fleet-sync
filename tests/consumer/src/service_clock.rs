use runtime_primitives::{Clock, SyncTime, SystemClock};
use std::sync::atomic::{AtomicI64, Ordering};

pub(crate) struct FixtureClock {
    injected: Option<AtomicI64>,
}
impl FixtureClock {
    pub(crate) fn from_environment() -> crate::TestResult<Self> {
        Ok(Self {
            injected: std::env::var("FLEET_SYNC_TEST_CLOCK")
                .ok()
                .map(|value| value.parse::<i64>())
                .transpose()?
                .map(AtomicI64::new),
        })
    }
    #[cfg(feature = "test-support")]
    pub(crate) fn advance(&self, to: SyncTime) -> crate::TestResult {
        let clock = self
            .injected
            .as_ref()
            .ok_or("service is using SystemClock")?;
        if to.unix_millis() < clock.load(Ordering::SeqCst) {
            return Err("clock must advance".into());
        }
        clock.store(to.unix_millis(), Ordering::SeqCst);
        Ok(())
    }
}
impl Clock for FixtureClock {
    fn now(&self) -> SyncTime {
        self.injected.as_ref().map_or_else(
            || SystemClock.now(),
            |clock| SyncTime::from_unix_millis(clock.load(Ordering::SeqCst)),
        )
    }
}
