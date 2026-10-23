use crate::{Clock, IdSource, SyncTime, WakeFailure, WakeScheduler};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::{
    sync::{Mutex, Notify},
    time::Instant,
};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SyncTime {
        let milliseconds = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
            Err(error) => i64::try_from(error.duration().as_millis())
                .unwrap_or(i64::MAX)
                .saturating_neg(),
        };
        SyncTime::from_unix_millis(milliseconds)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct UuidV7Ids;

impl IdSource for UuidV7Ids {
    fn new_id(&self) -> Uuid {
        Uuid::now_v7()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingWake {
    at: SyncTime,
    trigger: Instant,
}

/// One owner-consumed timer. `wake_at` never spawns work or creates a runtime.
///
/// The owner awaits `wait`, drives its due work, then requests the next deadline
/// from its own durable store. Dropping a wait leaves the requested wake intact.
pub struct TokioWakeTimer<C: Clock = SystemClock> {
    clock: C,
    pending: Mutex<Option<PendingWake>>,
    changed: Notify,
}

impl<C: Clock> TokioWakeTimer<C> {
    pub fn new(clock: C) -> Self {
        Self {
            clock,
            pending: Mutex::new(None),
            changed: Notify::new(),
        }
    }

    /// Wait for and consume the earliest trigger. One owner calls this method.
    pub async fn wait(&self) -> SyncTime {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            // Register before reading pending state, so a concurrent earlier wake
            // cannot slip between that read and waiting for the notification.
            notified.as_mut().enable();
            let pending = *self.pending.lock().await;
            match pending {
                None => notified.await,
                Some(expected) => {
                    tokio::select! {
                        () = &mut notified => {},
                        () = tokio::time::sleep_until(expected.trigger) => {
                            let mut current = self.pending.lock().await;
                            if *current == Some(expected) {
                                *current = None;
                                return expected.at;
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<C: Clock> WakeScheduler for TokioWakeTimer<C> {
    async fn wake_at(&self, at: SyncTime) -> Result<(), WakeFailure> {
        let mut pending = self.pending.lock().await;
        let at = pending.as_ref().map_or(at, |current| current.at.min(at));
        let delay_ms = u64::try_from(
            at.unix_millis()
                .saturating_sub(self.clock.now().unix_millis()),
        )
        .unwrap_or(0);
        let trigger = Instant::now()
            .checked_add(Duration::from_millis(delay_ms))
            .ok_or(WakeFailure::InvalidDeadline { at })?;
        // The clock can move independently of Tokio time. Recheck even a
        // repeated/later request, while preserving any earlier physical wake.
        let trigger = pending
            .as_ref()
            .map_or(trigger, |current| current.trigger.min(trigger));
        let next = Some(PendingWake { at, trigger });
        if *pending == next {
            return Ok(());
        }
        *pending = next;
        drop(pending);
        self.changed.notify_waiters();
        Ok(())
    }
}
