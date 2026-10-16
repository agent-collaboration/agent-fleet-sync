//! Runtime-independent time, identifier, wake-up and retry capabilities.
//!
//! Wake-ups trigger an owner's work; its durable store owns pending work and
//! deadlines. Native adapters are optional, keeping the default graph portable.

use serde::{Deserialize, Serialize};
use std::{future::Future, time::Duration};
use thiserror::Error;
use uuid::Uuid;

#[cfg(feature = "tokio")]
mod tokio_adapters;
#[cfg(feature = "tokio")]
pub use tokio_adapters::{SystemClock, TokioWakeTimer, UuidV7Ids};

/// UTC wall-clock time in milliseconds since the Unix epoch.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SyncTime(i64);

impl SyncTime {
    pub const fn from_unix_millis(milliseconds: i64) -> Self {
        Self(milliseconds)
    }

    pub const fn unix_millis(self) -> i64 {
        self.0
    }

    /// Saturates at the latest representable time rather than wrapping a deadline.
    pub fn after(self, delay: Duration) -> Self {
        Self(
            self.0
                .saturating_add(i64::try_from(delay.as_millis()).unwrap_or(i64::MAX)),
        )
    }
}

pub trait Clock: Send + Sync {
    fn now(&self) -> SyncTime;
}

pub trait IdSource: Send + Sync {
    /// Mint a UUIDv7 once at the owning boundary.
    fn new_id(&self) -> Uuid;
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum WakeFailure {
    #[error("wake deadline cannot be represented: {at:?}")]
    InvalidDeadline { at: SyncTime },
    #[error("wake scheduler unavailable: {message}")]
    Unavailable { message: String },
}

pub trait WakeScheduler: Send + Sync {
    /// Ensure a wake at or before `at`; never postpone an already earlier wake.
    fn wake_at(&self, at: SyncTime) -> impl Future<Output = Result<(), WakeFailure>> + Send;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Backoff {
    pub initial: Duration,
    pub max: Duration,
    pub factor: u32,
}

impl Backoff {
    pub fn delay(&self, attempt: u32) -> Duration {
        let mut delay = self.initial.min(self.max);
        if attempt == 0 || self.factor == 1 || delay.is_zero() {
            return delay;
        }
        if self.factor == 0 {
            return Duration::ZERO;
        }
        // Multiplying the Duration preserves growth beyond a u32 multiplier.
        // A positive delay reaches the cap within the Duration's bit width.
        let mut remaining = attempt;
        while remaining > 0 && delay < self.max {
            delay = delay.saturating_mul(self.factor).min(self.max);
            remaining -= 1;
        }
        delay
    }
}
