use crate::{SyncTime, WakeFailure, WakeScheduler};
use chrono::{DateTime, Utc};
use std::{future::Future, sync::Arc};
use worker::{
    ScheduledTime, Storage,
    send::{SendFuture, SendWrapper},
};

/// The object's one alarm, shared by every owner hosted in that object.
///
/// Construct once per object and share clones. Deadline-writing callers invoke
/// `wake_at` inside their storage transaction, so the alarm commits or rolls back
/// with the deadline. After an alarm fires, each owner requests its next wake.
#[derive(Clone)]
pub struct AlarmOwner {
    storage: Arc<SendWrapper<Storage>>,
}

impl AlarmOwner {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage: Arc::new(SendWrapper::new(storage)),
        }
    }
}

impl WakeScheduler for AlarmOwner {
    fn wake_at(&self, at: SyncTime) -> impl Future<Output = Result<(), WakeFailure>> + Send {
        SendFuture::new(async move {
            let existing = self.storage.get_alarm().await.map_err(unavailable)?;
            if existing.is_some_and(|current| current <= at.unix_millis()) {
                return Ok(());
            }
            let absolute: DateTime<Utc> = DateTime::from_timestamp_millis(at.unix_millis())
                .ok_or(WakeFailure::InvalidDeadline { at })?;
            // workers-rs interprets an integer as a relative delay. DateTime's
            // conversion selects its absolute Date path, within exact ms range.
            self.storage
                .set_alarm(ScheduledTime::from(absolute))
                .await
                .map_err(unavailable)
        })
    }
}

fn unavailable(error: worker::Error) -> WakeFailure {
    WakeFailure::Unavailable {
        message: error.to_string(),
    }
}
