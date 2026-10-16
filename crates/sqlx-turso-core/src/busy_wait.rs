//! Lock waits belong to Tokio; the native binding receives a zero busy timeout.
use crate::error::map_turso_error;
use sqlx_core::error::Error;
use std::time::Duration;
use tokio::time::Instant;

pub(crate) struct BusyWait {
    timeout: Duration,
    state: BusyWaitState,
}

enum BusyWaitState {
    NotWaiting,
    Waiting { started: Instant, delay: Duration },
}

impl BusyWait {
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            state: BusyWaitState::NotWaiting,
        }
    }

    pub(crate) async fn wait_after_error(&mut self, error: turso::Error) -> Result<(), Error> {
        // A busy snapshot cannot become writable by waiting; its transaction must
        // be reconsidered by the caller, never silently replayed by this driver.
        if !matches!(error, turso::Error::Busy(_)) {
            return Err(map_turso_error(error));
        }
        let (started, delay) = match self.state {
            BusyWaitState::NotWaiting => (Instant::now(), Duration::from_millis(5)),
            BusyWaitState::Waiting { started, delay } => (started, delay),
        };
        let remaining = self.timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(map_turso_error(error));
        }
        self.state = BusyWaitState::Waiting {
            started,
            delay: (delay * 2).min(Duration::from_millis(50)),
        };
        tokio::time::sleep(delay.min(remaining)).await;
        Ok(())
    }

    pub(crate) async fn next_row(
        &mut self,
        rows: &mut turso::Rows,
    ) -> Result<Option<turso::Row>, Error> {
        loop {
            match rows.next().await {
                Ok(row) => return Ok(row),
                Err(error) => self.wait_after_error(error).await?,
            }
        }
    }
}
