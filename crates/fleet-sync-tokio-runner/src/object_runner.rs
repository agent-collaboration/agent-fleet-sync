use crate::{CallRouter, TokioHost};
use durable_steps::{AtomicStore, Engine, StoreError};
use futures_util::{StreamExt, stream::FuturesUnordered};
use runtime_primitives::{Clock, SystemClock};
use std::sync::Arc;
use tokio::sync::watch;

// Bound wake-driven scans while earlier passes await calls. Actual operation
// execution is independently capped by the Engine's shared capacity permits.
const MAX_PENDING_DRIVES: usize = 8;

pub struct TokioRunner<S: AtomicStore, R: CallRouter, C: Clock + 'static = SystemClock> {
    engine: Arc<Engine<S, TokioHost<R, C>>>,
    host: Arc<TokioHost<R, C>>,
}
impl<S: AtomicStore, R: CallRouter, C: Clock + 'static> TokioRunner<S, R, C> {
    pub fn new(engine: Arc<Engine<S, TokioHost<R, C>>>, host: Arc<TokioHost<R, C>>) -> Self {
        Self { engine, host }
    }
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<(), StoreError> {
        if *shutdown.borrow() {
            return Ok(());
        }
        let mut drives = FuturesUnordered::new();
        drives.push(self.engine.drive_due());
        let mut stopping = false;
        let mut failure = None;
        loop {
            if *shutdown.borrow() {
                stopping = true;
            }
            if stopping && drives.is_empty() {
                return failure.map_or(Ok(()), Err);
            }
            tokio::select! {
                report = drives.next(), if !drives.is_empty() => {
                    match report {
                        Some(Ok(report)) => {
                            for (operation, failure) in &report.failed {
                                tracing::warn!(operation=%operation.as_uuid(),?failure,"durable operation failed");
                            }
                            if let Some(error) = report.wake_failure {
                                failure.get_or_insert_with(|| StoreError::backend(error));
                                stopping = true;
                            }
                        }
                        Some(Err(error)) => {
                            failure.get_or_insert(error);
                            stopping = true;
                        }
                        None => {}
                    }
                },
                _ = self.host.wait(), if !stopping && drives.len() < MAX_PENDING_DRIVES => {
                    drives.push(self.engine.drive_due());
                },
                changed = shutdown.changed(), if !stopping => {
                    if changed.is_err() || *shutdown.borrow() {stopping = true;}
                }
            }
        }
    }
}
