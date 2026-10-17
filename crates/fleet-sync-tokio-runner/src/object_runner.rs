use crate::{CallRouter, TokioHost};
use durable_steps::{AtomicStore, Engine, StoreError};
use runtime_primitives::{Clock, SystemClock};
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

pub struct TokioRunner<S: AtomicStore, R: CallRouter, C: Clock + 'static = SystemClock> {
    engine: Arc<Engine<S, TokioHost<R, C>>>,
    host: Arc<TokioHost<R, C>>,
    engine_work: Arc<Mutex<()>>,
}
impl<S: AtomicStore, R: CallRouter, C: Clock + 'static> TokioRunner<S, R, C> {
    pub fn new(
        engine: Arc<Engine<S, TokioHost<R, C>>>,
        host: Arc<TokioHost<R, C>>,
        engine_work: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            engine,
            host,
            engine_work,
        }
    }
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<(), StoreError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let report = {
                let _work = self.engine_work.lock().await;
                self.engine.drive_due().await?
            };
            for (operation, failure) in &report.failed {
                tracing::warn!(operation=%operation.0,?failure,"durable operation failed");
            }
            if let Some(error) = report.wake_failure {
                return Err(StoreError::backend(error));
            }
            tokio::select! {
                _=self.host.wait()=>{},
                changed=shutdown.changed()=>{
                    if changed.is_err() || *shutdown.borrow() {return Ok(());}
                }
            }
        }
    }
}
