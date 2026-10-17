use durable_steps::CallId;
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WriterTransportObservation {
    Released {
        call: CallId,
        request: ReleaseRequest,
        answer: ReleaseAnswer,
    },
    Withdrawn {
        call: CallId,
        request: WithdrawRelease,
        answer: WithdrawAnswer,
    },
    SendingPiece {
        receiver: ReceiverId,
        through: ReplicationPosition,
    },
}
pub(crate) struct WriterTransportProbe {
    armed: AtomicU8,
    observed: tokio::sync::watch::Sender<Option<WriterTransportObservation>>,
    release: tokio::sync::Notify,
}
impl Default for WriterTransportProbe {
    fn default() -> Self {
        Self {
            armed: AtomicU8::new(0),
            observed: tokio::sync::watch::channel(None).0,
            release: Default::default(),
        }
    }
}
impl WriterTransportProbe {
    pub fn arm(&self, kind: u8) {
        self.observed.send_replace(None);
        self.armed.store(kind, Ordering::SeqCst);
    }
    pub async fn at(&self, kind: u8, observation: WriterTransportObservation) {
        if self
            .armed
            .compare_exchange(kind, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.observed.send_replace(Some(observation));
            self.release.notified().await;
        }
    }
    pub async fn wait(&self) -> Result<WriterTransportObservation, durable_steps::StoreError> {
        let mut changed = self.observed.subscribe();
        loop {
            if let Some(observation) = changed.borrow().clone() {
                return Ok(observation);
            }
            changed
                .changed()
                .await
                .map_err(durable_steps::StoreError::backend)?;
        }
    }
    pub fn release(&self) {
        self.release.notify_one();
    }
}
