//! Test-only scheduling at the existing copy-call boundary, before any SQL.
use crate::ServiceError;
use durable_steps::{CallId, storage::CallReach};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CopyCallKind {
    Create,
    Acknowledged,
    Fence,
}
struct Hold {
    kind: CopyCallKind,
    observed: watch::Sender<Option<CallId>>,
    waiting_observed: watch::Sender<Option<CallReach>>,
    released: watch::Sender<bool>,
}
#[derive(Default)]
pub(crate) struct CopyCallProbe {
    held: Mutex<Option<Arc<Hold>>>,
}
impl CopyCallProbe {
    pub async fn arm(&self, kind: CopyCallKind) -> Result<(), ServiceError> {
        let mut held = self.held.lock().await;
        if held.is_some() {
            return Err(ServiceError::rule("copy call already held"));
        }
        *held = Some(Arc::new(Hold {
            kind,
            observed: watch::channel(None).0,
            waiting_observed: watch::channel(None).0,
            released: watch::channel(false).0,
        }));
        Ok(())
    }
    pub async fn before_call(&self, kind: CopyCallKind, call: CallId) {
        let held = self
            .held
            .lock()
            .await
            .clone()
            .filter(|held| held.kind == kind);
        if let Some(held) = held {
            let mut released = held.released.subscribe();
            held.observed.send_replace(Some(call));
            while !*released.borrow() {
                if released.changed().await.is_err() {
                    break;
                }
            }
        }
    }
    pub async fn wait(&self) -> Result<CallId, ServiceError> {
        let held = self
            .held
            .lock()
            .await
            .clone()
            .ok_or_else(|| ServiceError::rule("no copy call held"))?;
        let mut observed = held.observed.subscribe();
        loop {
            if let Some(call) = observed.borrow().clone() {
                return Ok(call);
            }
            observed.changed().await.map_err(ServiceError::rule)?;
        }
    }
    pub async fn release(&self) {
        if let Some(held) = self.held.lock().await.take() {
            held.released.send_replace(true);
        }
    }
    pub async fn observe_waiter(&self, reach: CallReach) {
        if let Some(held) = self.held.lock().await.as_ref() {
            held.waiting_observed.send_replace(Some(reach));
        }
    }
    pub async fn wait_for_waiter(&self) -> Result<CallReach, ServiceError> {
        let held = self
            .held
            .lock()
            .await
            .clone()
            .ok_or_else(|| ServiceError::rule("no copy call held"))?;
        let mut observed = held.waiting_observed.subscribe();
        loop {
            if let Some(reach) = *observed.borrow() {
                return Ok(reach);
            }
            observed.changed().await.map_err(ServiceError::rule)?;
        }
    }
}
