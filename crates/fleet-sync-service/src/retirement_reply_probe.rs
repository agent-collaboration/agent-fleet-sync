//! Test-only hold after a mailbox retirement commits and before its wire answer.
use crate::{ServiceConsumer, ServiceError, ServiceHandle};
use fleet_sync_protocol::{ConsumerProtocol, DeliveryTypes};
use fleet_sync_session::PeerAuthenticator;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

struct Hold {
    committed: watch::Sender<bool>,
    released: watch::Sender<bool>,
}
#[derive(Default)]
pub(crate) struct RetirementReplyProbe {
    held: Mutex<Option<Arc<Hold>>>,
}
impl RetirementReplyProbe {
    async fn arm(&self) -> Result<(), ServiceError> {
        let mut held = self.held.lock().await;
        if held.is_some() {
            return Err(ServiceError::rule("retirement reply already held"));
        }
        *held = Some(Arc::new(Hold {
            committed: watch::channel(false).0,
            released: watch::channel(false).0,
        }));
        Ok(())
    }
    pub async fn after_commit(&self) {
        let held = self.held.lock().await.clone();
        if let Some(held) = held {
            let mut released = held.released.subscribe();
            held.committed.send_replace(true);
            while !*released.borrow() {
                if released.changed().await.is_err() {
                    break;
                }
            }
        }
    }
    async fn wait(&self) -> Result<(), ServiceError> {
        let held = self
            .held
            .lock()
            .await
            .clone()
            .ok_or_else(|| ServiceError::rule("no retirement reply held"))?;
        let mut committed = held.committed.subscribe();
        loop {
            if *committed.borrow() {
                return Ok(());
            }
            committed.changed().await.map_err(ServiceError::rule)?;
        }
    }
}
impl<P, C, A> ServiceHandle<P, C, A>
where
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<Material = String, Route = <P::Delivery as DeliveryTypes>::StatusRoute>,
{
    pub async fn hold_next_retirement_reply(&self) -> Result<(), ServiceError> {
        self.state.retirement_reply_probe.arm().await
    }
    pub async fn wait_retirement_reply_committed(&self) -> Result<(), ServiceError> {
        self.state.retirement_reply_probe.wait().await
    }
}
