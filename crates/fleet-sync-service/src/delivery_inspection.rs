use crate::{ServiceConsumer, ServiceError, ServiceHandle, delivery_owner::DeliveryObject};
use fleet_sync_delivery::test_support::{DeliveryObservation, snapshot};
use fleet_sync_protocol::{ConsumerProtocol, PeerId};
use fleet_sync_session::PeerAuthenticator;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct DeliveryServiceSnapshot {
    pub mailbox: Vec<DeliveryObservation>,
    pub origin: Vec<DeliveryObservation>,
}
impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>>
    ServiceHandle<P, C, A>
{
    /// Observe a registered mailbox's successful native acquisition, without initializing it.
    pub async fn wait_delivery_owner(&self, mailbox: PeerId) -> Result<(), ServiceError> {
        let slot = self
            .state
            .delivery
            .lock()
            .await
            .get(&DeliveryObject::Mailbox(mailbox))
            .cloned()
            .ok_or_else(|| ServiceError::rule("delivery mailbox is not registered"))?;
        let mut acquired = slot.acquired.subscribe();
        loop {
            if slot.owner.get().is_some() {
                return Ok(());
            }
            acquired.changed().await.map_err(ServiceError::rule)?;
        }
    }

    /// Immediate test-only read through acquired owners. Wait for an inventoried
    /// mailbox with `wait_delivery_owner` before requiring its rows after restart.
    /// No peer frames expose either observation seam.
    pub async fn delivery_snapshot(
        &self,
        mailbox: PeerId,
        origin: PeerId,
    ) -> Result<DeliveryServiceSnapshot, ServiceError> {
        let (mailbox, origin) = {
            let owners = self.state.delivery.lock().await;
            (
                owners
                    .get(&DeliveryObject::Mailbox(mailbox))
                    .and_then(|slot| slot.owner.get())
                    .cloned(),
                owners
                    .get(&DeliveryObject::Status(origin))
                    .and_then(|slot| slot.owner.get())
                    .cloned(),
            )
        };
        Ok(DeliveryServiceSnapshot {
            mailbox: match mailbox {
                Some(owner) => owner
                    .object
                    .rule_transaction(|scope| Box::pin(snapshot(scope)))
                    .await
                    .map_err(ServiceError::rule)?,
                None => Vec::new(),
            },
            origin: match origin {
                Some(owner) => owner
                    .object
                    .rule_transaction(|scope| Box::pin(snapshot(scope)))
                    .await
                    .map_err(ServiceError::rule)?,
                None => Vec::new(),
            },
        })
    }
}
