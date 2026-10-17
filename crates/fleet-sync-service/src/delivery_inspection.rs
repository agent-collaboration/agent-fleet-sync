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
    /// Test-only read through each delivery owner's transaction. No peer frames expose this seam.
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
                    .and_then(|slot| slot.get())
                    .cloned(),
                owners
                    .get(&DeliveryObject::Status(origin))
                    .and_then(|slot| slot.get())
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
