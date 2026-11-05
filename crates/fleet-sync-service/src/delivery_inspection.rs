use crate::{ServiceConsumer, ServiceError, ServiceHandle};
use fleet_sync_delivery::test_support::{DeliveryObservation, snapshot};
use fleet_sync_protocol::{ConsumerProtocol, DeliveryTypes};
use fleet_sync_session::PeerAuthenticator;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct DeliveryServiceSnapshot {
    pub relay: Vec<DeliveryObservation>,
}
impl<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<Material = String, Route = <P::Delivery as DeliveryTypes>::StatusRoute>,
> ServiceHandle<P, C, A>
{
    pub async fn wake_delivery(&self) -> Result<(), ServiceError> {
        use runtime_primitives::{Clock, WakeScheduler};
        let host = self.state.registry.host();
        host.wake_at(host.now()).await.map_err(ServiceError::rule)
    }
    /// Read committed relay custody and status through their one existing owner.
    pub async fn delivery_snapshot(&self) -> Result<DeliveryServiceSnapshot, ServiceError> {
        let binding = self.state.consumer.clone();
        let relay = self
            .state
            .registry
            .rule_transaction(move |scope| {
                Box::pin(async move {
                    snapshot::<_, P::Delivery, _>(
                        scope,
                        fleet_sync_delivery::DeliveryHostRole::Relay,
                        binding.as_ref(),
                    )
                    .await
                })
            })
            .await
            .map_err(ServiceError::rule)?;
        Ok(DeliveryServiceSnapshot { relay })
    }
}
