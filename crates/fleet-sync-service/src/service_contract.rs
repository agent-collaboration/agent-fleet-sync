use fleet_sync_lease::RegistryConfig;
use fleet_sync_lease::{HolderChangeGate, LifecycleGate};
use fleet_sync_protocol::{ConsumerProtocol, PeerId, ReceiverId};
use fleet_sync_receiver::{FollowUpHandler, ReceiverRules};
use serde::{Serialize, de::DeserializeOwned};
use std::{future::Future, path::PathBuf, time::Duration};

#[derive(Clone, Debug)]
pub struct RelayDeliveryConfig {
    pub relay: PeerId,
    pub limits: fleet_sync_delivery::DeliveryLimits,
}

#[derive(Clone)]
pub struct ServiceOptions {
    pub clock: std::sync::Arc<dyn runtime_primitives::Clock>,
    pub directory: PathBuf,
    pub delivery: Option<RelayDeliveryConfig>,
    pub receiver: ReceiverId,
    pub registry: RegistryConfig,
    pub call_timeout: Duration,
    pub socket_timeout: Duration,
}
impl std::fmt::Debug for ServiceOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceOptions")
            .field("clock", &"dyn Clock")
            .field("directory", &self.directory)
            .field("delivery", &self.delivery)
            .field("receiver", &self.receiver)
            .field("registry", &self.registry)
            .field("call_timeout", &self.call_timeout)
            .field("socket_timeout", &self.socket_timeout)
            .finish()
    }
}
pub trait ConsumerHandler<P: ConsumerProtocol>: Send + Sync + 'static {
    fn request(
        &self,
        peer: PeerId,
        request: P::Request,
    ) -> impl Future<Output = Result<P::Answer, ServiceError>> + Send;
}
/// The service combines existing consumer contracts on its native borrowed
/// scope. Consumer code stays outside every shared rules crate.
pub trait ServiceConsumer<P: ConsumerProtocol>:
    ConsumerHandler<P>
    + for<'transaction> fleet_sync_delivery::SenderBinding<
        crate::ServiceScope<'transaction>,
        P::Delivery,
    > + LifecycleGate
    + HolderChangeGate
    + FollowUpHandler<Item = <Self as ServiceConsumer<P>>::HookItem>
    + for<'transaction> ReceiverRules<
        crate::ServiceScope<'transaction>,
        Protocol = P,
        FollowUp = <Self as ServiceConsumer<P>>::HookItem,
    >
{
    type HookItem: Serialize + DeserializeOwned + Send + Sync + 'static;
}
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error(transparent)]
    Storage(#[from] durable_steps::StoreError),
    #[error("service I/O failed: {message}")]
    Io { message: String },
    #[error("service rule failed: {message}")]
    Rule { message: String },
    #[error("invalid service data: {field}")]
    Invalid { field: String },
    #[error("operation failed: {failure}")]
    Operation {
        failure: durable_steps::OperationFailure,
    },
    #[error("operation has not completed: {operation:?}")]
    Pending {
        operation: durable_steps::OperationId,
    },
}
impl ServiceError {
    pub(crate) fn io(error: impl std::fmt::Display) -> Self {
        Self::Io {
            message: error.to_string(),
        }
    }
    pub(crate) fn rule(error: impl std::fmt::Display) -> Self {
        Self::Rule {
            message: error.to_string(),
        }
    }
}
