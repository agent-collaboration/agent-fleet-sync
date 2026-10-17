use fleet_sync_lease::RegistryConfig;
use fleet_sync_lease::{HolderChangeGate, LifecycleGate};
use fleet_sync_protocol::{ConsumerProtocol, PeerId, ReceiverId};
use fleet_sync_receiver::ReceiverHook;
use serde::{Serialize, de::DeserializeOwned};
use std::{future::Future, path::PathBuf, time::Duration};

#[derive(Clone, Debug)]
pub struct ServiceOptions {
    pub directory: PathBuf,
    pub receiver: ReceiverId,
    pub registry: RegistryConfig,
    pub call_timeout: Duration,
    pub socket_timeout: Duration,
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
    + LifecycleGate
    + HolderChangeGate
    + for<'transaction> ReceiverHook<
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
