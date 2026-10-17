//! Standalone native hosts for shared rules, without a board dependency.

mod call_router;
mod copy_owner;
mod database_status;
mod install_host;
#[cfg(feature = "test-support")]
mod install_page_probe;
mod source_host;
pub use source_host::AttachedSource;
mod registry_host;
mod replication_host;
mod routed_calls;
mod service_contract;
mod service_handle;
mod service_runtime;
mod service_state;
mod session_handler;
mod websocket_server;

pub use call_router::{ServiceCalls, ServiceHost, ServiceObject, ServiceScope};
pub use service_contract::{ConsumerHandler, ServiceConsumer, ServiceError, ServiceOptions};
pub use service_handle::ImportSeedRequest;
pub use service_runtime::{FleetService, ServiceHandle};

mod delivery_owner;
mod delivery_publication;
mod delivery_requests;

#[cfg(feature = "test-support")]
mod delivery_inspection;
#[cfg(feature = "test-support")]
pub use delivery_inspection::DeliveryServiceSnapshot;
