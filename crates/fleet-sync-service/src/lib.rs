//! Standalone native hosts for shared rules, without a board dependency.

mod call_router;
mod copy_owner;
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
