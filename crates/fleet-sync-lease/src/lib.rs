//! Writer lease state and the registry's shared values.
//!
//! The registry rules and durable lifecycle operations are still being implemented.
//! The holder implementation lives beside these values and shares protocol types.

mod lease_values;

pub use fleet_sync_protocol::{
    Epoch, HolderChangeAnswer, HolderChangeRefusal, HolderChangeRequest, LeaseRefusal, LeaseState,
    LeaseStatus, RenewAnswer, RenewRequest, RenewResult, ReplicationPosition, SyncTime, TransferId,
};
pub use lease_values::{LeasePolicy, WriterState};
