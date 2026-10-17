//! Writer lease values and SQL-backed registry rules and durable operations.
//!
//! Consumer hosts supply admission, copy calls, time and the transaction scope.

mod checked_queries;
mod lease_values;
mod workflow_values;
pub use fleet_sync_protocol::ActorRef;
pub use workflow_values::*;
mod registry_contract;
mod registry_reads;
mod registry_records;
mod registry_storage;
pub use registry_reads::database_ids;

pub use fleet_sync_protocol::{
    Epoch, HolderChangeAnswer, HolderChangeRefusal, HolderChangeRequest, LeaseRefusal, LeaseState,
    LeaseStatus, RenewAnswer, RenewRequest, RenewResult, ReplicationPosition, SyncTime, TransferId,
};
pub use lease_values::{LeasePolicy, RegistryConfig, WriterState};

pub use registry_contract::{GateFailure, HolderChangeGate, LeaseError, LifecycleGate};
pub use registry_records::DatabaseLifecycle;

mod renew_rules;
pub use renew_rules::{database_lifecycle, lease_status, renew};

#[cfg(test)]
mod turso_renew_tests;

mod call_targets;
pub use call_targets::*;

mod operation_support;
pub use operation_support::lifecycle_request_key;

pub use fleet_sync_protocol::{GateAnswer, SettleOutcome};

mod registration_rules;
pub use registration_rules::register;

mod registration_task;
pub use registration_task::RegistrationTask;

mod import_rules;
mod import_task;
pub use import_rules::import;
pub use import_task::ImportTask;

mod retirement_rules;
mod retirement_task;
pub use retirement_rules::retire;
pub use retirement_task::RetirementTask;

mod preview_rules;
mod preview_task;
pub use preview_rules::preview_takeover;
pub use preview_task::PreviewTakeoverTask;

mod change_checks;
pub use change_checks::holder_change_request_key;

mod change_completion;

mod holder_change_rules;
pub use holder_change_rules::holder_change;

mod floor_repair;
mod holder_change_task;
mod settle_reservation_task;
pub use holder_change_task::HolderChangeTask;
pub use settle_reservation_task::SettleReservationTask;

#[cfg(test)]
mod turso_registry_copy;
#[cfg(test)]
mod turso_registry_fixture;
#[cfg(test)]
mod turso_registry_gates;

#[cfg(test)]
mod turso_lifecycle_tests;

pub use change_completion::holder_change_answer;
#[cfg(test)]
mod turso_copy_fixture;

#[cfg(test)]
mod turso_change_race_tests;
#[cfg(test)]
mod turso_change_tests;
#[cfg(test)]
mod turso_epoch_proof_tests;
#[cfg(test)]
mod turso_floor_repair_tests;

#[cfg(test)]
mod turso_claim_recovery_tests;
#[cfg(test)]
mod turso_claim_refusal_tests;
#[cfg(test)]
mod turso_claim_replay_tests;
#[cfg(test)]
mod turso_idle_move_tests;
#[cfg(test)]
mod turso_move_refusal_tests;
#[cfg(test)]
mod turso_registry_integrity_tests;
#[cfg(test)]
mod turso_registry_read_tests;
#[cfg(test)]
mod turso_retirement_recovery_tests;
#[cfg(test)]
mod turso_stale_holder_tests;
#[cfg(test)]
mod turso_workflow_tests;

mod transfer_admission;
pub use transfer_admission::{admit_holder_change, cancel_holder_change};
mod holder_change_view;
pub use holder_change_view::holder_change_view;

mod transfer_storage;

mod holder_change_cancel_task;
mod transfer_cancellation;
pub use holder_change_cancel_task::HolderChangeCancelTask;

#[cfg(test)]
mod turso_workflow_race_tests;

#[cfg(test)]
mod turso_workflow_storage_tests;
