//! Replication copy rules; the host owns transactions and carries out effects after commit.

mod apply_follow_up;
mod apply_rules;
mod bound_sql;
mod checked_queries;
mod copy_contract;
mod copy_lifecycle;
mod copy_storage;
mod fence_rules;
mod part_assembly;
mod receiver_contract;
mod row_apply;
mod row_transactions;
mod schema_reads;
mod seed_publication;
mod seed_rules;
mod seed_storage;
mod snapshot_reads;
mod sql_transport;

pub use apply_follow_up::{ApplyFollowUp, FollowUpCall, FollowUpInput, admit_follow_ups};
pub use apply_rules::{apply_piece, discard_staged_parts};
pub use copy_lifecycle::{copy_status, create_copy, retire_copy};
pub use fence_rules::fence;
pub use fleet_sync_protocol::{CopyRole, CopyStatus};
pub use receiver_contract::{
    AppliedPiece, ApplyDecision, ApplyFailure, FollowUpDone, FollowUpFailure, ReceiverError,
    ReceiverHook,
};
pub use seed_rules::{seed_begin, seed_complete, seed_page};
pub use snapshot_reads::snapshot_page;
pub use sql_transport::StoreError;
use sql_transport::{BoundStatement, BoundValue};

#[cfg(test)]
mod turso_transport_tests;

#[cfg(test)]
mod turso_rule_tests;

#[cfg(test)]
mod turso_seed_tests;

#[cfg(test)]
mod turso_follow_up_tests;
