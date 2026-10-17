//! Replication copy rules; the host owns transactions and carries out effects after commit.

mod apply_follow_up;
mod apply_rules;
mod bound_sql;
mod checked_queries;
mod copy_contract;
mod copy_lifecycle;
mod copy_storage;
mod fence_rules;
mod hook_migrations;
mod part_assembly;
mod receiver_contract;
mod row_apply;
mod row_transactions;
mod schema_membership;
mod schema_reads;
mod seed_publication;
mod seed_rules;
mod seed_storage;
mod snapshot_follow_up;
mod snapshot_hooks;
mod snapshot_manifest;
mod snapshot_reads;
mod sql_transport;

pub use apply_follow_up::{ApplyFollowUp, FollowUpCall, FollowUpInput, admit_follow_ups};
pub use apply_rules::{apply_piece, discard_staged_parts};
pub use copy_lifecycle::{copy_status, create_copy, retire_copy};
pub use fence_rules::fence;
pub use fleet_sync_protocol::{CopyRole, CopyStatus};
pub use hook_migrations::initialize_hook;
pub use receiver_contract::{
    AppliedPiece, ApplyDecision, ApplyFailure, FollowUpAnswer, FollowUpDone, FollowUpFailure,
    InstalledSnapshot, ReceiverError, ReceiverHook, SeedDecision,
};
pub use seed_rules::{seed_begin, seed_complete, seed_page};
pub use snapshot_follow_up::{admit_snapshot_follow_ups, snapshot_follow_up_key};
pub use snapshot_hooks::run_snapshot_hook;
pub use snapshot_manifest::snapshot_manifest;
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
mod turso_follow_up_fixture;
#[cfg(test)]
mod turso_follow_up_outcome_tests;
#[cfg(test)]
mod turso_follow_up_tests;
#[cfg(test)]
mod turso_seed_hook_tests;

#[cfg(test)]
mod turso_capacity_tests;
#[cfg(test)]
mod turso_copy_identity_tests;
#[cfg(test)]
mod turso_fence_integrity_tests;
#[cfg(test)]
mod turso_hook_migration_tests;
#[cfg(test)]
mod turso_integrity_tests;
#[cfg(test)]
mod turso_real_staging_tests;
#[cfg(test)]
mod turso_rule_fixture;
#[cfg(test)]
mod turso_schema_isolation_tests;
