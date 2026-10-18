//! Replication copy rules; the host owns transactions and carries out effects after commit.

/// Copy tables applied in the host's migration transaction.
pub static RECEIVER_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

mod apply_follow_up;
mod apply_rules;
mod bound_sql;
mod checked_queries;
mod copy_contract;
mod copy_failure;
pub use copy_failure::{
    CopyTransactionFailure, apply_rollback, classify_copy_store_error, copy_rollback,
    receiver_rollback, retryable_copy, unavailable_receiver,
};
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
mod snapshot_install;
mod snapshot_install_contract;
mod snapshot_install_fence;
pub use snapshot_install_fence::{SNAPSHOT_REPLACED_TABLES, SnapshotFence};
mod snapshot_install_storage;
pub use snapshot_install::SnapshotInstallRules;
pub use snapshot_install_contract::{
    InstallPageReceipt, SnapshotInstall, SnapshotInstallError, SnapshotInstallState, SnapshotReady,
};
mod snapshot_manifest;
mod snapshot_reads;
mod snapshot_rows;
mod sql_transport;
#[cfg(test)]
mod turso_install_identity_tests;
#[cfg(test)]
mod turso_snapshot_install_tests;

pub use apply_follow_up::{ApplyFollowUp, FollowUpCall, FollowUpInput, admit_follow_ups};
pub use apply_rules::{apply_piece, discard_staged_parts};
pub use copy_lifecycle::{copy_status, create_copy, retire_copy};
pub use fence_rules::fence;
pub use fleet_sync_protocol::{CopyRole, CopyStatus};
pub use hook_migrations::initialize_hook;
pub use receiver_contract::{
    AppliedPiece, ApplyDecision, ApplyFailure, FollowUpAnswer, FollowUpDone, FollowUpFailure,
    InstalledSnapshot, ReceiverError, ReceiverHook, SeedAuthority, SeedDecision,
};
pub use seed_rules::{seed_begin, seed_complete, seed_page};
pub use snapshot_follow_up::{admit_snapshot_follow_ups, snapshot_follow_up_key};
pub use snapshot_hooks::run_snapshot_hook;
pub use snapshot_manifest::snapshot_manifest;
pub use snapshot_reads::snapshot_page;
pub use sql_transport::StoreError;
use sql_transport::{BoundStatement, BoundValue};

#[cfg(test)]
mod turso_autoincrement_tests;
#[cfg(test)]
mod turso_transport_tests;
#[cfg(test)]
mod turso_uuid_key_tests;

#[cfg(test)]
mod turso_rule_tests;

#[cfg(test)]
mod turso_seed_authority_tests;
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
