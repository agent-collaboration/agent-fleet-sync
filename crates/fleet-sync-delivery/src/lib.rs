//! Custody belongs to the hop's transaction; hosts carry out durable calls.

#[cfg(test)]
extern crate self as fleet_sync_delivery;
#[cfg(test)]
#[path = "../tests/support/mod.rs"]
mod custody_fixture;

/// Delivery tables applied in the host's migration transaction.
pub static DELIVERY_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

mod backfill_state;
#[expect(
    clippy::too_many_arguments,
    reason = "generated SQL functions bind each checked parameter"
)]
mod checked_queries;
mod custody_record;
mod delivery_calls;
mod delivery_contract;
mod delivery_host_role;
mod delivery_rollback;
mod inbox_cancellation;
mod inbox_delivery;
mod inbox_local;
mod inbox_push;
mod inbox_record;
mod inbox_retirement;
mod inbox_row;
mod inbox_storage;
mod outbox_record;
mod outbox_rules;
mod outbox_status;
mod outbox_storage;
mod sender_binding;
mod status_publication;
mod status_row;
mod status_stream;
mod stored_outcome;
mod submitter;
pub use sender_binding::{CustodyRow, SenderBinding};
mod submitter_backfill;
pub use submitter_backfill::backfill_submitters;
mod task_support;

pub use delivery_contract::*;
pub use delivery_host_role::DeliveryHostRole;
pub use delivery_rollback::{
    DeliveryTransactionFailure, classify_delivery_store_error, delivery_rollback,
};
pub use fleet_sync_protocol::*;
pub use inbox_record::{
    DeliveryAttempt, DeliveryCheck, InboxEffects, LocalCancelAdmission, LocalItemAdmission,
    ReportRef, ResolvedDelivery,
};
pub use inbox_retirement::{LocalRetirement, LocalRetirementRefusal};
pub use inbox_storage::Inbox;
pub use outbox_record::{
    OutboxAdmission, OutboxStatus, OutboxStatusState, ResolvedPush, ResolvedSubmit, SubmitInput,
};
pub use outbox_rules::Outbox;
pub use status_publication::{admit_publications, resolve_publication, retire_publication};
pub use status_stream::{PublicationRef, PublicationTarget, StatusStream};

pub use delivery_calls::*;
pub use task_support::delivery_request_key;

mod deliver_task;
mod publication_task;
mod report_row;
mod report_task;
pub use deliver_task::DeliverLocally;
pub use publication_task::PublishStatus;
pub use report_task::ReportOutcome;

mod submit_task;
pub use submit_task::SubmitItem;
mod push_task;
pub use push_task::{PushCall, PushItem};

mod outbox_admission;
mod outbox_relay;
mod record_retention;
mod relay_schema;
mod relay_sender_authority;
pub use outbox_record::PushInput;
pub use record_retention::{
    DELIVERY_RETENTION_MILLIS, RemoveDeliveryRecord, admit_existing_record_removals,
    admit_record_removal, item_is_too_old, removal_deadline,
};
pub use relay_schema::retire_relay_host_tables;

mod delivery_sizes;

mod delivery_limits;
mod settlement_reserve;
pub use delivery_limits::{DeliveryConfigError, DeliveryLimits};
pub use settlement_reserve::minimum_settlement_allowance;

#[cfg(feature = "test-support")]
mod checked_inspection_queries;
#[cfg(feature = "test-support")]
pub mod test_support;
