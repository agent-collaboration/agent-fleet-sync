//! Custody belongs to the hop's transaction; hosts carry out durable calls.

/// Delivery tables applied in the host's migration transaction.
pub static DELIVERY_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[expect(
    clippy::too_many_arguments,
    reason = "generated SQL functions bind each checked parameter"
)]
mod checked_queries;
mod custody_record;
mod delivery_calls;
mod delivery_contract;
mod delivery_rollback;
mod inbox_cancellation;
mod inbox_cycles;
mod inbox_delivery;
mod inbox_receive;
mod inbox_record;
mod inbox_storage;
mod mail_storage;
mod mailbox_custody;
mod mailbox_deadlines;
mod mailbox_failure;
mod mailbox_rules;
mod outbox_record;
mod outbox_rules;
mod outbox_status;
mod status_publication;
mod status_stream;
mod task_support;

pub use delivery_contract::*;
pub use delivery_rollback::{
    DeliveryTransactionFailure, classify_delivery_store_error, delivery_rollback,
};
pub use fleet_sync_protocol::*;
pub use inbox_record::{DeliveryAttempt, DeliveryCheck, InboxEffects, ReceiveInput, ReportRef};
pub use inbox_storage::Inbox;
pub use mailbox_failure::{
    MailboxCorruption, MailboxFailureDisposition, MailboxOperation, mailbox_failure_disposition,
};
pub use mailbox_rules::Mailbox;
pub use outbox_record::{
    OutboxAdmission, OutboxStatus, OutboxStatusState, ResolvedSubmit, SubmitInput,
};
pub use outbox_rules::Outbox;
pub use status_publication::{admit_publications, resolve_publication, retire_publication};
pub use status_stream::{PublicationRef, PublicationTarget, StatusStream};

pub use delivery_calls::*;
pub use task_support::delivery_request_key;

mod deliver_task;
mod publication_task;
mod report_task;
pub use deliver_task::DeliverLocally;
pub use publication_task::PublishStatus;
pub use report_task::ReportOutcome;

mod receive_task;
pub use receive_task::ReceiveItems;

mod submit_task;
pub use submit_task::SubmitItem;

mod outbox_admission;

mod mailbox_settlement;

mod delivery_sizes;

mod delivery_limits;
mod settlement_reserve;
pub use delivery_limits::{DeliveryConfigError, DeliveryLimits};
pub use settlement_reserve::minimum_settlement_allowance;

#[cfg(feature = "test-support")]
mod checked_inspection_queries;
#[cfg(feature = "test-support")]
pub mod test_support;
