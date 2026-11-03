#![allow(clippy::unwrap_used)]
#[path = "../support/mod.rs"]
mod support;

mod checked_write_clocks;
mod existing_record_removals;
mod push_recovery;
mod pushed_inbox;
mod record_retention;
mod relay_outbox;
mod relay_sender_authority;
mod relayed_capacity;
mod row_metadata;
mod schema_index_lineage;
mod scoped_status;
mod submitter_backfill;
mod typed_custody;
mod typed_submitter;
mod typed_submitter_upgrade;

mod accounting_integrity;

mod cancel_admission_identity;

mod delivery_deadlines;

mod hop_tasks;

mod inbox_accounting_plan;

mod inbox_integrity;

mod local_custody;

mod local_observations;

mod local_retirement;

mod outbox_integrity;

mod outbox_release_gate;

mod per_item_compatibility;

mod per_item_reopen;

mod per_item_settlement;

mod projection_hooks;

mod projection_recovery;

mod routed_publication;

#[path = "../support/per_item.rs"]
mod per_item;
#[path = "../support/projection.rs"]
mod projection;

mod status_acknowledgement;

mod attempt_recovery;

mod delivery_rollback;

mod mailbox_custody;

mod mailbox_deadlines;

mod mailbox_recipient;

mod mailbox_reopen;

mod mailbox_succession;

mod recipient_refusals;

mod revision_retirement;

mod routed_settlement;

mod legacy_receive_checkpoint;

mod local_cancel_routes;

mod local_cancellation;

mod local_item_admission;

mod local_retirement_operations;

mod per_item_local_checkpoint;

#[path = "../support/local_cancellation.rs"]
mod cancellation_support;

mod retention_replay;

mod surrogate_status;
