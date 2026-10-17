//! Consumer-independent values and JSON envelopes for fleet synchronization.
//!
//! Each transport carries the same JSON bytes in one ordered binary message.
//! Operation identities belong to their owners; frame IDs only correlate replies.

mod actor_reference;
mod crate_call_frame;
mod gate_message;
mod holder_change_message;
mod lease_message;
mod lifecycle_message;
mod piece_digest;
mod replicated_key;
mod replication_ids;
mod replication_piece;
mod replication_position;
mod reserved_namespace;
mod schema_admission;
mod schema_fingerprint;
mod schema_sql;
mod session_frame;
mod snapshot_manifest;
mod snapshot_page;
mod transaction_part;

pub use actor_reference::*;
pub use crate_call_frame::*;
pub use gate_message::*;
pub use holder_change_message::*;
pub use lease_message::*;
pub use lifecycle_message::*;
pub use piece_digest::*;
pub use replicated_key::{
    KeyColumnError, assigns_replicated_key, canonical_uuid_v7, replicated_key_columns,
};
pub use replication_ids::*;
pub use replication_piece::*;
pub use replication_position::*;
pub use reserved_namespace::*;
pub use runtime_primitives::{CallId, OperationId, StepName, SyncTime};
pub use schema_admission::*;
pub use schema_fingerprint::*;
pub use session_frame::*;
pub use snapshot_manifest::*;
pub use snapshot_page::*;
pub use transaction_part::*;
