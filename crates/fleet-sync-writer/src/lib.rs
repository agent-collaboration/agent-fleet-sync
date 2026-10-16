//! Captured writes and transport-independent replication senders for Turso.

mod capture_normalization;
mod capture_stream;
mod checked_queries;
mod foreign_key_integrity;
mod guarded_query;
mod maintenance_transaction;
mod migration_execution;
mod migration_inspection;
mod migration_ownership;
mod native_rows;
mod piece_builder;
mod read_pool;
mod replicated_database;
mod replicated_schema;
mod replication_sender;
mod schema_inspection;
mod schema_layout;
mod sender_contract;
mod sender_retention;
mod sender_runner;
mod sender_storage;
mod snapshot_schema;
mod statement_guard;
mod write_transaction;
mod writer_authority;
mod writer_error;
mod writer_file_record;
mod writer_release;
mod writer_release_record;
mod writer_scope;
mod writer_snapshot;

pub use capture_normalization::{CaptureChange, CaptureError, normalize_capture_change};
pub use maintenance_transaction::MaintenanceTransaction;
pub use read_pool::ReadPool;
pub use replicated_database::ReplicatedDatabase;
pub use replicated_schema::{Migration, ReplicatedSchema, WriterOptions};
pub use replication_sender::ReplicationSender;
pub use schema_layout::{LayoutError, TableLayout};
pub use sender_contract::{
    BookkeepingError, CaptureBookkeeping, CapturedTransaction, PieceAnnotation, RejectedTail,
    ReplicationEvents, ReplicationTransport, Retention, SendFailure, SenderError, SenderOptions,
};
pub use sender_runner::CopyFailure;
pub use sender_storage::SenderCopyStatus;
pub use statement_guard::{StatementForm, StatementGuard, StatementRefusal};
pub use write_transaction::WriteTransaction;
pub use writer_error::{OpenError, SchemaForm, WriteRefusal};
pub use writer_snapshot::WriterSnapshot;

#[cfg(test)]
mod capture_stream_tests;
#[cfg(test)]
mod guarded_executor_tests;
#[cfg(test)]
mod schema_metadata_tests;

#[cfg(test)]
mod sender_test_fixture;
#[cfg(test)]
mod sender_tests;

#[cfg(test)]
mod writer_release_tests;
#[cfg(test)]
mod writer_snapshot_tests;
