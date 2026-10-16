//! Captured writes and transport-independent replication senders for Turso.

mod capture_normalization;
mod checked_queries;
mod foreign_key_integrity;
mod guarded_query;
mod migration_determinism;
mod migration_execution;
mod read_pool;
mod replicated_database;
mod replicated_schema;
mod schema_inspection;
mod schema_layout;
mod statement_guard;
mod write_transaction;
mod writer_error;
mod writer_file_record;
mod writer_scope;

pub use capture_normalization::{CaptureChange, CaptureError, normalize_capture_change};
pub use read_pool::ReadPool;
pub use replicated_database::ReplicatedDatabase;
pub use replicated_schema::{Migration, ReplicatedSchema, WriterOptions};
pub use schema_layout::{LayoutError, TableLayout};
pub use statement_guard::{StatementForm, StatementGuard, StatementRefusal};
pub use write_transaction::WriteTransaction;
pub use writer_error::{OpenError, SchemaForm, WriteRefusal};

#[cfg(test)]
mod guarded_executor_tests;
#[cfg(test)]
mod schema_metadata_tests;
