//! Durable operations with atomic local steps and receipted calls.
//!
//! The execution core has no runtime dependency. A store owns transactions; a
//! host supplies time, identifiers, call dispatch and timer re-arming.

extern crate self as durable_steps;

#[cfg(any(feature = "turso", feature = "durable-object"))]
// The portable corpus includes queries used only by each storage host.
#[cfg_attr(
    not(all(feature = "durable-object", feature = "turso")),
    allow(dead_code)
)]
mod checked_migration_queries;

mod call_execution;
mod canonical_payload;
#[cfg(feature = "durable-object")]
pub mod durable_object_store;
#[cfg(any(feature = "turso", feature = "durable-object"))]
mod journal_migrations;
#[cfg(feature = "turso")]
mod native_journal_metadata;
mod operation_context;
mod operation_contract;
mod operation_engine;
mod operation_model;
pub mod scope_migrations;
#[cfg(any(feature = "turso", feature = "durable-object"))]
mod sql_journal;
#[cfg(any(feature = "turso", feature = "durable-object"))]
mod sql_rows;
pub mod storage;
#[cfg(feature = "turso")]
pub mod turso_store;
pub mod volatile_store;

pub use operation_contract::{
    AtomicStore, BoxFuture, CallTarget, Host, JournalScope, OperationContext, ReborrowScope, Task,
};
#[cfg(feature = "test-support")]
pub use operation_engine::DriveHold;
pub use operation_engine::Engine;
pub use operation_model::*;
pub use uuid::Uuid;
