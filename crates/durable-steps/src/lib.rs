//! Durable operations with atomic local steps and receipted calls.
//!
//! The execution core has no runtime dependency. A store owns transactions; a
//! host supplies time, identifiers, call dispatch and timer re-arming.

mod call_execution;
mod canonical_payload;
#[cfg(feature = "durable-object")]
pub mod durable_object_store;
mod operation_context;
mod operation_contract;
mod operation_engine;
mod operation_model;
#[cfg(any(feature = "turso", feature = "durable-object"))]
mod sql_journal;
#[cfg(any(feature = "turso", feature = "durable-object"))]
mod sql_rows;
pub mod storage;
#[cfg(feature = "turso")]
pub mod turso_store;
pub mod volatile_store;

pub use operation_contract::{
    AtomicStore, BoxFuture, CallTarget, Host, JournalScope, OperationContext, Task,
};
pub use operation_engine::Engine;
pub use operation_model::*;
pub use uuid::Uuid;
