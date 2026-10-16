//! Durable operations with atomic local steps and receipted calls.
//!
//! The execution core has no runtime dependency. A store owns transactions; a
//! host supplies time, identifiers, call dispatch and timer re-arming.

mod canonical_payload;
mod operation_context;
mod operation_contract;
mod operation_engine;
mod operation_model;
pub mod storage;
pub mod volatile_store;

pub use operation_contract::{
    AtomicStore, BoxFuture, CallTarget, Host, JournalScope, OperationContext, Task,
};
pub use operation_engine::Engine;
pub use operation_model::*;
pub use uuid::Uuid;
