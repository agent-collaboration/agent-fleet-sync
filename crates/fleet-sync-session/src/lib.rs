//! Logical sessions survive transport loss. Hosts own transactions and I/O;
//! server rules return effects to execute only after their transaction commits.

mod frame_shape;
mod transport_port;
pub use bytes::Bytes;
pub use transport_port::*;
mod schema_support_publication;
mod session_contract;
mod session_notices;
mod session_rules;
pub use schema_support_publication::*;
pub use session_contract::*;
pub use session_notices::*;
pub use session_rules::*;
mod session_client;
pub use session_client::*;

mod checked_session_queries;
mod session_storage;
pub use durable_steps::storage::TransactionScope;
pub static SESSION_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[cfg(test)]
mod rule_lifecycle_tests;
#[cfg(test)]
mod rule_test_support;
