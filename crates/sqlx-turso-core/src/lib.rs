//! SQLx driver internals for the Turso database engine
//!
//! This crate owns the [`Turso`] SQLx database marker, connection options, the engine
//! connection, executor, values, transactions, migrations and the type checking that the
//! checked query macros use. Applications depend on the `sqlx-turso` facade instead.

#![warn(missing_docs)]
#![cfg_attr(test, allow(clippy::panic_in_result_fn))]

mod arguments;
mod busy_wait;
mod column;
mod connect_options;
mod connection;
mod database;
mod engine_connection;
mod error;
mod executor;
mod macro_type_checking;
#[cfg(feature = "migrate")]
mod migrate;
mod query_result;
#[cfg(test)]
mod read_only_tests;
mod row;
mod statement;
mod transaction;
mod type_info;
mod value;

pub use arguments::TursoArguments;
pub use column::TursoColumn;
pub use connect_options::{TursoConnectOptions, TursoDatabaseTarget};
pub use connection::TursoConnection;
pub use database::Turso;
pub use error::{TursoAdapterError, TursoDatabaseError};
pub use query_result::TursoQueryResult;
pub use row::TursoRow;
pub use statement::TursoStatement;
pub use transaction::{TursoTransaction, TursoTransactionManager};
pub use type_info::TursoTypeInfo;
pub use value::{TursoStorageClass, TursoValue, TursoValueRef};

#[cfg(feature = "migrate")]
pub use sqlx_core::migrate::{Migrate, Migration, MigrationType};

sqlx_core::impl_acquire!(Turso, TursoConnection);
