#![forbid(unsafe_code)]
//! A Cloudflare Durable Objects driver for sqlx.
//!
//! [`Do`] is an [`sqlx::Database`](sqlx_core::database::Database) over a
//! Durable Object's SQL storage, so code written against sqlx's runtime API --
//! `sqlx::query`, `sqlx::query_as`, `#[derive(sqlx::FromRow)]`, `Executor` --
//! runs inside a SQLite-backed Durable Object with the database type swapped.
//!
//! ```no_run
//! use sqlx_durable_object::{Do, DoConnection};
//! use worker::{durable_object, DurableObject, Env, Request, Response, State};
//!
//! #[derive(sqlx::FromRow)]
//! struct User {
//!     id: i64,
//!     name: String,
//! }
//!
//! #[durable_object]
//! pub struct Users {
//!     conn: DoConnection,
//! }
//!
//! impl DurableObject for Users {
//!     fn new(state: State, _env: Env) -> Self {
//!         // One connection for the object's lifetime: it is only a handle to
//!         // storage the object already owns.
//!         Self { conn: DoConnection::new(state.storage()) }
//!     }
//!
//!     async fn fetch(&self, _req: Request) -> worker::Result<Response> {
//!         let user = sqlx::query_as::<Do, User>("SELECT id, name FROM users WHERE id = ?")
//!             .bind(1_i64)
//!             .fetch_one(&self.conn)
//!             .await
//!             .map_err(|e| worker::Error::RustError(e.to_string()))?;
//!
//!         Response::ok(user.name)
//!     }
//! }
//! ```
//!
//! The database lives in the Durable Object: queries never leave the process,
//! and each one runs to completion before its future first returns. The class
//! must be declared with `new_sqlite_classes` in `wrangler.toml` -- the
//! key-value-backed kind has no SQL storage.
//!
//! # What Durable Object storage cannot do
//!
//! Each of these fails loudly rather than pretending:
//!
//! - **`begin()`.** It returns an error: `sql.exec` rejects `BEGIN` and
//!   `SAVEPOINT`. Transactions take a callback instead --
//!   [`DoConnection::transaction`] -- and [`DoConnection::execute_batch`] runs
//!   several statements atomically without one.
//! - **Integers beyond ±(2^53 − 1).** Values cross as JavaScript numbers, so
//!   binding a wider `i64` is an encode error rather than a rounded value.
//!   See [`types`].
//! - **`query!` and `describe`.** sqlx's macros only know its built-in
//!   drivers, and `sql.exec` cannot describe a statement without running it.
//!
//! There is no `sqlx::Pool` either -- a Durable Object's storage has nothing
//! to pool -- and no `sqlx::migrate!`.
//!
//! This crate only *runs* on `wasm32-unknown-unknown`, inside a Worker. It
//! compiles on other targets so that docs, unit tests and `cargo check` work,
//! but its `Send` impls lean on Durable Objects being single-threaded: do not
//! call it from a threaded host.

mod arguments;
mod batch;
mod column;
mod connection;
mod database;
mod error;
mod executor;
mod js;
mod row;
mod statement;
mod transaction;
pub mod types;
mod value;

pub use arguments::DoArguments;
pub use column::DoColumn;
pub use connection::{DoConnectOptions, DoConnection};
pub use database::Do;
pub use row::DoRow;
pub use statement::DoStatement;
pub use transaction::{DoTransaction, DoTransactionManager};
pub use value::{DoValue, DoValueRef};

// The types Durable Object storage shares with D1, under this crate's names:
// both are SQLite behind a JavaScript API, so they are the same types
// underneath.
#[doc(inline)]
pub use sqlx_durable_object_core::{
    DatabaseError as DoDatabaseError, QueryResult as DoQueryResult, TypeInfo as DoTypeInfo,
    Value as DoArgumentValue,
};

/// What one statement of a [`DoConnection::fetch_batch`] did, and the rows it
/// returned.
pub type DoBatchResult = sqlx_durable_object_core::BatchResult<DoRow>;

/// An executor for Durable Object storage: `&DoConnection` or
/// `&mut DoConnection`.
pub trait DoExecutor<'c>: sqlx_core::executor::Executor<'c, Database = Do> {}
impl<'c, T: sqlx_core::executor::Executor<'c, Database = Do>> DoExecutor<'c> for T {}

// NOTE: required due to the lack of lazy normalization
sqlx_core::impl_into_arguments_for_arguments!(DoArguments);
sqlx_core::impl_column_index_for_row!(DoRow);
sqlx_core::impl_column_index_for_statement!(DoStatement);
sqlx_core::impl_acquire!(Do, DoConnection);
sqlx_core::impl_encode_for_option!(Do);
