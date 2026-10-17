#![forbid(unsafe_code)]
//! The shared internals of the sqlx-cloudflare drivers.
//!
//! You want [`sqlx-cloudflare-d1`](https://docs.rs/sqlx-cloudflare-d1) or
//! [`sqlx-durable-object`](https://docs.rs/sqlx-durable-object) instead. Both are
//! SQLite behind a JavaScript API, so they share what this crate holds:
//!
//! - the value model -- [`Value`], bound and read alike, and [`TypeInfo`] --
//!   and the ±(2^53 − 1) rule JavaScript numbers impose on integers;
//! - the `Type`, `Encode` and `Decode` impls, written into each driver by
//!   [`impl_types!`];
//! - the conversion to and from JavaScript values ([`js`]);
//! - [`DatabaseError`], read from SQLite's message text;
//! - [`QueryResult`], [`BatchResult`] and the typing of a result's columns
//!   ([`rows`]).
//!
//! What a driver keeps for itself is the part the orphan rule will not let it
//! share: its `Database` type and the row, column, value and arguments types
//! whose sqlx traits name it.
//!
//! This API serves those drivers and carries no stability promise of its own.

mod arguments;
mod error;
pub mod js;
mod result;
mod row;
mod types;
mod value;

pub use arguments::add_argument;
pub use error::DatabaseError;
pub use result::{BatchResult, QueryResult};
pub use row::rows;
pub use value::{AsValue, MAX_SAFE_INTEGER, TypeInfo, Value, safe_integer};

// Named by `impl_types!`, which expands in a driver crate that need not
// depend on sqlx-core under that name.
#[doc(hidden)]
pub use sqlx_core as __sqlx_core;
