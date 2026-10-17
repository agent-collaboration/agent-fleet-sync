//! SQLx driver for the Turso database engine
//!
//! `sqlx-turso` exposes a distinct SQLx [`Database`](sqlx::Database), [`Turso`], backed by the
//! Rust `turso` crate: one owned connection per store, checked queries through this crate's own
//! [`query!`] family and SQLx migrations.
//!
//! # Example
//!
//! ```no_run
//! use sqlx_turso::{
//!     TursoConnection,
//!     sqlx::{Connection, Executor, Row},
//! };
//!
//! # async fn example() -> sqlx_turso::sqlx::Result<()> {
//! let mut connection = TursoConnection::connect("turso::memory:").await?;
//! connection
//!     .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
//!     .await?;
//! connection
//!     .execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
//!     .await?;
//!
//! let row = connection.fetch_one("SELECT name FROM users WHERE id = 1").await?;
//! assert_eq!(row.try_get::<String, _>("name")?, "alice");
//! # Ok(())
//! # }
//! ```
//!
//! # Features
//!
//! All features are on by default.
//!
//! - `runtime-tokio`: the Tokio runtime integration
//! - `macros`: `sqlx_turso::query!` and the rest of the checked query macros
//! - `migrate`: SQLx's `Migrate` for [`TursoConnection`]
//! - `chrono`: chrono date and time codecs
//!
//! The crate README lists the known limitations and where offline metadata comes from.

#![warn(missing_docs)]

pub use sqlx_turso_core::{
    Turso, TursoAdapterError, TursoArguments, TursoColumn, TursoConnectOptions, TursoConnection,
    TursoDatabaseError, TursoDatabaseTarget, TursoQueryResult, TursoRow, TursoStatement,
    TursoStorageClass, TursoTransaction, TursoTransactionManager, TursoTypeInfo, TursoValue,
    TursoValueRef,
};

#[cfg(feature = "migrate")]
pub use sqlx_turso_core::{Migrate, Migration, MigrationType};

/// The generic SQLx crate the generated macro code and the examples refer to
#[doc(hidden)]
pub use sqlx;

#[cfg(feature = "macros")]
pub use sqlx_turso_macros::{
    query, query_as, query_file, query_file_as, query_file_scalar, query_scalar,
};
