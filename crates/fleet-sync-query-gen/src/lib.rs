//! Describe exact SQL once, then build portable calls over a borrowed SQLx scope.
//! Build scripts use only the lightweight hash verifier; the native CLI alone
//! depends on Turso. Unknown column types are refused, never overridden.

mod source_freshness;
mod source_manifest;
pub use source_freshness::{
    ColumnMetadata, ColumnTypeSource, CorpusMetadata, QueryMetadata, verify_manifest,
};
pub use source_manifest::*;

#[cfg(feature = "native")]
mod expression_classes;
#[cfg(feature = "native")]
mod native_generation;
#[cfg(feature = "native")]
mod query_render;
#[cfg(feature = "native")]
pub use native_generation::generate_manifest;
