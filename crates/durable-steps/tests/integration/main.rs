#![allow(clippy::panic, clippy::panic_in_result_fn)]

mod call_reconciliation;
#[cfg(feature = "turso")]
mod journal_constraints;
#[cfg(feature = "turso")]
mod journal_corpus;
#[cfg(feature = "turso")]
mod journal_metadata;
#[cfg(feature = "turso")]
mod journal_parser_stack;
#[cfg(all(feature = "turso", unix))]
mod journal_upgrade_crash;
#[cfg(feature = "turso")]
mod legacy_integrity;
#[cfg(feature = "turso")]
mod legacy_rows;
#[cfg(feature = "turso")]
mod scope_migration_cases;
#[cfg(feature = "turso")]
mod scope_migrations;
mod store_conformance;
#[path = "../support/mod.rs"]
mod support;
