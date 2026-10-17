//! Helpers shared by the integration test binaries; each binary uses a subset of them.

#![allow(dead_code)]

pub mod project_admission;
pub mod scratch_store;

/// Result type for tests and helpers: any error fails the test with its message
pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
