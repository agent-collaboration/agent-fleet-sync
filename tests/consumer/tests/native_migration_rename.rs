#![allow(dead_code, unused_imports)]
#[path = "migration_rename_support/mod.rs"]
mod rename;
mod support;
use rename::*;
#[tokio::test]
async fn warm_rename_and_next_write_replicate() -> TestResult {
    run_case("warm_rename", rename_table_case(false, false)).await
}
#[tokio::test]
async fn reopened_v1_rename_and_next_write_replicate() -> TestResult {
    run_case("reopened_rename", rename_table_case(true, false)).await
}
#[tokio::test]
async fn fresh_full_bundle_rename_and_next_write_replicate() -> TestResult {
    run_case("fresh_rename", rename_table_case(false, true)).await
}
#[tokio::test]
async fn fresh_rename_chain_and_next_write_replicate() -> TestResult {
    run_case("fresh_chain", rename_chain_case()).await
}
