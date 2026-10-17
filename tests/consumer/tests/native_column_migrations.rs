#![allow(dead_code, unused_imports)]
#[path = "migration_rename_support/mod.rs"]
mod migrations;
mod support;
use migrations::*;

#[tokio::test]
async fn replace_column_in_one_migration_with_live_copy() -> TestResult {
    run_column_journey(
        "replace_live",
        ColumnJourney::Replace,
        CopyAvailability::Live,
    )
    .await
}

#[tokio::test]
async fn replace_column_in_one_migration_with_offline_copy() -> TestResult {
    run_column_journey(
        "replace_offline",
        ColumnJourney::Replace,
        CopyAvailability::Offline,
    )
    .await
}

#[tokio::test]
async fn add_then_drop_in_one_writer_open_with_live_copy() -> TestResult {
    run_column_journey(
        "release_live",
        ColumnJourney::Release,
        CopyAvailability::Live,
    )
    .await
}

#[tokio::test]
async fn add_then_drop_in_one_writer_open_with_offline_copy() -> TestResult {
    run_column_journey(
        "release_offline",
        ColumnJourney::Release,
        CopyAvailability::Offline,
    )
    .await
}
