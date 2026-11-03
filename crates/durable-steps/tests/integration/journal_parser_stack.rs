//! Real migrations must fit beneath callers that retain their own stack frames.
use super::journal_metadata::{Result, history, legacy_file};
use durable_steps::turso_store::migrate_journal;
use sqlx_core::{connection::Connection, migrate::Migrator};
use std::{hint::black_box, process::Command};

// Reserve at least one MiB for the caller on an ordinary default-stack thread. Do not
// increase the stack to make SQL expression parsing pass.
const CALLER_FRAMES: usize = 128;

#[inline(never)]
fn retain_caller_stack(depth: usize, action: &mut dyn FnMut() -> Result) -> Result {
    let retained = [0_u8; 8192];
    black_box(&retained);
    let result = if depth == 0 {
        action()
    } else {
        retain_caller_stack(depth - 1, action)
    };
    black_box(&retained);
    result
}

#[test]
fn migration_child_with_retained_caller_frames() -> Result {
    let Ok(phase) = std::env::var("FLEET_JOURNAL_STACK_CHILD") else {
        return Ok(());
    };
    std::thread::spawn(move || -> std::result::Result<(), String> {
        let run = || -> Result {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("nested-upgrade.sqlite");
            let mut connection = runtime.block_on(legacy_file(&path))?;
            let before = runtime.block_on(history(&mut connection))?;
            if phase == "receipts" {
                // Isolate the native bootstrap: the journal SQL has completed,
                // while the receipt table still has its original SQLx columns.
                let mut migrator = Migrator::with_migrations(
                    sqlx::migrate!("./migrations").iter().cloned().collect(),
                );
                migrator.dangerous_set_table_name("_fleet_sync_migrations");
                runtime.block_on(async {
                    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
                    migrator.run_direct(None, &mut *transaction, false).await?;
                    transaction.commit().await
                })?;
            } else {
                assert_eq!(phase, "journal");
            }
            retain_caller_stack(CALLER_FRAMES, &mut || {
                runtime.block_on(async {
                    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
                    migrate_journal(&mut transaction).await?;
                    transaction.commit().await?;
                    Ok(())
                })
            })?;
            assert_eq!(runtime.block_on(history(&mut connection))?, before);
            println!("JOURNAL_STACK_UPGRADE_OK:{phase}");
            Ok(())
        };
        run().map_err(|error| error.to_string())
    })
    .join()
    .map_err(|_| "migration thread panicked")??;
    Ok(())
}

fn assert_nested_migration(phase: &str) -> Result {
    // current_exe is the test binary already signed by the Cargo runner.
    let child = Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "journal_parser_stack::migration_child_with_retained_caller_frames",
            "--nocapture",
        ])
        .env("FLEET_JOURNAL_STACK_CHILD", phase)
        .output()?;
    assert!(
        child.status.success(),
        "{phase} migration failed below retained caller frames: {}\n{}",
        child.status,
        String::from_utf8_lossy(&child.stderr)
    );
    assert!(
        String::from_utf8_lossy(&child.stdout)
            .contains(&format!("JOURNAL_STACK_UPGRADE_OK:{phase}")),
        "child did not execute the selected migration"
    );
    Ok(())
}

#[test]
fn journal_upgrade_fits_below_retained_caller_frames() -> Result {
    assert_nested_migration("journal")
}

#[test]
fn native_receipt_upgrade_fits_below_retained_caller_frames() -> Result {
    assert_nested_migration("receipts")
}
