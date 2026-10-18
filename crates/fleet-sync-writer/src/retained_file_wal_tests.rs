#![allow(clippy::panic_in_result_fn)]
use crate::{
    read_transaction_tests::{ReadSchema, writer},
    retained_file_fixture::*,
    *,
};
use std::path::Path;

async fn leave_committed_wal(path: &Path) -> TestResult {
    let database = writer(path).await?;
    add_mark(&database, "main file sentinel").await?;
    let identity = WriterIdentity::observe(&database).await;
    database.close().await;
    drop(database);
    // Establish a known main-file base, rather than assuming pool shutdown
    // checkpoints it. The later crash commit must remain only in the WAL.
    use sqlx_core::connection::{ConnectOptions, Connection};
    let mut checkpoint = sqlx_turso::TursoConnectOptions::new()
        .filename(path)
        .connect()
        .await?;
    crate::native_file::checkpoint_for_publish(&mut checkpoint).await?;
    checkpoint.close().await?;
    crate::native_file::ensure_empty_wal(path).await?;
    let reopened = identity.open(path).await?;
    add_mark(&reopened, "committed WAL sentinel").await?;
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    assert!(std::fs::metadata(&wal)?.len() > 0);
    // This child exits after the actual commit, deliberately skipping every
    // connection destructor and graceful checkpoint. Its parent waits/join it.
    #[allow(
        clippy::exit,
        reason = "owned crash fixture must retain committed WAL without running destructors"
    )]
    std::process::exit(0);
}

#[tokio::test]
async fn retained_abrupt_exit_reads_committed_wal_and_preserves_the_complete_file_set() -> TestResult
{
    if let Some((action, path)) = child_action() {
        if action == "leave-wal" {
            return leave_committed_wal(&path).await;
        }
        let expected = if action == "read-wal" {
            2
        } else {
            assert_eq!(action, "read-main");
            1
        };
        let retained = RetainedFile::open_read_only(&path, ReadSchema).await?;
        assert_eq!(mark_count(retained.reader()).await?, expected);
        let mut transaction = retained.reader().begin_read().await?;
        drop(retained);
        use sqlx_core::{executor::Executor, row::Row};
        assert_eq!(
            (&mut transaction)
                .fetch_one("SELECT COUNT(*) AS count FROM local_marks")
                .await?
                .try_get::<i64, _>("count")?,
            expected
        );
        transaction.close().await?;
        return Ok(());
    }
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("retained.db");
    child("retained_file_wal_tests::retained_abrupt_exit_reads_committed_wal_and_preserves_the_complete_file_set", "leave-wal", &path).await?;
    let sensitivity = tempfile::tempdir()?;
    let main_only = sensitivity.path().join("retained.db");
    std::fs::copy(&path, &main_only)?;
    let _permissions = ReadOnlyFixture::apply(directory.path())?;
    let _sensitivity_permissions = ReadOnlyFixture::apply(sensitivity.path())?;
    let before = snapshot(directory.path())?;
    let before_sensitivity = snapshot(sensitivity.path())?;

    child("retained_file_wal_tests::retained_abrupt_exit_reads_committed_wal_and_preserves_the_complete_file_set", "read-wal", &path).await?;
    assert_eq!(snapshot(directory.path())?, before);

    // Sensitivity: copying only the main file demonstrably loses the WAL row.
    child("retained_file_wal_tests::retained_abrupt_exit_reads_committed_wal_and_preserves_the_complete_file_set", "read-main", &main_only).await?;
    assert_eq!(snapshot(sensitivity.path())?, before_sensitivity);
    Ok(())
}
