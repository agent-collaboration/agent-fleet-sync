//! SQLx migrations inside a caller-owned `BEGIN IMMEDIATE` transaction on a native Turso file,
//! including the foreign-key-off policy a same-name table rebuild needs
#![allow(clippy::panic_in_result_fn)]

mod support;

use sqlx::{error::ErrorKind, migrate::MigrateError};
use sqlx_turso::{TursoConnection, sqlx::Connection};
use support::{
    TestResult,
    project_admission::{
        ProjectSnapshot, TaskRow, admit_task, complete_task, initialize_project, task_id,
    },
    scratch_store::{
        PROJECT_STORE_MIGRATOR, ensure_no_dangling_foreign_keys, expected_fingerprint,
        foreign_keys_enabled, migrate_in_owned_transaction, open_local_store,
        project_store_migrator_through, project_store_migrator_with_copy_rename_rebuild,
        schema_fingerprint, set_foreign_keys,
    },
};

/// A file store on the base schema with three admitted tasks, the first one completed
async fn populated_store(directory: &tempfile::TempDir) -> TestResult<TursoConnection> {
    let mut store = open_local_store(&directory.path().join("project.db")).await?;
    migrate_in_owned_transaction(&mut store, &PROJECT_STORE_MIGRATOR).await?;
    initialize_project(&mut store).await?;
    for sequence in 1..=3 {
        admit_task(&mut store, sequence).await?;
    }
    complete_task(&mut store, 1).await?;
    Ok(store)
}

async fn reinsert_tasks(connection: &mut TursoConnection, tasks: &[TaskRow]) -> TestResult {
    for task in tasks {
        sqlx_turso::query!(
            "INSERT INTO tasks (id, milestone_id, status, revision) VALUES (?, ?, ?, ?)",
            task.id,
            task.milestone_id,
            task.status,
            task.revision
        )
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_rolled_back_owned_migration_leaves_no_schema_and_a_rerun_commits() -> TestResult {
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut store = open_local_store(&directory.path().join("project.db")).await?;

    // Act
    let mut transaction = store.begin_with("BEGIN IMMEDIATE").await?;
    PROJECT_STORE_MIGRATOR
        .run_direct(None, &mut *transaction, false)
        .await?;
    let tables_inside = schema_fingerprint(&mut transaction).await?.len();
    transaction.rollback().await?;
    let tables_after_rollback = schema_fingerprint(&mut store).await?;
    migrate_in_owned_transaction(&mut store, &PROJECT_STORE_MIGRATOR).await?;

    // Assert: eight slice tables plus the migration history, then nothing, then all of them
    assert_eq!(tables_inside, 9);
    assert!(
        tables_after_rollback.is_empty(),
        "{tables_after_rollback:?}"
    );
    assert_eq!(
        schema_fingerprint(&mut store).await?,
        expected_fingerprint(&PROJECT_STORE_MIGRATOR).await?
    );
    Ok(())
}

#[tokio::test]
async fn an_add_column_step_matches_a_fresh_native_schema_and_keeps_rows() -> TestResult {
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut store = populated_store(&directory).await?;
    let before = ProjectSnapshot::read(&mut store).await?;
    let migrator = project_store_migrator_through(2);

    // Act
    migrate_in_owned_transaction(&mut store, &migrator).await?;

    // Assert
    assert_eq!(
        schema_fingerprint(&mut store).await?,
        expected_fingerprint(&migrator).await?
    );
    assert_eq!(ProjectSnapshot::read(&mut store).await?, before);
    Ok(())
}

#[tokio::test]
async fn a_same_name_rebuild_with_foreign_keys_off_keeps_the_row_graph() -> TestResult {
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut store = populated_store(&directory).await?;
    let before = ProjectSnapshot::read(&mut store).await?;
    let migrator = project_store_migrator_through(3);
    let expected = expected_fingerprint(&migrator).await?;

    // Act: foreign keys off before the owned transaction; rows back and checked before commit
    set_foreign_keys(&mut store, false).await?;
    let mut transaction = store.begin_with("BEGIN IMMEDIATE").await?;
    migrator.run_direct(None, &mut *transaction, false).await?;
    reinsert_tasks(&mut transaction, &before.tasks).await?;
    ensure_no_dangling_foreign_keys(&mut transaction).await?;
    let migrated = schema_fingerprint(&mut transaction).await?;
    transaction.commit().await?;
    set_foreign_keys(&mut store, true).await?;

    // Assert
    assert_eq!(migrated, expected);
    assert!(foreign_keys_enabled(&mut store).await?);
    assert_eq!(ProjectSnapshot::read(&mut store).await?, before);
    Ok(())
}

#[tokio::test]
async fn a_sql_only_copy_and_rename_rebuild_with_foreign_keys_off_keeps_the_row_graph() -> TestResult
{
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut store = populated_store(&directory).await?;
    let before = ProjectSnapshot::read(&mut store).await?;
    let migrator = project_store_migrator_with_copy_rename_rebuild();
    let expected = expected_fingerprint(&migrator).await?;

    // Act: the rows move in SQL (copy, drop, rename) inside the owned transaction
    set_foreign_keys(&mut store, false).await?;
    migrate_in_owned_transaction(&mut store, &migrator).await?;
    set_foreign_keys(&mut store, true).await?;
    let orphan = sqlx_turso::query!(
        "INSERT INTO tasks (id, milestone_id, status, revision) VALUES (?, ?, ?, ?)",
        task_id(99),
        "019a0000-0000-7000-8000-00000000dead",
        "open",
        1_i64
    )
    .execute(&mut store)
    .await;
    let dependency_on_rebuilt_table = sqlx_turso::query!(
        "INSERT INTO task_dependencies (task_id, dependency_id) VALUES (?, ?)",
        task_id(3),
        task_id(98)
    )
    .execute(&mut store)
    .await;

    // Assert: same rows, the new schema, and both directions of the foreign keys still enforced
    assert_eq!(schema_fingerprint(&mut store).await?, expected);
    assert!(foreign_keys_enabled(&mut store).await?);
    assert_eq!(ProjectSnapshot::read(&mut store).await?, before);
    for rejected in [orphan, dependency_on_rebuilt_table] {
        let error = rejected.expect_err("a dangling reference is rejected");
        assert!(
            error.as_database_error().is_some_and(
                |database_error| database_error.kind() == ErrorKind::ForeignKeyViolation
            ),
            "{error}"
        );
    }
    Ok(())
}

/// Known limitation: with foreign keys enforced, dropping a table that other tables reference
/// fails, and `PRAGMA defer_foreign_keys = ON` does not change that.
#[tokio::test]
async fn a_same_name_rebuild_with_foreign_keys_on_fails_and_rolls_back() -> TestResult {
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut store = populated_store(&directory).await?;
    let before = ProjectSnapshot::read(&mut store).await?;

    // Act
    let mut transaction = store.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query("PRAGMA defer_foreign_keys = ON")
        .execute(&mut *transaction)
        .await?;
    let error = project_store_migrator_through(3)
        .run_direct(None, &mut *transaction, false)
        .await
        .expect_err("dropping a referenced table fails with foreign keys on");
    transaction.rollback().await?;

    // Assert
    let MigrateError::ExecuteMigration(source, version) = &error else {
        return Err(format!("unexpected migration error: {error}").into());
    };
    assert_eq!(*version, 3);
    assert!(
        source
            .as_database_error()
            .is_some_and(|database_error| database_error.kind() == ErrorKind::ForeignKeyViolation),
        "{source}"
    );
    assert_eq!(
        schema_fingerprint(&mut store).await?,
        expected_fingerprint(&PROJECT_STORE_MIGRATOR).await?
    );
    assert_eq!(ProjectSnapshot::read(&mut store).await?, before);
    Ok(())
}
