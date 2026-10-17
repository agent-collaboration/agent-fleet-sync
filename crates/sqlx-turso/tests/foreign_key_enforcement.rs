//! Foreign keys on a native Turso store: enforced by default, honoured for parent-first
//! writes, and not deferrable (a known limitation kept as a negative test)
#![allow(clippy::panic_in_result_fn)]

mod support;

use sqlx_turso::{
    TursoConnectOptions, TursoConnection,
    sqlx::{ConnectOptions, Connection, error::ErrorKind},
};
use support::{
    TestResult,
    project_admission::{DEFAULT_MILESTONE_ID, PROJECT_ID, initialize_project, task_id},
    scratch_store::{
        PROJECT_STORE_MIGRATOR, ensure_no_dangling_foreign_keys, foreign_keys_enabled,
        migrate_in_owned_transaction,
    },
};

const SECOND_MILESTONE_ID: &str = "019a0000-0000-7000-8000-000000000003";

async fn initialized_memory_store() -> TestResult<TursoConnection> {
    let mut connection = TursoConnectOptions::new().connect().await?;
    migrate_in_owned_transaction(&mut connection, &PROJECT_STORE_MIGRATOR).await?;
    initialize_project(&mut connection).await?;
    Ok(connection)
}

fn foreign_key_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(|database_error| database_error.kind() == ErrorKind::ForeignKeyViolation)
}

#[tokio::test]
async fn foreign_keys_are_enforced_by_default() -> TestResult {
    // Arrange
    let mut connection = initialized_memory_store().await?;

    // Act
    let enforced = foreign_keys_enabled(&mut connection).await?;
    let error = sqlx_turso::query!(
        "INSERT INTO tasks (id, milestone_id, status, revision) VALUES (?, ?, ?, ?)",
        task_id(1),
        "019a0000-0000-7000-8000-00000000dead",
        "open",
        1_i64
    )
    .execute(&mut connection)
    .await
    .expect_err("the milestone does not exist");

    // Assert
    assert!(enforced);
    assert!(foreign_key_violation(&error), "{error}");
    Ok(())
}

#[tokio::test]
async fn a_parent_written_before_its_child_commits_in_one_transaction() -> TestResult {
    // Arrange
    let mut connection = initialized_memory_store().await?;

    // Act
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    sqlx_turso::query!(
        "INSERT INTO milestones (id, project_id, is_default, status, revision) VALUES (?, ?, ?, ?, ?)",
        SECOND_MILESTONE_ID,
        PROJECT_ID,
        false,
        "open",
        1_i64
    )
    .execute(&mut *transaction)
    .await?;
    for (sequence, milestone) in [(1_i64, DEFAULT_MILESTONE_ID), (2, SECOND_MILESTONE_ID)] {
        sqlx_turso::query!(
            "INSERT INTO tasks (id, milestone_id, status, revision) VALUES (?, ?, ?, ?)",
            task_id(sequence),
            milestone,
            "open",
            1_i64
        )
        .execute(&mut *transaction)
        .await?;
    }
    sqlx_turso::query!(
        "INSERT INTO task_dependencies (task_id, dependency_id) VALUES (?, ?)",
        task_id(2),
        task_id(1)
    )
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;

    // Assert
    ensure_no_dangling_foreign_keys(&mut connection).await?;
    let tasks = sqlx_turso::query_scalar!(r#"SELECT COUNT(*) AS "count!: i64" FROM tasks"#)
        .fetch_one(&mut connection)
        .await?;
    assert_eq!(tasks, 2);
    Ok(())
}

/// Known limitation: `PRAGMA defer_foreign_keys = ON` is accepted but does not defer the check,
/// so writes must put parents first.
#[tokio::test]
async fn deferring_foreign_keys_does_not_allow_a_child_before_its_parent() -> TestResult {
    // Arrange
    let mut connection = initialized_memory_store().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query("PRAGMA defer_foreign_keys = ON")
        .execute(&mut *transaction)
        .await?;

    // Act: the child row comes first; its parent would follow in the same transaction
    let error = sqlx_turso::query!(
        "INSERT INTO tasks (id, milestone_id, status, revision) VALUES (?, ?, ?, ?)",
        task_id(1),
        SECOND_MILESTONE_ID,
        "open",
        1_i64
    )
    .execute(&mut *transaction)
    .await
    .expect_err("the check is not deferred");
    transaction.rollback().await?;

    // Assert
    assert!(foreign_key_violation(&error), "{error}");
    Ok(())
}
