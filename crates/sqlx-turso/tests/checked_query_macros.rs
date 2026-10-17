//! Checked queries against a natively migrated Turso schema, read from offline metadata
#![allow(clippy::panic_in_result_fn)]

mod support;

use sqlx_turso::{
    TursoConnectOptions, TursoConnection,
    sqlx::{ConnectOptions, Connection},
};
use support::{
    TestResult,
    scratch_store::{PROJECT_STORE_MIGRATOR, migrate_in_owned_transaction},
};

#[derive(Debug, Eq, PartialEq)]
struct RecordRow {
    sequence: i64,
    body: String,
}

async fn migrated_memory_store() -> TestResult<TursoConnection> {
    let mut connection = TursoConnectOptions::new().connect().await?;
    migrate_in_owned_transaction(&mut connection, &PROJECT_STORE_MIGRATOR).await?;
    for (sequence, body) in [(1_i64, "first"), (2, "second")] {
        sqlx_turso::query!(
            "INSERT INTO records (sequence, body) VALUES (?, ?)",
            sequence,
            body
        )
        .execute(&mut connection)
        .await?;
    }
    Ok(connection)
}

#[tokio::test]
async fn query_reads_typed_columns() -> TestResult {
    // Arrange
    let mut connection = migrated_memory_store().await?;

    // Act
    let row = sqlx_turso::query!(
        r#"SELECT sequence AS "sequence!: i64", body AS "body!: String"
           FROM records WHERE sequence = ?"#,
        1_i64
    )
    .fetch_one(&mut connection)
    .await?;

    // Assert
    assert_eq!((row.sequence, row.body.as_str()), (1, "first"));
    Ok(())
}

#[tokio::test]
async fn query_as_maps_rows_into_a_named_struct() -> TestResult {
    // Arrange
    let mut connection = migrated_memory_store().await?;

    // Act
    let rows = sqlx_turso::query_as!(
        RecordRow,
        r#"SELECT sequence AS "sequence!: i64", body AS "body!: String"
           FROM records ORDER BY sequence"#
    )
    .fetch_all(&mut connection)
    .await?;

    // Assert
    assert_eq!(
        rows,
        [
            RecordRow {
                sequence: 1,
                body: "first".to_owned()
            },
            RecordRow {
                sequence: 2,
                body: "second".to_owned()
            },
        ]
    );
    Ok(())
}

#[tokio::test]
async fn query_scalar_reads_one_column() -> TestResult {
    let mut connection = migrated_memory_store().await?;

    let count = sqlx_turso::query_scalar!(r#"SELECT COUNT(*) AS "count!: i64" FROM records"#)
        .fetch_one(&mut connection)
        .await?;

    assert_eq!(count, 2);
    Ok(())
}

#[tokio::test]
async fn query_file_macros_read_sql_from_files() -> TestResult {
    // Arrange
    let mut connection = migrated_memory_store().await?;

    // Act
    let record = sqlx_turso::query_file!("tests/queries/record_by_sequence.sql", 2_i64)
        .fetch_one(&mut connection)
        .await?;
    let typed =
        sqlx_turso::query_file_as!(RecordRow, "tests/queries/record_by_sequence.sql", 1_i64)
            .fetch_one(&mut connection)
            .await?;
    let count = sqlx_turso::query_file_scalar!("tests/queries/record_count.sql")
        .fetch_one(&mut connection)
        .await?;

    // Assert
    assert_eq!(record.body, "second");
    assert_eq!(typed.body, "first");
    assert_eq!(count, 2);
    Ok(())
}

#[tokio::test]
async fn checked_writes_roll_back_with_their_owned_transaction() -> TestResult {
    // Arrange
    let mut connection = migrated_memory_store().await?;

    // Act
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    sqlx_turso::query!(
        "INSERT INTO records (sequence, body) VALUES (?, ?)",
        3_i64,
        "uncommitted"
    )
    .execute(&mut *transaction)
    .await?;
    let inside = sqlx_turso::query_file_scalar!("tests/queries/record_count.sql")
        .fetch_one(&mut *transaction)
        .await?;
    transaction.rollback().await?;
    let after = sqlx_turso::query_file_scalar!("tests/queries/record_count.sql")
        .fetch_one(&mut connection)
        .await?;

    // Assert
    assert_eq!((inside, after), (3, 2));
    Ok(())
}

/// Known limitation: Turso's high-level statement exposes no parameter count, so a missing
/// bind compiles and the placeholder reads as NULL.
#[tokio::test]
async fn a_missing_bind_compiles_and_reads_null() -> TestResult {
    let mut connection = migrated_memory_store().await?;

    let value = sqlx_turso::query_scalar!(r#"SELECT ? AS "value: i64""#)
        .fetch_one(&mut connection)
        .await?;

    assert_eq!(value, None);
    Ok(())
}

/// Known limitation: an extra bind compiles and is rejected only when the query runs.
#[tokio::test]
async fn an_extra_bind_compiles_and_fails_at_execution() -> TestResult {
    let mut connection = migrated_memory_store().await?;

    let result = sqlx_turso::query_scalar!(r#"SELECT ? AS "value!: i64""#, 1_i64, 2_i64)
        .fetch_one(&mut connection)
        .await;

    assert!(result.is_err(), "an extra bind ran: {result:?}");
    Ok(())
}
