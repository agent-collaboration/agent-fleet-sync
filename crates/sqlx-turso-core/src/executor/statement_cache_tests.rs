//! A cached prepared statement must return the current schema's result columns
//!
//! The engine reprepares a statement only on its first step, after the driver and the SDK have
//! already taken the column list, so a statement cached before a schema change would otherwise
//! keep returning the old shape.

use std::time::Duration;

use futures_util::FutureExt;
use sqlx_core::{
    column::Column,
    connection::{ConnectOptions, Connection},
    describe::Describe,
    executor::Executor,
    query::query,
    raw_sql::raw_sql,
    row::Row,
    sql_str::SqlStr,
};

use crate::{Turso, TursoConnectOptions, TursoConnection};

const SELECT_ALL: &str = "SELECT * FROM items";

async fn warmed_connection(create_table: &'static str) -> sqlx_core::Result<TursoConnection> {
    let mut connection = TursoConnectOptions::new().connect().await?;
    (&mut connection).execute(create_table).await?;
    (&mut connection)
        .execute("INSERT INTO items (id) VALUES (1)")
        .await?;
    // A persistent query prepares and caches the statement.
    query::<Turso>(SELECT_ALL)
        .fetch_one(&mut connection)
        .await?;
    Ok(connection)
}

fn column_names(row: &crate::TursoRow) -> Vec<String> {
    row.columns()
        .iter()
        .map(|column| column.name().to_owned())
        .collect()
}

fn described_column_names(describe: &Describe<Turso>) -> Vec<String> {
    describe
        .columns()
        .iter()
        .map(|column| column.name().to_owned())
        .collect()
}

/// Rolling back restores the earlier schema and its cookie, so the next schema change reuses the
/// cookie the cache recorded for the rolled-back column.
const REPLACE_LABEL_WITH_TITLE: &str =
    "ALTER TABLE items ADD COLUMN title TEXT; UPDATE items SET title = 'new' WHERE id = 1";

async fn connection_with_one_item() -> sqlx_core::Result<TursoConnection> {
    let mut connection = TursoConnectOptions::new().connect().await?;
    (&mut connection)
        .execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .await?;
    (&mut connection)
        .execute("INSERT INTO items (id) VALUES (1)")
        .await?;
    Ok(connection)
}

async fn assert_items_have_the_title_column(
    connection: &mut TursoConnection,
) -> sqlx_core::Result<()> {
    let describe = connection.describe(SqlStr::from_static(SELECT_ALL)).await?;
    let row = query::<Turso>(SELECT_ALL)
        .fetch_one(&mut *connection)
        .await?;
    assert_eq!(described_column_names(&describe), ["id", "title"]);
    assert_eq!(column_names(&row), ["id", "title"]);
    assert_eq!(row.try_get::<String, _>("title")?, "new");
    Ok(())
}

#[tokio::test]
async fn a_cached_statement_returns_a_column_added_after_it_was_prepared() -> sqlx_core::Result<()>
{
    // Arrange
    let mut connection = warmed_connection("CREATE TABLE items (id INTEGER PRIMARY KEY)").await?;
    (&mut connection)
        .execute("ALTER TABLE items ADD COLUMN label TEXT")
        .await?;
    (&mut connection)
        .execute("UPDATE items SET label = 'added' WHERE id = 1")
        .await?;

    // Act
    let row = query::<Turso>(SELECT_ALL)
        .fetch_one(&mut connection)
        .await?;
    let describe = (&mut connection)
        .describe(SqlStr::from_static(SELECT_ALL))
        .await?;

    // Assert
    assert_eq!(column_names(&row), ["id", "label"]);
    assert_eq!(row.try_get::<String, _>("label")?, "added");
    assert_eq!(describe.columns().len(), 2);
    Ok(())
}

#[tokio::test]
async fn a_cached_statement_returns_a_renamed_column_under_its_new_name() -> sqlx_core::Result<()> {
    // Arrange
    let mut connection =
        warmed_connection("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)").await?;
    (&mut connection)
        .execute("ALTER TABLE items RENAME COLUMN label TO title")
        .await?;

    // Act
    let row = query::<Turso>(SELECT_ALL)
        .fetch_one(&mut connection)
        .await?;

    // Assert
    assert_eq!(column_names(&row), ["id", "title"]);
    assert!(row.try_get_raw("label").is_err());
    Ok(())
}

#[tokio::test]
async fn an_unchanged_schema_keeps_the_cached_statements() -> sqlx_core::Result<()> {
    // Arrange
    let mut connection = warmed_connection("CREATE TABLE items (id INTEGER PRIMARY KEY)").await?;
    let cached = connection.cached_statements_size();

    // Act
    query::<Turso>(SELECT_ALL)
        .fetch_one(&mut connection)
        .await?;

    // Assert: nothing was evicted or re-prepared into a new entry
    assert!(cached > 0);
    assert_eq!(connection.cached_statements_size(), cached);
    Ok(())
}

#[tokio::test]
async fn a_cached_statement_sees_a_column_another_connection_added() -> sqlx_core::Result<()> {
    // Arrange: fetch_one stops the reader's cached statement after its first row
    let directory = tempfile::tempdir()?;
    let options = TursoConnectOptions::new()
        .filename(directory.path().join("shared.db"))
        .create_if_missing(true);
    let mut writer = options.connect().await?;
    let mut reader = options.connect().await?;
    (&mut writer)
        .execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .await?;
    (&mut writer)
        .execute("INSERT INTO items (id) VALUES (1), (2)")
        .await?;
    query::<Turso>(SELECT_ALL).fetch_one(&mut reader).await?;
    (&mut writer)
        .execute("ALTER TABLE items ADD COLUMN label TEXT")
        .await?;

    // Act
    let row = query::<Turso>(SELECT_ALL).fetch_one(&mut reader).await?;

    // Assert
    assert_eq!(column_names(&row), ["id", "label"]);
    Ok(())
}

#[tokio::test]
async fn a_rolled_back_savepoint_column_is_not_reused_after_a_batch() -> sqlx_core::Result<()> {
    // Arrange: the savepoint's column is cached against the cookie the batch will reach again
    let mut connection = connection_with_one_item().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut savepoint = transaction.begin().await?;
    (&mut *savepoint)
        .execute("ALTER TABLE items ADD COLUMN label TEXT")
        .await?;
    query::<Turso>(SELECT_ALL)
        .fetch_all(&mut *savepoint)
        .await?;
    savepoint.rollback().await?;

    // Act
    (&mut *transaction)
        .execute(REPLACE_LABEL_WITH_TITLE)
        .await?;

    // Assert
    assert_items_have_the_title_column(&mut transaction).await?;
    transaction.commit().await
}

#[tokio::test]
async fn a_dropped_savepoint_column_is_not_reused_after_a_batch() -> sqlx_core::Result<()> {
    // Arrange: dropping the savepoint defers its rollback to the batch's execution
    let mut connection = connection_with_one_item().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    {
        let mut savepoint = transaction.begin().await?;
        (&mut *savepoint)
            .execute("ALTER TABLE items ADD COLUMN label TEXT")
            .await?;
        query::<Turso>(SELECT_ALL)
            .fetch_all(&mut *savepoint)
            .await?;
    }

    // Act
    (&mut *transaction)
        .execute(REPLACE_LABEL_WITH_TITLE)
        .await?;

    // Assert
    assert_items_have_the_title_column(&mut transaction).await?;
    transaction.commit().await
}

#[tokio::test]
async fn a_column_rolled_back_by_written_sql_is_not_reused_after_unprepared_statements()
-> sqlx_core::Result<()> {
    // Arrange: the savepoint and its rollback are written SQL, run as unprepared statements
    let mut connection = connection_with_one_item().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    raw_sql(SqlStr::from_static("SAVEPOINT written"))
        .execute(&mut *transaction)
        .await?;
    raw_sql(SqlStr::from_static(
        "ALTER TABLE items ADD COLUMN label TEXT",
    ))
    .execute(&mut *transaction)
    .await?;
    query::<Turso>(SELECT_ALL)
        .fetch_all(&mut *transaction)
        .await?;
    raw_sql(SqlStr::from_static("ROLLBACK TO written"))
        .execute(&mut *transaction)
        .await?;

    // Act
    raw_sql(SqlStr::from_static(
        "ALTER TABLE items ADD COLUMN title TEXT",
    ))
    .execute(&mut *transaction)
    .await?;
    raw_sql(SqlStr::from_static(
        "UPDATE items SET title = 'new' WHERE id = 1",
    ))
    .execute(&mut *transaction)
    .await?;

    // Assert
    assert_items_have_the_title_column(&mut transaction).await?;
    transaction.commit().await
}

#[tokio::test]
async fn a_column_rolled_back_inside_a_batch_is_not_reused() -> sqlx_core::Result<()> {
    // Arrange: the rollback is one of the batch's own statements
    let mut connection = connection_with_one_item().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    (&mut *transaction)
        .execute("SAVEPOINT written; ALTER TABLE items ADD COLUMN label TEXT")
        .await?;
    query::<Turso>(SELECT_ALL)
        .fetch_all(&mut *transaction)
        .await?;

    // Act
    (&mut *transaction)
        .execute(
            "ROLLBACK TO written; ALTER TABLE items ADD COLUMN title TEXT; \
             UPDATE items SET title = 'new' WHERE id = 1",
        )
        .await?;

    // Assert
    assert_items_have_the_title_column(&mut transaction).await?;
    transaction.commit().await
}

#[tokio::test]
async fn a_batch_dropped_while_it_waits_on_a_lock_has_already_forgotten_the_cache()
-> sqlx_core::Result<()> {
    // Arrange: another connection holds the write lock the batch's first statement needs. In
    // Turso 0.8.1 a batch yields only while a statement waits on a lock, so this is the boundary
    // where a caller can drop it part-way.
    let directory = tempfile::tempdir()?;
    let options = TursoConnectOptions::new()
        .filename(directory.path().join("cancelled.db"))
        .create_if_missing(true);
    let mut connection = options
        .clone()
        .busy_timeout(Duration::from_secs(60))
        .connect()
        .await?;
    let mut lock_holder = options.connect().await?;
    (&mut connection)
        .execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .await?;
    (&mut connection)
        .execute("INSERT INTO items (id) VALUES (1)")
        .await?;
    query::<Turso>(SELECT_ALL)
        .fetch_all(&mut connection)
        .await?;
    let cached_before = connection.cached_statements_size();
    (&mut lock_holder).execute("BEGIN IMMEDIATE").await?;

    // Act: poll the batch by hand while it waits on the lock, then drop it
    let waited_on_the_lock = {
        let mut batch = (&mut connection).execute(REPLACE_LABEL_WITH_TITLE);
        (0..3).all(|_| (&mut batch).now_or_never().is_none())
    };
    let cached_after_drop = connection.cached_statements_size();
    (&mut lock_holder).execute("ROLLBACK").await?;
    let describe = (&mut connection)
        .describe(SqlStr::from_static(SELECT_ALL))
        .await?;

    // Assert: none of the batch ran, and the cache was already forgotten when it was dropped
    assert!(cached_before > 0);
    assert!(waited_on_the_lock, "the batch waits on the held write lock");
    assert_eq!(cached_after_drop, 0);
    assert_eq!(described_column_names(&describe), ["id"]);
    Ok(())
}
