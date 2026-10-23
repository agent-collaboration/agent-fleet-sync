//! Real engine rollback is distinct from a failed requested rollback.
use crate::{Turso, TursoConnectOptions, TursoConnection};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    query::query,
    row::Row,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

pub(super) async fn connection() -> TestResult<TursoConnection> {
    let mut connection = TursoConnectOptions::new().connect().await?;
    (&mut connection)
        .execute("CREATE TABLE capacity_rows(id INTEGER PRIMARY KEY,value BLOB NOT NULL)")
        .await?;
    Ok(connection)
}
pub(super) async fn small_write(connection: &mut TursoConnection) -> TestResult {
    query::<Turso>("INSERT INTO capacity_rows(id,value) VALUES(1,?1)")
        .bind(vec![1_u8])
        .execute(connection)
        .await?;
    Ok(())
}
pub(super) async fn cap(connection: &mut TursoConnection) -> TestResult {
    (&mut *connection)
        .fetch_one("PRAGMA max_page_count=1")
        .await?;
    Ok(())
}
pub(super) async fn restore_capacity(connection: &mut TursoConnection) -> TestResult {
    (&mut *connection)
        .fetch_one("PRAGMA max_page_count=100000")
        .await?;
    Ok(())
}
pub(super) async fn count(connection: &mut TursoConnection) -> TestResult<i64> {
    Ok((&mut *connection)
        .fetch_one("SELECT count(*) AS count FROM capacity_rows")
        .await?
        .try_get("count")?)
}
async fn single_insert(connection: &mut TursoConnection) -> Result<(), sqlx_core::Error> {
    query::<Turso>("INSERT INTO capacity_rows(id,value) VALUES(2,?1)")
        .bind(vec![7_u8; 128 * 1024])
        .execute(connection)
        .await?;
    Ok(())
}
pub(super) fn assert_full(error: sqlx_core::Error) {
    assert!(matches!(error, sqlx_core::Error::Database(ref database)
        if database.code().as_deref() == Some("SQLITE_FULL")));
}
pub(super) async fn fail_single_insert(connection: &mut TursoConnection) -> TestResult {
    let error = single_insert(connection)
        .await
        .err()
        .ok_or("insert did not exhaust pages")?;
    assert_full(error);
    assert!(
        connection.raw().is_autocommit()?,
        "engine ended the entire transaction"
    );
    Ok(())
}

#[tokio::test]
async fn single_row_full_acknowledges_engine_rollback_and_allows_an_identical_retry() -> TestResult
{
    let mut connection = connection().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(!transaction.raw().is_autocommit()?);
    small_write(&mut transaction).await?;
    cap(&mut transaction).await?;
    fail_single_insert(&mut transaction).await?;

    transaction.rollback().await?;
    assert_eq!(count(&mut connection).await?, 0);
    restore_capacity(&mut connection).await?;
    let mut retry = connection.begin_with("BEGIN IMMEDIATE").await?;
    small_write(&mut retry).await?;
    single_insert(&mut retry).await?;
    retry.commit().await?;
    assert_eq!(count(&mut connection).await?, 2);
    Ok(())
}

#[tokio::test]
async fn commit_after_real_engine_rollback_fails_and_does_not_save_prior_writes() -> TestResult {
    let mut connection = connection().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    small_write(&mut transaction).await?;
    cap(&mut transaction).await?;
    fail_single_insert(&mut transaction).await?;

    assert!(transaction.commit().await.is_err());
    assert_eq!(count(&mut connection).await?, 0);
    restore_capacity(&mut connection).await?;
    let transaction = connection.begin().await?;
    transaction.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn another_write_cannot_escape_a_transaction_the_engine_already_rolled_back() -> TestResult {
    let mut connection = connection().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    small_write(&mut transaction).await?;
    cap(&mut transaction).await?;
    fail_single_insert(&mut transaction).await?;

    let escaped = query::<Turso>("INSERT INTO capacity_rows(id,value) VALUES(3,?1)")
        .bind(vec![3_u8])
        .execute(&mut *transaction)
        .await;
    assert!(
        escaped.is_err(),
        "a write escaped into an autocommit transaction"
    );
    assert!(
        transaction.begin().await.is_err(),
        "a nested begin revived the ended transaction"
    );
    transaction.rollback().await?;
    assert_eq!(count(&mut connection).await?, 0);
    Ok(())
}

#[tokio::test]
async fn nested_handles_acknowledge_engine_rollback_without_reviving_the_outer_transaction()
-> TestResult {
    let mut connection = connection().await?;
    let mut outer = connection.begin_with("BEGIN IMMEDIATE").await?;
    small_write(&mut outer).await?;
    let mut inner = outer.begin().await?;
    cap(&mut inner).await?;
    fail_single_insert(&mut inner).await?;

    inner.rollback().await?;
    assert!(outer.begin().await.is_err());
    assert!(outer.commit().await.is_err());
    assert_eq!(count(&mut connection).await?, 0);
    restore_capacity(&mut connection).await?;
    let mut next = connection.begin().await?;
    small_write(&mut next).await?;
    next.commit().await?;
    assert_eq!(count(&mut connection).await?, 1);
    Ok(())
}

#[tokio::test]
async fn a_multi_write_full_keeps_the_outer_transaction_for_ordinary_rollback() -> TestResult {
    let mut connection = connection().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    small_write(&mut transaction).await?;
    cap(&mut transaction).await?;
    let error = query::<Turso>("INSERT INTO capacity_rows(id,value) VALUES(2,?1),(3,?1)")
        .bind(vec![7_u8; 128 * 1024])
        .execute(&mut *transaction)
        .await
        .err()
        .ok_or("multi-write did not exhaust pages")?;
    assert_full(error);
    assert!(
        !transaction.raw().is_autocommit()?,
        "statement savepoint must keep the outer transaction"
    );
    assert_eq!(count(&mut transaction).await?, 1);
    transaction.rollback().await?;
    assert_eq!(count(&mut connection).await?, 0);
    Ok(())
}

#[tokio::test]
async fn a_manual_engine_rollback_is_not_mistaken_for_a_failed_statement_auto_rollback()
-> TestResult {
    let mut connection = connection().await?;
    let mut transaction = connection.begin().await?;
    small_write(&mut transaction).await?;
    transaction.raw().execute("ROLLBACK", ()).await?;
    assert!(transaction.raw().is_autocommit()?);

    assert!(
        transaction.rollback().await.is_err(),
        "no failed statement established automatic rollback"
    );
    assert_eq!(count(&mut connection).await?, 0);
    Ok(())
}
