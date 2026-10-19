//! Transaction control and streaming errors must not manufacture rollback certainty.
use super::auto_rollback_tests::*;
use crate::Turso;
use futures_util::TryStreamExt;
use sqlx_core::{connection::Connection, executor::Executor, query::query, row::Row};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn dropped_nested_handles_preserve_engine_rollback_until_a_fresh_begin() -> TestResult {
    let mut connection = connection().await?;
    for _ in 0..2 {
        {
            let mut outer = connection.begin().await?;
            small_write(&mut outer).await?;
            {
                let mut inner = outer.begin().await?;
                cap(&mut inner).await?;
                fail_single_insert(&mut inner).await?;
            }
        }
        assert_eq!(count(&mut connection).await?, 0);
        restore_capacity(&mut connection).await?;
    }
    let mut transaction = connection.begin().await?;
    small_write(&mut transaction).await?;
    transaction.commit().await?;
    assert_eq!(count(&mut connection).await?, 1);
    Ok(())
}

#[tokio::test]
async fn explicit_nested_rollback_can_repeat_before_a_fresh_top_level_begin() -> TestResult {
    let mut connection = connection().await?;
    for _ in 0..2 {
        let mut outer = connection.begin().await?;
        small_write(&mut outer).await?;
        let mut inner = outer.begin().await?;
        cap(&mut inner).await?;
        fail_single_insert(&mut inner).await?;
        inner.rollback().await?;
        outer.rollback().await?;
        assert_eq!(count(&mut connection).await?, 0);
        restore_capacity(&mut connection).await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_constraint_error_with_a_live_transaction_keeps_prior_writes_and_allows_more()
-> TestResult {
    let mut connection = connection().await?;
    let mut transaction = connection.begin().await?;
    small_write(&mut transaction).await?;
    let duplicate = query::<Turso>("INSERT INTO capacity_rows(id,value) VALUES(1,?1)")
        .bind(vec![2_u8])
        .execute(&mut *transaction)
        .await;
    assert!(
        matches!(duplicate, Err(sqlx_core::Error::Database(ref error))
        if error.code().as_deref() == Some("SQLITE_CONSTRAINT"))
    );
    assert!(!transaction.raw().is_autocommit()?);
    query::<Turso>("INSERT INTO capacity_rows(id,value) VALUES(2,?1)")
        .bind(vec![2_u8])
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    assert_eq!(count(&mut connection).await?, 2);
    Ok(())
}

#[tokio::test]
async fn caller_commit_then_begin_and_full_cannot_certify_the_original_transaction_rolled_back()
-> TestResult {
    for end in ["COMMIT", "END"] {
        let mut connection = connection().await?;
        let mut transaction = connection.begin().await?;
        small_write(&mut transaction).await?;
        (&mut *transaction)
            .execute(sqlx_core::sql_str::AssertSqlSafe(end))
            .await?;
        (&mut *transaction).execute("BEGIN IMMEDIATE").await?;
        cap(&mut transaction).await?;
        fail_single_insert(&mut transaction).await?;
        assert!(
            transaction.rollback().await.is_err(),
            "earlier committed effects cannot be called rolled back"
        );
        assert_eq!(count(&mut connection).await?, 1);
    }
    Ok(())
}

#[tokio::test]
async fn a_control_batch_with_commit_and_later_full_stays_uncertain() -> TestResult {
    for end in ["COMMIT", "END"] {
        let mut connection = connection().await?;
        let mut transaction = connection.begin().await?;
        small_write(&mut transaction).await?;
        cap(&mut transaction).await?;
        let sql = format!(
            "{end}; BEGIN IMMEDIATE; INSERT INTO capacity_rows(id,value) VALUES(2,zeroblob(131072))"
        );
        let error = (&mut *transaction)
            .execute(sqlx_core::sql_str::AssertSqlSafe(sql))
            .await
            .err()
            .ok_or("batch did not fail full")?;
        assert_full(error);
        assert!(transaction.raw().is_autocommit()?);
        assert!(
            transaction.rollback().await.is_err(),
            "batch committed effects before the failure"
        );
        assert_eq!(count(&mut connection).await?, 1);
    }
    Ok(())
}

#[tokio::test]
async fn a_late_row_stream_error_keeps_the_first_row_and_observes_the_engine_rollback() -> TestResult
{
    let mut connection = connection().await?;
    let mut transaction = connection.begin().await?;
    small_write(&mut transaction).await?;
    query::<Turso>("INSERT INTO capacity_rows(id,value) VALUES(2,?1)")
        .bind(vec![2_u8])
        .execute(&mut *transaction)
        .await?;
    {
        let mut stream = (&mut *transaction).fetch(
            "SELECT CASE WHEN id=1 THEN id ELSE abs(-9223372036854775808) END AS value FROM capacity_rows ORDER BY id"
        );
        let first = stream.try_next().await?.ok_or("missing first row")?;
        assert_eq!(first.try_get::<i64, _>("value")?, 1);
        assert!(
            stream.try_next().await.is_err(),
            "the second row must overflow"
        );
    }
    assert!(transaction.raw().is_autocommit()?);
    transaction.rollback().await?;
    assert_eq!(count(&mut connection).await?, 0);
    Ok(())
}

#[tokio::test]
async fn a_failed_deferred_constraint_commit_is_still_a_commit_failure() -> TestResult {
    let mut connection = connection().await?;
    (&mut connection).execute(
        "CREATE TABLE parents(id INTEGER PRIMARY KEY); CREATE TABLE children(id INTEGER PRIMARY KEY,parent INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED)"
    ).await?;
    let mut transaction = connection.begin().await?;
    small_write(&mut transaction).await?;
    (&mut *transaction)
        .execute("INSERT INTO children VALUES(1,99)")
        .await?;
    assert!(
        transaction.commit().await.is_err(),
        "commit must preserve its failure"
    );
    assert_eq!(count(&mut connection).await?, 0);
    let row = (&mut connection)
        .fetch_one("SELECT count(*) AS count FROM children")
        .await?;
    assert_eq!(row.try_get::<i64, _>("count")?, 0);
    Ok(())
}

#[tokio::test]
async fn a_caller_savepoint_release_keeps_the_managed_transaction_and_persists_after_reopen()
-> TestResult {
    use sqlx_core::connection::ConnectOptions;
    let directory = tempfile::tempdir()?;
    let options = crate::TursoConnectOptions::new()
        .filename(directory.path().join("savepoint.db"))
        .create_if_missing(true);
    let mut connection = options.connect().await?;
    (&mut connection)
        .execute("CREATE TABLE capacity_rows(id INTEGER PRIMARY KEY,value BLOB NOT NULL)")
        .await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    (&mut *transaction)
        .execute("SAVEPOINT caller_owned")
        .await?;
    small_write(&mut transaction).await?;
    (&mut *transaction).execute("RELEASE caller_owned").await?;
    assert!(!transaction.raw().is_autocommit()?);
    transaction.commit().await?;
    connection.close().await?;
    let mut reopened = options.connect().await?;
    assert_eq!(count(&mut reopened).await?, 1);
    Ok(())
}

#[tokio::test]
async fn a_batch_containing_release_stays_uncertain_even_when_the_outer_transaction_is_live()
-> TestResult {
    let mut connection = connection().await?;
    let mut transaction = connection.begin().await?;
    small_write(&mut transaction).await?;
    (&mut *transaction)
        .execute("SAVEPOINT caller_owned; RELEASE caller_owned")
        .await?;
    assert!(!transaction.raw().is_autocommit()?);
    assert!(
        transaction.commit().await.is_err(),
        "a control batch cannot establish the manager boundary"
    );
    assert_eq!(count(&mut connection).await?, 0);
    Ok(())
}

#[tokio::test]
async fn a_release_that_ends_a_caller_started_transaction_never_certifies_rollback() -> TestResult {
    let mut connection = connection().await?;
    let mut transaction = connection.begin().await?;
    small_write(&mut transaction).await?;
    (&mut *transaction).execute("COMMIT").await?;
    (&mut *transaction)
        .execute("SAVEPOINT caller_outer")
        .await?;
    assert!(!transaction.raw().is_autocommit()?);
    query::<Turso>("INSERT INTO capacity_rows(id,value) VALUES(2,?1)")
        .bind(vec![2_u8])
        .execute(&mut *transaction)
        .await?;
    (&mut *transaction).execute("RELEASE caller_outer").await?;
    assert!(
        transaction.raw().is_autocommit()?,
        "outer RELEASE ended the caller transaction"
    );
    assert!(
        transaction.rollback().await.is_err(),
        "both committed effects stay uncertain"
    );
    assert_eq!(count(&mut connection).await?, 2);
    Ok(())
}

#[tokio::test]
async fn an_ordinary_full_inside_a_savepoint_only_batch_is_never_a_rollback_witness() -> TestResult
{
    let mut connection = connection().await?;
    let mut transaction = connection.begin().await?;
    small_write(&mut transaction).await?;
    cap(&mut transaction).await?;
    let failure = (&mut *transaction)
        .execute("SAVEPOINT caller_owned; INSERT INTO capacity_rows(id,value) VALUES(2,zeroblob(131072))")
        .await.err().ok_or("batch did not fail full")?;
    assert_full(failure);
    assert!(transaction.raw().is_autocommit()?);
    assert!(
        transaction.rollback().await.is_err(),
        "a caller-control batch cannot certify no effect"
    );
    assert_eq!(count(&mut connection).await?, 0);
    Ok(())
}
