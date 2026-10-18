#![allow(clippy::panic_in_result_fn)]
use crate::{native_file::PublishError, read_transaction_tests::writer, *};
use fleet_sync_protocol::{GateReason, GateReasonCode};
use futures_util::TryStreamExt;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    row::Row,
    sql_str::SqlSafeStr,
};
use sqlx_turso::TursoConnectOptions;
use std::time::Duration;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn reader_refusal(error: sqlx_core::Error, statement: &str) {
    assert_eq!(
        WriteRefusal::from_sqlx(error),
        WriteRefusal::StatementRefused {
            form: StatementForm::WriteOnReader
        },
        "{statement}"
    );
}

#[tokio::test]
async fn every_read_transaction_executor_entry_refuses_mutation_before_execution() -> TestResult {
    let directory = tempfile::tempdir()?;
    let database = writer(&directory.path().join("writer.db")).await?;
    let mut read = database.reader().begin_read().await?;
    for statement in [
        "INSERT INTO local_marks VALUES('forbidden')",
        "UPDATE local_marks SET id='forbidden'",
        "DELETE FROM local_marks",
        "CREATE TABLE forbidden(id INTEGER PRIMARY KEY)",
        "ALTER TABLE local_marks ADD COLUMN forbidden TEXT",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT forbidden",
        "PRAGMA foreign_keys=OFF",
        "ATTACH ':memory:' AS forbidden",
        "SELECT 1; INSERT INTO local_marks VALUES('forbidden')",
    ] {
        // Each of the four required Executor methods gets the same real SQL.
        reader_refusal(
            (&mut read)
                .fetch_many(statement)
                .try_collect::<Vec<_>>()
                .await
                .expect_err("fetch_many admitted a reader mutation"),
            statement,
        );
        reader_refusal(
            (&mut read)
                .fetch_optional(statement)
                .await
                .expect_err("fetch_optional admitted a reader mutation"),
            statement,
        );
        reader_refusal(
            (&mut read)
                .prepare_with(statement.into_sql_str(), &[])
                .await
                .expect_err("prepare_with admitted a reader mutation"),
            statement,
        );
        reader_refusal(
            (&mut read)
                .describe(statement.into_sql_str())
                .await
                .expect_err("describe admitted a reader mutation"),
            statement,
        );
    }
    // Positive controls ensure the wrapper has not simply refused all SQL.
    let values = (&mut read)
        .fetch_many("SELECT 1 AS value UNION ALL SELECT 2 AS value")
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter_map(either::Either::right)
        .map(|row| row.try_get::<i64, _>("value"))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(values, [1, 2]);
    assert_eq!(
        (&mut read)
            .fetch_optional("SELECT COUNT(*) AS count FROM local_marks")
            .await?
            .ok_or("missing row")?
            .try_get::<i64, _>("count")?,
        0
    );
    (&mut read)
        .prepare_with("SELECT id FROM local_marks".into_sql_str(), &[])
        .await?;
    let described = (&mut read)
        .describe("SELECT id FROM local_marks".into_sql_str())
        .await?;
    assert_eq!(described.columns().len(), 1);
    read.close().await?;
    database.close().await;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ReadEnd {
    Close,
    Drop,
    CancelClose,
}

#[tokio::test]
async fn close_drop_and_cancelled_close_release_reads_and_publication() -> TestResult {
    for ending in [ReadEnd::Close, ReadEnd::Drop, ReadEnd::CancelClose] {
        let directory = tempfile::tempdir()?;
        let database = writer(&directory.path().join("writer.db")).await?;
        let mut read = database.reader().begin_read().await?;
        assert_eq!(
            (&mut read)
                .fetch_one("SELECT COUNT(*) AS count FROM local_marks")
                .await?
                .try_get::<i64, _>("count")?,
            0
        );
        let mut write = database.begin_write().await?;
        (&mut write)
            .execute("INSERT INTO local_marks VALUES('committed after read began')")
            .await?;
        write.commit().await?;
        database.begin_seeding().await?;
        let refused = tokio::time::timeout(
            Duration::from_secs(1),
            database.with_drained_file(|_| {
                Box::pin(async { Err::<(), _>(PublishError::io("callback must not run")) })
            }),
        )
        .await?;
        assert!(
            matches!(refused, Err(PublishError::ActiveReaders)),
            "{ending:?}"
        );

        match ending {
            ReadEnd::Close => read.close().await?,
            ReadEnd::Drop => drop(read),
            // Cancelling before the first poll owns and drops the real SQLx
            // transaction. Follow-up reads and publication prove its cleanup;
            // constructing/dropping the close future alone would not.
            ReadEnd::CancelClose => drop(read.close()),
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            assert_eq!(
                database
                    .reader()
                    .fetch_one("SELECT COUNT(*) AS count FROM local_marks")
                    .await?
                    .try_get::<i64, _>("count")?,
                1,
                "later read after {ending:?}"
            );
            database
                .with_drained_file(|path| {
                    Box::pin(async move {
                        let mut connection = TursoConnectOptions::new()
                            .filename(path)
                            .create_if_missing(false)
                            .connect()
                            .await
                            .map_err(PublishError::io)?;
                        let row = (&mut connection)
                            .fetch_one("SELECT COUNT(*) AS count FROM local_marks")
                            .await
                            .map_err(PublishError::io)?;
                        let count = row.try_get::<i64, _>("count").map_err(PublishError::io)?;
                        connection.close().await.map_err(PublishError::io)?;
                        assert_eq!(count, 1);
                        Ok(())
                    })
                })
                .await?;
            let mut reopened = database.reader().begin_read().await?;
            assert_eq!(
                (&mut reopened)
                    .fetch_one("SELECT COUNT(*) AS count FROM local_marks")
                    .await?
                    .try_get::<i64, _>("count")?,
                1
            );
            reopened.close().await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await??;
        database.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn move_aside_refuses_an_open_read_then_succeeds_after_close() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("writer.db");
    let destination = directory.path().join("rejected.db");
    let database = writer(&source).await?;
    database
        .reject_registration(GateReason {
            code: GateReasonCode::new("fixture_rejected")?,
            detail: "move-aside read guard".into(),
        })
        .await?;
    let mut read = database.reader().begin_read().await?;
    assert!(
        (&mut read)
            .fetch_optional("SELECT id FROM local_marks")
            .await?
            .is_none()
    );
    let refused =
        tokio::time::timeout(Duration::from_secs(1), database.move_aside(&destination)).await?;
    assert!(matches!(
        refused,
        Err(MoveAsideError::Publish(PublishError::ActiveReaders))
    ));
    assert!(source.exists());
    assert!(!destination.exists());
    read.close().await?;
    tokio::time::timeout(Duration::from_secs(5), database.move_aside(&destination)).await??;
    assert!(!source.exists());
    assert!(destination.exists());
    assert!(matches!(
        database.reader().begin_read().await,
        Err(sqlx_core::Error::PoolClosed)
    ));
    Ok(())
}
