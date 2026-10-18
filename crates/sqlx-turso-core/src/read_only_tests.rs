#![allow(clippy::panic_in_result_fn)]
use crate::TursoConnectOptions;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    row::Row,
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn read_only_connection_uses_the_engine_flag_and_never_creates_a_missing_file() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("existing.db");
    let mut writer = TursoConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .connect()
        .await?;
    writer
        .execute("CREATE TABLE values_kept(value INTEGER); INSERT INTO values_kept VALUES(11)")
        .await?;
    writer.close().await?;
    let mut reader = TursoConnectOptions::new()
        .filename(&path)
        .read_only(true)
        .connect()
        .await?;
    assert!(reader.options().get_read_only());
    assert!(!reader.options().get_create_if_missing());
    assert_eq!(
        reader
            .fetch_one("SELECT value FROM values_kept")
            .await?
            .try_get::<i64, _>("value")?,
        11
    );
    for statement in [
        "UPDATE values_kept SET value=99",
        "CREATE TABLE forbidden(value INTEGER)",
    ] {
        let error = reader
            .execute(statement)
            .await
            .expect_err("SDK must refuse writes");
        assert!(
            matches!(error, sqlx_core::Error::Database(ref error) if error.code().as_deref()==Some("SQLITE_READONLY")),
            "{statement}: {error}"
        );
    }
    reader.close().await?;
    let missing = directory.path().join("missing.db");
    assert!(
        TursoConnectOptions::new()
            .filename(&missing)
            .read_only(true)
            .connect()
            .await
            .is_err()
    );
    assert!(!missing.exists());
    Ok(())
}
