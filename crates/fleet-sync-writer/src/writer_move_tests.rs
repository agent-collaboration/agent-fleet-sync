#![allow(clippy::panic_in_result_fn)]
use crate::{
    writer_release_tests::{TestClock, create},
    *,
};
use fleet_sync_protocol::*;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
};
use std::sync::{Arc, atomic::AtomicI64};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn rejected(
    path: &std::path::Path,
) -> TestResult<Arc<ReplicatedDatabase<crate::writer_release_tests::ReleaseSchema>>> {
    let database = create(path, Arc::new(TestClock(AtomicI64::new(100)))).await?;
    let mut write = database.begin_write().await?;
    (&mut write).execute("INSERT INTO text_records VALUES('019eef00-0000-7000-8000-00000000000a',42,'rejected data')").await?;
    write.commit().await?;
    database
        .reject_registration(GateReason {
            code: GateReasonCode::new("denied")?,
            detail: "fixture rejection".into(),
        })
        .await?;
    Ok(database)
}

#[tokio::test]
async fn moving_a_rejected_file_preserves_wal_data_and_is_terminal_and_idempotent() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source.db");
    let destination = directory.path().join("rejected.db");
    let database = rejected(&source).await?;
    let alias = directory.path().join(".").join("source.db");
    assert!(matches!(
        database.move_aside(&alias).await,
        Err(MoveAsideError::InvalidPaths)
    ));
    assert!(source.exists());
    assert!(std::fs::metadata(directory.path().join("source.db-wal"))?.len() > 0);
    database.move_aside(&destination).await?;
    assert!(!source.exists());
    assert!(destination.exists());
    assert!(database.snapshot().await.is_err());
    assert!(database.begin_write().await.is_err());
    database.move_aside(&destination).await?;
    ReplicatedDatabase::<crate::writer_release_tests::ReleaseSchema>::retry_move_aside(
        database.database(),
        &source,
        &destination,
    )
    .await?;
    let mut connection = sqlx_turso::TursoConnectOptions::new()
        .filename(&destination)
        .create_if_missing(false)
        .connect()
        .await?;
    assert_eq!(
        connection
            .fetch_all("SELECT * FROM text_records")
            .await?
            .len(),
        2
    );
    assert!(matches!(
        crate::writer_file_record::read_file_record(&mut connection)
            .await?
            .ok_or("writer record missing")?
            .authority,
        Some(crate::writer_authority::AuthorityObservation::RegistrationRejected { .. })
    ));
    connection.close().await?;
    assert!(
        std::fs::metadata(directory.path().join("source.db-wal"))
            .map_or(true, |metadata| metadata.len() == 0)
    );
    Ok(())
}

#[tokio::test]
async fn occupied_destination_and_guarded_reader_refuse_before_closing_the_rejected_source()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source.db");
    let destination = directory.path().join("occupied.db");
    let database = rejected(&source).await?;
    std::fs::write(&destination, b"another file")?;
    let before = std::fs::read(&source)?;
    assert!(matches!(
        database.move_aside(&destination).await,
        Err(MoveAsideError::DestinationOccupied)
    ));
    assert_eq!(std::fs::read(&source)?, before);
    assert_eq!(std::fs::read(&destination)?, b"another file");
    let snapshot = database.snapshot().await?;
    let destination = directory.path().join("rejected.db");
    assert!(matches!(
        database.move_aside(&destination).await,
        Err(MoveAsideError::Publish(
            native_file::PublishError::ActiveReaders
        ))
    ));
    assert!(source.exists());
    assert!(!destination.exists());
    drop(snapshot);
    database.move_aside(&destination).await?;
    Ok(())
}

#[tokio::test]
async fn move_requires_durable_rejection_and_a_retry_never_creates_a_missing_source() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source.db");
    let destination = directory.path().join("rejected.db");
    let database = create(&source, Arc::new(TestClock(AtomicI64::new(100)))).await?;
    assert!(matches!(
        database.move_aside(&destination).await,
        Err(MoveAsideError::NotRejected)
    ));
    assert!(database.snapshot().await.is_ok());
    let missing = directory.path().join("missing.db");
    assert!(matches!(
        ReplicatedDatabase::<crate::writer_release_tests::ReleaseSchema>::retry_move_aside(
            database.database(),
            &missing,
            &destination
        )
        .await,
        Err(MoveAsideError::SourceMissing)
    ));
    assert!(!missing.exists());
    assert!(!destination.exists());
    database.close().await;
    Ok(())
}
