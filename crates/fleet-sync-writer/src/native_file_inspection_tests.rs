//! Metadata inspection must neither migrate nor repair the file it classifies.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    read_transaction_tests::writer,
    retained_file_fixture::{ReadOnlyFixture, TestResult, snapshot},
    *,
};
use fleet_sync_protocol::{CommitSequence, Epoch, GateReason, GateReasonCode, ReplicationPosition};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions};

#[tokio::test]
async fn ready_native_file_inspection_preserves_origin_and_every_file() -> TestResult {
    // Arrange a real ready writer, then remove all writable owners.
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("ready.db");
    let database = writer(&path).await?;
    let expected = RetainedFileOrigin {
        database: database.database(),
        epoch: Epoch::new(1)?,
        start: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0)),
        kind: RetainedFileOriginKind::Created,
    };
    database.close().await;
    drop(database);
    let _permissions = ReadOnlyFixture::apply(directory.path())?;
    let before = snapshot(directory.path())?;

    // Act through the metadata-only public entry point, including a repeated open.
    for _ in 0..2 {
        let inspected = inspect_native_file(&path).await?;
        assert_eq!(inspected.origin, expected);
        assert_eq!(inspected.bootstrap, None);
        assert_eq!(inspected.authority, None);
    }
    assert_eq!(snapshot(directory.path())?, before);
    Ok(())
}

#[tokio::test]
async fn retained_native_file_inspection_preserves_the_recorded_authority_loss() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("writer.db");
    let path = directory.path().join("retained.db");
    let database = writer(&source).await?;
    let identity = database.database();
    let reason = GateReason {
        code: GateReasonCode::new("fixture_rejected")?,
        detail: "retain after rejection".into(),
    };
    database.reject_registration(reason.clone()).await?;
    database.move_aside(&path).await?;
    drop(database);
    let _permissions = ReadOnlyFixture::apply(directory.path())?;
    let before = snapshot(directory.path())?;

    for _ in 0..2 {
        let inspected = inspect_native_file(&path).await?;
        assert_eq!(
            inspected.origin,
            RetainedFileOrigin {
                database: identity,
                epoch: Epoch::new(1)?,
                start: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0)),
                kind: RetainedFileOriginKind::Created,
            }
        );
        assert_eq!(inspected.bootstrap, None);
        assert_eq!(
            inspected.authority,
            Some(RetainedAuthority::RegistrationRejected {
                reason: reason.clone(),
            })
        );
    }
    assert!(!source.exists());
    assert_eq!(snapshot(directory.path())?, before);
    Ok(())
}

#[tokio::test]
async fn native_file_inspection_refuses_corruption_and_never_creates_a_missing_file() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let missing = directory.path().join("missing.db");
    let empty = snapshot(directory.path())?;
    assert!(inspect_native_file(&missing).await.is_err());
    assert_eq!(snapshot(directory.path())?, empty);

    let path = directory.path().join("corrupt.db");
    let database = writer(&path).await?;
    database.close().await;
    drop(database);
    // Inject an invalid origin at the stored boundary; only the library decodes it.
    let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
    let encoded: String = query::<Turso>("SELECT record FROM _fleet_sync_writer")
        .fetch_one(&mut connection)
        .await?
        .try_get("record")?;
    let mut record: serde_json::Value = serde_json::from_str(&encoded)?;
    record["origin"] = serde_json::json!("unknown-origin");
    query::<Turso>("UPDATE _fleet_sync_writer SET record=?1")
        .bind(serde_json::to_string(&record)?)
        .execute(&mut connection)
        .await?;
    connection.close().await?;
    let _permissions = ReadOnlyFixture::apply(directory.path())?;
    let before = snapshot(directory.path())?;
    for _ in 0..2 {
        assert!(matches!(
            inspect_native_file(&path).await,
            Err(OpenError::Corrupt { .. })
        ));
    }
    assert_eq!(snapshot(directory.path())?, before);
    Ok(())
}
