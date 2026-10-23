//! Provenance comes from the stored writer origin, with actual read-only opens.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    read_transaction_tests::{ReadSchema, writer},
    retained_file_fixture::{ReadOnlyFixture, TestResult, snapshot},
    *,
};
use fleet_sync_protocol::{CommitSequence, Epoch, ReplicationPosition};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions};

#[tokio::test]
async fn retained_created_origin_preserves_identity_and_zero_start_without_writes() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("created.db");
    let owned = writer(&path).await?;
    let expected = RetainedFileOrigin {
        database: owned.database(),
        epoch: Epoch::new(1)?,
        start: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0)),
        kind: RetainedFileOriginKind::Created,
    };
    owned.close().await;
    drop(owned);
    let _permissions = ReadOnlyFixture::apply(directory.path())?;
    let before = snapshot(directory.path())?;
    for _ in 0..2 {
        let retained = RetainedFile::open_read_only(&path, ReadSchema).await?;
        assert_eq!(retained.origin(), expected);
        assert_eq!(retained.origin(), expected);
        drop(retained);
    }
    assert_eq!(snapshot(directory.path())?, before);
    Ok(())
}

#[tokio::test]
async fn retained_corrupt_origin_is_refused_without_defaulting_or_repair() -> TestResult {
    for corruption in ["missing", "unknown", "null"] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("corrupt.db");
        let owned = writer(&path).await?;
        owned.close().await;
        drop(owned);
        let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
        let raw = query::<Turso>("SELECT record FROM _fleet_sync_writer")
            .fetch_one(&mut connection)
            .await?
            .try_get::<String, _>("record")?;
        let mut record: serde_json::Value = serde_json::from_str(&raw)?;
        match corruption {
            "missing" => {
                record
                    .as_object_mut()
                    .ok_or("writer record is not an object")?
                    .remove("origin");
            }
            "unknown" => record["origin"] = serde_json::json!("future-origin"),
            _ => record["origin"] = serde_json::Value::Null,
        }
        query::<Turso>("UPDATE _fleet_sync_writer SET record=?1")
            .bind(serde_json::to_string(&record)?)
            .execute(&mut connection)
            .await?;
        connection.close().await?;
        let _permissions = ReadOnlyFixture::apply(directory.path())?;
        let before = snapshot(directory.path())?;
        assert!(
            matches!(
                RetainedFile::open_read_only(&path, ReadSchema).await,
                Err(OpenError::Corrupt { .. })
            ),
            "accepted {corruption} origin"
        );
        assert_eq!(snapshot(directory.path())?, before);
    }
    Ok(())
}
