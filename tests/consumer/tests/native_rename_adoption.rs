#![allow(clippy::panic_in_result_fn)]
#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{SampleSchema, TestResult, WriterCommand, WriterReply};
use fleet_sync_writer::{
    OpenError, ReplicatedDatabase, ReplicatedSchema, WriteRefusal, WriterOptions,
};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    migrate::Migrator,
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::{path::Path, time::Duration};
use support::{writer_process::WriterProcess, *};
const KEY: &str = "019eef00-0000-7000-8000-000000000001";

async fn pre_capture(path: &Path, key: &str) -> TestResult {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .connect()
        .await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    Migrator::with_migrations(SampleSchema::rename_prefix()?.migrations().to_vec())
        .run_direct(None, &mut *transaction, false)
        .await?;
    query::<Turso>("UPDATE records SET id=?,value='pre-capture preserved'")
        .bind(key)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    connection.close().await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_v1_adoption_migrates_rename_and_seeds_then_follows_the_copy() -> TestResult {
    let root = tempfile::tempdir()?;
    let directory = root.path().join("writer");
    tokio::fs::create_dir_all(&directory).await?;
    pre_capture(&directory.join("writer.db"), KEY).await?;
    let service = ServiceProcess::start(&root.path().join("service")).await?;
    let endpoint = format!("ws://127.0.0.1:{}/session", service.port);
    // adopt owns migration of the unapplied v2 after checking the exact v1 history.
    let mut writer = WriterProcess::start(&directory, &endpoint, "rename-adopt", None).await?;
    writer.command(WriterCommand::Register).await?;
    writer.command(WriterCommand::WaitReady).await?;
    let through = match writer.command(WriterCommand::Position).await? {
        WriterReply::Position(position) => position,
        _ => return Err("adopted writer position missing".into()),
    };
    writer.command(WriterCommand::WaitAcked { through }).await?;
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let rows = copy_rows(&mut reader, writer.database).await?;
    assert_eq!(
        rows.get("tasks"),
        Some(&vec![vec![
            SqlValue::Text(KEY.into()),
            SqlValue::Text("pre-capture preserved".into())
        ]])
    );
    assert!(!rows.contains_key("records"));
    let manifest = match writer.command(WriterCommand::SnapshotManifest).await? {
        WriterReply::SnapshotManifest(manifest) => manifest,
        _ => return Err("adopted writer manifest missing".into()),
    };
    assert_eq!(manifest.schema.version, 2);
    assert_eq!(manifest.tables.len(), 1);
    assert_eq!(manifest.tables[0].table.as_str(), "tasks");
    let next = match writer
        .command(WriterCommand::WriteRenamedRecord {
            value: "adopted follows".into(),
        })
        .await?
    {
        WriterReply::Position(position) => position,
        _ => return Err("adopted writer write refused".into()),
    };
    writer
        .command(WriterCommand::WaitAcked { through: next })
        .await?;
    let rows = copy_rows(&mut reader, writer.database).await?;
    assert_eq!(
        rows.get("tasks"),
        Some(&vec![vec![
            SqlValue::Text(KEY.into()),
            SqlValue::Text("adopted follows".into())
        ]])
    );
    assert!(!rows.contains_key("records"));
    writer.stop().await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    service.stop(&mut control).await?;
    Ok(())
}

#[tokio::test]
async fn sqlx_v1_adoption_still_refuses_an_invalid_key_before_capture_setup() -> TestResult {
    let root = tempfile::tempdir()?;
    let path = root.path().join("bad-prefix.db");
    pre_capture(&path, "not-a-uuid").await?;
    let result = ReplicatedDatabase::adopt(
        &path,
        SampleSchema::rename_bundle()?,
        PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000002,
        ))?,
        LeasePolicy {
            lease_duration: Duration::from_secs(600),
            renew_interval: Duration::from_secs(30),
        },
        WriterOptions::default(),
    )
    .await;
    assert!(
        matches!(result, Err(OpenError::WriteRefused { refusal: WriteRefusal::InvalidKey { table } }) if table == "records")
    );
    let mut connection = TursoConnectOptions::new()
        .filename(&path)
        .create_if_missing(false)
        .connect()
        .await?;
    assert_eq!(
        query::<Turso>("SELECT id,value FROM records")
            .fetch_one(&mut connection)
            .await?
            .try_get::<String, _>("id")?,
        "not-a-uuid"
    );
    assert_eq!(
        query::<Turso>("SELECT version FROM _sqlx_migrations")
            .fetch_all(&mut connection)
            .await?
            .len(),
        1
    );
    assert!(query::<Turso>("SELECT name FROM sqlite_schema WHERE name IN ('_fleet_sync_writer','turso_cdc','tasks')").fetch_all(&mut connection).await?.is_empty());
    connection.close().await?;
    Ok(())
}
