#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{
    ControlAnswer, ControlRequest, TestResult, WriterCommand, WriterReply,
};
use fleet_sync_tokio_runner::InstallStatus;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
};
use support::{writer_process::WriterProcess, *};

#[tokio::test]
async fn new_writer_resumes_converted_snapshot_after_process_kill_then_migrates_and_follows()
-> TestResult {
    bootstrap_kill(false).await
}
#[tokio::test]
async fn new_writer_resumes_published_snapshot_after_process_kill_before_reopen() -> TestResult {
    bootstrap_kill(true).await
}
async fn bootstrap_kill(after_rename: bool) -> TestResult {
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start(&root.path().join("service")).await?;
    let endpoint = format!("ws://127.0.0.1:{}/session", service.port);
    let mut old = WriterProcess::start(&root.path().join("old"), &endpoint, "adopt", None).await?;
    let database = old.database;
    old.command(WriterCommand::Register).await?;
    old.command(WriterCommand::WaitReady).await?;
    let through = match old.command(WriterCommand::Position).await? {
        WriterReply::Position(position) => position,
        _ => return Err("writer position missing".into()),
    };
    old.command(WriterCommand::WaitAcked { through }).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let change = HolderChangeRequest::Move {
        attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
        database,
        from_epoch: through.epoch,
        to: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000004,
        ))?,
    };
    control_request(&mut control, ControlRequest::HolderChange(change.clone())).await?;
    assert!(
        matches!(control_request(&mut control, ControlRequest::WaitHolderChange(change)).await?,
        ControlAnswer::HolderChange(HolderChangeAnswer::Granted { epoch, start, .. }) if epoch==Epoch::new(2)? && start==through)
    );
    let path = root.path().join("new");
    let mut writer = WriterProcess::start(&path, &endpoint, "bootstrap", Some(database)).await?;
    let session = writer.session;
    assert!(matches!(
        writer
            .command(WriterCommand::TryWrite {
                label: "must stay seeding".into()
            })
            .await?,
        WriterReply::WriteRefused { seeding: true, .. }
    ));
    writer
        .command(if after_rename {
            WriterCommand::HoldBootstrapAfterRename
        } else {
            WriterCommand::HoldBootstrapBeforeRename
        })
        .await?;
    let attempt = InstallAttemptId::new(durable_steps::Uuid::now_v7())?;
    writer
        .command(WriterCommand::InstallSnapshot { attempt })
        .await?;
    let converted = match (
        after_rename,
        writer
            .command(if after_rename {
                WriterCommand::WaitBootstrapPublished
            } else {
                WriterCommand::WaitBootstrapConverted
            })
            .await?,
    ) {
        (false, WriterReply::Converted(snapshot))
        | (true, WriterReply::PublishedSnapshot(snapshot)) => snapshot,
        _ => return Err("converted snapshot missing".into()),
    };
    assert_eq!(converted.position, through);
    writer.kill().await?;
    let mut writer =
        WriterProcess::start(&path, &endpoint, "resume-bootstrap", Some(database)).await?;
    assert_eq!(writer.session, session);
    let ready = match writer
        .command(WriterCommand::WaitInstall { attempt })
        .await?
    {
        WriterReply::Install(record) => record,
        _ => return Err("install completion missing".into()),
    };
    assert_eq!(ready.install_id, converted.install_id);
    assert_eq!(ready.generation, 1);
    assert!(matches!(ready.status, InstallStatus::Ready { snapshot } if snapshot==converted));
    assert!(
        matches!(writer.command(WriterCommand::State).await?, WriterReply::State(fleet_sync_lease::WriterState::Writing { epoch }) if epoch==Epoch::new(2)?)
    );
    let migrated = match writer.command(WriterCommand::Migrate).await? {
        WriterReply::Position(position) => position,
        _ => return Err("migration position missing".into()),
    };
    writer
        .command(WriterCommand::WaitAcked { through: migrated })
        .await?;
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let rows = copy_rows(&mut reader, database).await?;
    let samples = rows.get("samples").ok_or("samples missing")?;
    assert_eq!(samples.len(), 301);
    assert!(samples.iter().all(|row| matches!(row.as_slice(), [SqlValue::Text(id), SqlValue::Text(_), SqlValue::Integer(9_007_199_254_740_993), SqlValue::Blob(bytes), SqlValue::Text(note)] if canonical_uuid_v7(id) && bytes==&[0,255,128] && note=="after bootstrap")));
    let next = match writer
        .command(WriterCommand::TryWrite {
            label: "new epoch follows".into(),
        })
        .await?
    {
        WriterReply::Position(position) => position,
        _ => return Err("new writer refused its write".into()),
    };
    writer
        .command(WriterCommand::WaitAcked { through: next })
        .await?;
    let rows = copy_rows(&mut reader, database).await?;
    assert_eq!(rows.get("samples").map(Vec::len), Some(302));
    assert!(matches!(
        old.command(WriterCommand::TryWrite {
            label: "old holder".into()
        })
        .await?,
        WriterReply::WriteRefused { .. }
    ));
    writer.stop().await?;
    // Inspect the real published file after its pools and owner are closed.
    let mut connection = sqlx_turso::TursoConnectOptions::new()
        .filename(path.join("writer.db"))
        .create_if_missing(false)
        .connect()
        .await?;
    for (table, query) in [
        ("_fleet_sync_install", "SELECT * FROM _fleet_sync_install"),
        (
            "_fleet_sync_install_pages",
            "SELECT * FROM _fleet_sync_install_pages",
        ),
        (
            "_fleet_sync_copy_migrations",
            "SELECT * FROM _fleet_sync_copy_migrations",
        ),
        (
            "_fleet_sync_copy_tables",
            "SELECT * FROM _fleet_sync_copy_tables",
        ),
    ] {
        let rows = connection.fetch_all(query).await?;
        assert!(rows.is_empty(), "receiver bookkeeping remained in {table}");
    }
    connection.close().await?;
    old.stop().await?;
    service.stop(&mut control).await?;
    Ok(())
}
