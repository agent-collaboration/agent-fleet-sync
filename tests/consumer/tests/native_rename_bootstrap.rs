#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{
    ControlAnswer, ControlRequest, TestResult, WriterCommand, WriterReply,
};
use fleet_sync_tokio_runner::InstallStatus;
use support::{writer_process::WriterProcess, *};

const KEY: &str = "019eef00-0000-7000-8000-000000000001";

#[tokio::test]
async fn new_writer_publishes_v1_prefix_then_renames_writes_and_replicates() -> TestResult {
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start(&root.path().join("service")).await?;
    let endpoint = format!("ws://127.0.0.1:{}/session", service.port);
    let mut old =
        WriterProcess::start(&root.path().join("old"), &endpoint, "rename-source", None).await?;
    let database = old.database;
    old.command(WriterCommand::Register).await?;
    old.command(WriterCommand::WaitReady).await?;
    let through = position(old.command(WriterCommand::Position).await?)?;
    old.command(WriterCommand::WaitAcked { through }).await?;
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let expected = vec![vec![
        SqlValue::Text(KEY.into()),
        SqlValue::Text("before".into()),
    ]];
    let rows = copy_rows(&mut reader, database).await?;
    assert_eq!(rows.get("records"), Some(&expected));
    assert!(!rows.contains_key("tasks"));
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
        ControlAnswer::HolderChange(HolderChangeAnswer::Granted { epoch, start, .. })
        if epoch == Epoch::new(2)? && start == through)
    );

    // This writer offers both migrations and declares only tasks. The received
    // snapshot is still v1/records and must be published before v2 runs.
    let mut writer = WriterProcess::start(
        &root.path().join("new"),
        &endpoint,
        "rename-bootstrap",
        Some(database),
    )
    .await?;
    let attempt = InstallAttemptId::new(durable_steps::Uuid::now_v7())?;
    writer
        .command(WriterCommand::InstallSnapshot { attempt })
        .await?;
    let installed = match writer
        .command(WriterCommand::WaitInstall { attempt })
        .await?
    {
        WriterReply::Install(record) => record,
        _ => return Err("install record missing".into()),
    };
    assert!(matches!(installed.status, InstallStatus::Ready { snapshot }
        if snapshot.schema.version == 1 && snapshot.position == through));
    let published = match writer.command(WriterCommand::SnapshotManifest).await? {
        WriterReply::SnapshotManifest(manifest) => manifest,
        _ => return Err("published writer manifest missing".into()),
    };
    assert_eq!(published.schema.version, 1);
    assert_eq!(published.tables.len(), 1);
    assert_eq!(published.tables[0].table.as_str(), "records");
    assert!(matches!(writer.command(WriterCommand::State).await?,
        WriterReply::State(fleet_sync_lease::WriterState::Writing { epoch }) if epoch == Epoch::new(2)?));
    let migrated = position(writer.command(WriterCommand::Migrate).await?)?;
    writer
        .command(WriterCommand::WaitAcked { through: migrated })
        .await?;
    let rows = copy_rows(&mut reader, database).await?;
    assert_eq!(rows.get("tasks"), Some(&expected));
    assert!(!rows.contains_key("records"));
    let next = position(
        writer
            .command(WriterCommand::WriteRenamedRecord {
                value: "after bootstrap rename".into(),
            })
            .await?,
    )?;
    writer
        .command(WriterCommand::WaitAcked { through: next })
        .await?;
    let rows = copy_rows(&mut reader, database).await?;
    assert_eq!(
        rows.get("tasks"),
        Some(&vec![vec![
            SqlValue::Text(KEY.into()),
            SqlValue::Text("after bootstrap rename".into())
        ]])
    );
    assert!(!rows.contains_key("records"));
    let published = match writer.command(WriterCommand::SnapshotManifest).await? {
        WriterReply::SnapshotManifest(manifest) => manifest,
        _ => return Err("renamed writer manifest missing".into()),
    };
    assert_eq!(published.schema.version, 2);
    assert_eq!(published.position, next);
    assert_eq!(published.tables[0].table.as_str(), "tasks");
    writer.stop().await?;
    old.stop().await?;
    service.stop(&mut control).await?;
    Ok(())
}
fn position(reply: WriterReply) -> TestResult<ReplicationPosition> {
    match reply {
        WriterReply::Position(position) => Ok(position),
        _ => Err("writer position missing".into()),
    }
}
