#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{
    ControlAnswer, ControlRequest, TestResult, WriterCommand, WriterReply,
};
use support::{writer_process::WriterProcess, *};

#[tokio::test]
async fn idle_writer_answers_release_and_clean_move_keeps_every_accepted_row() -> TestResult {
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start(&root.path().join("service")).await?;
    let endpoint = format!("ws://127.0.0.1:{}/session", service.port);
    let mut writer =
        WriterProcess::start(&root.path().join("old-writer"), &endpoint, "adopt", None).await?;
    let database = writer.database;
    writer.command(WriterCommand::Register).await?;
    println!("OBS old writer registration admitted");
    writer.command(WriterCommand::WaitReady).await?;
    println!("OBS old writer seed ready");
    let through = match writer.command(WriterCommand::Position).await? {
        WriterReply::Position(position) => position,
        _ => return Err("writer position missing".into()),
    };
    writer.command(WriterCommand::WaitAcked { through }).await?;
    println!("OBS old writer copy acknowledged {through:?}");
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let _capability_request = service
        .peer(4, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let request = HolderChangeRequest::Move {
        attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
        database,
        from_epoch: through.epoch,
        to: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000004,
        ))?,
    };
    println!("OBS requesting clean move");
    let mut moved =
        match control_request(&mut control, ControlRequest::HolderChange(request.clone())).await? {
            ControlAnswer::HolderChange(answer) => answer,
            _ => return Err("move answer missing".into()),
        };
    if matches!(moved, HolderChangeAnswer::Pending { .. }) {
        moved = match control_request(
            &mut control,
            ControlRequest::WaitHolderChange(request.clone()),
        )
        .await?
        {
            ControlAnswer::HolderChange(answer) => answer,
            _ => return Err("move completion missing".into()),
        };
    }
    println!("OBS idle writer clean move: {moved:?}");
    assert!(
        matches!(moved,HolderChangeAnswer::Granted {epoch,start,..} if epoch==Epoch::new(2)? && start==through)
    );
    assert!(matches!(
        writer
            .command(WriterCommand::TryWrite {
                label: "must be refused after release".into()
            })
            .await?,
        WriterReply::WriteRefused { .. }
    ));
    let replay = match control_request(&mut control, ControlRequest::HolderChange(request)).await? {
        ControlAnswer::HolderChange(answer) => answer,
        _ => return Err("move replay missing".into()),
    };
    assert_eq!(replay, moved);
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let rows = copy_rows(&mut reader, database).await?;
    assert_eq!(rows.get("samples").map(Vec::len), Some(301));
    assert!(rows.get("samples").ok_or("samples missing")?.iter().all(|row|matches!(row.as_slice(),[SqlValue::Text(id),SqlValue::Text(_),SqlValue::Integer(9_007_199_254_740_993),SqlValue::Blob(bytes)] if canonical_uuid_v7(id) && bytes==&[0,255,128])));
    writer.stop().await?;
    service.stop(&mut control).await?;
    Ok(())
}
