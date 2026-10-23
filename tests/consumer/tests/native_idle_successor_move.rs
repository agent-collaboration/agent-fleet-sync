#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{
    ControlAnswer, ControlRequest, TestResult, WriterCommand, WriterReply,
};
use support::{writer_process::WriterProcess, *};

async fn move_holder(
    control: &mut fleet_sync_transport_websocket::WebSocketConnection,
    database: DatabaseId,
    from: u64,
    to: u128,
) -> TestResult<(HolderChangeRequest, HolderChangeAnswer)> {
    let request = HolderChangeRequest::Move {
        reason: HolderChangeReason::new("move idle successor")?,
        attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
        database,
        from_epoch: Epoch::new(from)?,
        to: PeerId::new(durable_steps::Uuid::from_u128(
            to | 0x019eef00000070008000000000000000,
        ))?,
    };
    let admitted = control_request(control, ControlRequest::HolderChange(request.clone())).await?;
    let answer = match admitted {
        ControlAnswer::HolderChange(HolderChangeAnswer::Pending { .. }) => {
            match control_request(control, ControlRequest::WaitHolderChange(request.clone()))
                .await?
            {
                ControlAnswer::HolderChange(answer) => answer,
                _ => return Err("move completion missing".into()),
            }
        }
        ControlAnswer::HolderChange(answer) => answer,
        _ => return Err("move admission missing".into()),
    };
    Ok((request, answer))
}

#[tokio::test]
async fn seeded_successor_moves_without_a_write_in_its_epoch_and_keeps_inherited_rows() -> TestResult
{
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start(&root.path().join("service")).await?;
    let endpoint = format!("ws://127.0.0.1:{}/session", service.port);
    let mut original =
        WriterProcess::start(&root.path().join("original"), &endpoint, "adopt", None).await?;
    let database = original.database;
    original.command(WriterCommand::Register).await?;
    original.command(WriterCommand::WaitReady).await?;
    let inherited = match original.command(WriterCommand::Position).await? {
        WriterReply::Position(position) => position,
        _ => return Err("original position missing".into()),
    };
    original
        .command(WriterCommand::WaitAcked { through: inherited })
        .await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let _next_capability = service
        .peer(4, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let (_, first) = move_holder(&mut control, database, 1, 4).await?;
    assert!(
        matches!(first, HolderChangeAnswer::Granted { epoch, start, .. }
        if epoch == Epoch::new(2)? && start == inherited)
    );

    let mut successor = WriterProcess::start(
        &root.path().join("successor"),
        &endpoint,
        "bootstrap-current",
        Some(database),
    )
    .await?;
    let attempt = InstallAttemptId::new(durable_steps::Uuid::now_v7())?;
    successor
        .command(WriterCommand::InstallSnapshot { attempt })
        .await?;
    successor
        .command(WriterCommand::WaitInstall { attempt })
        .await?;
    assert!(
        matches!(successor.command(WriterCommand::State).await?, WriterReply::State(
        fleet_sync_lease::WriterState::Writing { epoch }) if epoch == Epoch::new(2)?)
    );
    assert!(
        matches!(successor.command(WriterCommand::Position).await?,
        WriterReply::Position(position) if position == inherited),
        "installation must not write epoch two"
    );
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let before = copy_rows(&mut reader, database).await?;

    let (request, moved) = move_holder(&mut control, database, 2, 5).await?;
    println!("idle successor move answer: {moved:?}");

    assert!(
        matches!(moved, HolderChangeAnswer::Granted { epoch, start, .. }
        if epoch == Epoch::new(3)? && start == inherited)
    );
    let state = successor.command(WriterCommand::State).await?;
    match state {
        WriterReply::State(fleet_sync_lease::WriterState::Released {
            epoch,
            final_position,
        }) => {
            assert_eq!(epoch, Epoch::new(2)?);
            assert_eq!(final_position, inherited);
        }
        WriterReply::State(fleet_sync_lease::WriterState::Superseded { current }) => {
            assert!(
                current >= Epoch::new(3)?,
                "only the granted successor can supersede this writer"
            );
        }
        other => {
            return Err(format!("idle released writer has unexpected authority: {other:?}").into());
        }
    }
    assert_eq!(copy_rows(&mut reader, database).await?, before);
    assert_eq!(before.get("samples").map(Vec::len), Some(301));
    let replay = control_request(&mut control, ControlRequest::HolderChange(request)).await?;
    assert!(matches!(replay, ControlAnswer::HolderChange(answer) if answer == moved));
    assert!(matches!(
        successor
            .command(WriterCommand::TryWrite {
                label: "released idle holder".into()
            })
            .await?,
        WriterReply::WriteRefused { .. }
    ));
    println!("idle successor Move completed: epoch2 -> epoch3, inherited={inherited:?}");
    successor.stop().await?;
    original.stop().await?;
    service.stop(&mut control).await?;
    Ok(())
}
