#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_lease::{
    CancelAnswer, CancelHolderChange, CancelReason, HolderChangePhase, WaitingOn, WriterState,
};
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::{
    ControlAnswer, ControlRequest, TestResult, WriterCommand, WriterReply,
};
use fleet_sync_tokio_runner::WriterTransportObservation;
use support::{writer_process::WriterProcess, *};

#[tokio::test]
async fn cancel_unanswered_release_restores_same_epoch_after_pruning_and_writer_reopen()
-> TestResult {
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start(&root.path().join("service")).await?;
    let endpoint = format!("ws://127.0.0.1:{}/session", service.port);
    let writer_path = root.path().join("writer");
    let mut writer = WriterProcess::start(&writer_path, &endpoint, "adopt", None).await?;
    let (database, session) = (writer.database, writer.session);
    writer.command(WriterCommand::Register).await?;
    writer.command(WriterCommand::WaitReady).await?;
    println!("OBS cancellation writer registered");
    let before = match writer
        .command(WriterCommand::TryWrite {
            label: "before cancellation".into(),
        })
        .await?
    {
        WriterReply::Position(position) => position,
        _ => return Err("writer refused before move".into()),
    };
    writer
        .command(WriterCommand::WaitAcked { through: before })
        .await?;
    assert!(matches!(
        writer.command(WriterCommand::PruneAcknowledged).await?,
        WriterReply::CaptureRows(0)
    ));
    writer.command(WriterCommand::HoldReleaseAnswer).await?;
    println!("OBS cancellation capture acknowledged and pruned");
    let attempt = TransferId::new(durable_steps::Uuid::now_v7())?;
    let _capability_change = service
        .peer(4, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let change = HolderChangeRequest::Move {
        attempt,
        database,
        from_epoch: before.epoch,
        to: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000004,
        ))?,
    };
    let mut starting = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let submitted = change.clone();
    let moving = tokio::spawn(async move {
        let answer = control_request(&mut starting, ControlRequest::HolderChange(submitted)).await;
        starting.close(CloseReason::ClientClosed).await;
        answer
    });
    let (release_request, release_call) =
        match writer.command(WriterCommand::WaitTransportCall).await? {
            WriterReply::TransportCall(WriterTransportObservation::Released {
                call,
                request,
                answer: ReleaseAnswer::Released { final_position },
            }) if final_position == before => (request, call),
            _ => return Err("release did not drain to the acknowledged position".into()),
        };
    println!("OBS cancellation release committed, answer held");
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let view =
        match control_request(&mut control, ControlRequest::HolderChangeView { attempt }).await? {
            ControlAnswer::HolderChangeView(Some(view)) => view,
            _ => return Err("pending move has no admin view".into()),
        };
    println!("OBS cancellation admin view returned");
    assert_eq!(view.phase, HolderChangePhase::Releasing);
    assert_eq!(view.requested_by, ActorRef::new("consumer-control")?);
    assert!(
        matches!(view.waiting, Some(waiting) if waiting.on==WaitingOn::Holder(PeerId::new(durable_steps::Uuid::from_u128(0x019eef00000070008000000000000002))?))
    );
    let by = ActorRef::new("test-admin")?;
    let cancel = CancelHolderChange {
        attempt,
        by: by.clone(),
        reason: CancelReason::new("keep the existing writer")?,
    };
    assert!(matches!(
        control_request(
            &mut control,
            ControlRequest::CancelHolderChange(cancel.clone())
        )
        .await?,
        ControlAnswer::Cancel(CancelAnswer::Accepted {
            phase: HolderChangePhase::Releasing,
            ..
        })
    ));
    // The cancellation is durable before the actual Release answer is sent.
    println!("OBS cancellation recorded before release answer");
    writer.command(WriterCommand::ReleaseTransportCall).await?;
    println!("OBS cancellation release answer sent");
    assert!(matches!(moving.await??, ControlAnswer::HolderChange(_)));
    println!("OBS cancellation move returned");
    assert!(
        matches!(control_request(&mut control, ControlRequest::WaitHolderChange(change.clone())).await?, ControlAnswer::HolderChange(HolderChangeAnswer::Cancelled { by: actual }) if actual==by)
    );
    assert!(
        matches!(writer.command(WriterCommand::State).await?, WriterReply::State(WriterState::Writing { epoch }) if epoch==before.epoch)
    );
    assert!(matches!(
        control_request(
            &mut control,
            ControlRequest::ReplayHolderRelease {
                holder: PeerId::new(durable_steps::Uuid::from_u128(
                    0x019eef00000070008000000000000002
                ))?,
                request: release_request,
                call: release_call
            }
        )
        .await?,
        ControlAnswer::Release(ReleaseAnswer::ReleaseRefused(ReleaseRefusal::Withdrawn))
    ));
    let after = match writer
        .command(WriterCommand::TryWrite {
            label: "after withdrawal".into(),
        })
        .await?
    {
        WriterReply::Position(position) => position,
        _ => return Err("withdrawn writer did not resume".into()),
    };
    assert_eq!(after.epoch, before.epoch);
    assert!(after > before);
    writer
        .command(WriterCommand::WaitAcked { through: after })
        .await?;
    writer.stop().await?;
    let mut writer =
        WriterProcess::start(&writer_path, &endpoint, "resume", Some(database)).await?;
    assert_eq!(writer.session, session);
    writer
        .command(WriterCommand::WaitAcked { through: after })
        .await?;
    assert!(matches!(
        writer.command(WriterCommand::PruneAcknowledged).await?,
        WriterReply::CaptureRows(0)
    ));
    let reopened = match writer
        .command(WriterCommand::TryWrite {
            label: "after authority reopens".into(),
        })
        .await?
    {
        WriterReply::Position(position) => position,
        _ => return Err("reopened withdrawn writer refused admission".into()),
    };
    assert_eq!(reopened.epoch, before.epoch);
    assert!(reopened > after);
    writer
        .command(WriterCommand::WaitAcked { through: reopened })
        .await?;
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let rows = copy_rows(&mut reader, database).await?;
    assert_eq!(rows.get("samples").map(Vec::len), Some(304));
    assert!(rows.get("samples").ok_or("samples missing")?.iter().all(|row| matches!(row.as_slice(),[SqlValue::Text(id),SqlValue::Text(_),SqlValue::Integer(9_007_199_254_740_993),SqlValue::Blob(bytes)] if canonical_uuid_v7(id) && bytes==&[0,255,128])));
    writer.stop().await?;
    service.stop(&mut control).await?;
    Ok(())
}

#[tokio::test]
async fn cancel_answered_release_while_copy_is_behind_prevents_fencing_and_resumes_writer()
-> TestResult {
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start(&root.path().join("service")).await?;
    let endpoint = format!("ws://127.0.0.1:{}/session", service.port);
    let mut writer =
        WriterProcess::start(&root.path().join("writer"), &endpoint, "adopt", None).await?;
    let database = writer.database;
    writer.command(WriterCommand::Register).await?;
    writer.command(WriterCommand::WaitReady).await?;
    writer.command(WriterCommand::HoldPiece).await?;
    let position = match writer
        .command(WriterCommand::TryWrite {
            label: "accepted before release".into(),
        })
        .await?
    {
        WriterReply::Position(position) => position,
        _ => return Err("writer refused its accepted row".into()),
    };
    assert!(
        matches!(writer.command(WriterCommand::WaitTransportCall).await?, WriterReply::TransportCall(WriterTransportObservation::SendingPiece { through, .. }) if through==position)
    );
    let attempt = TransferId::new(durable_steps::Uuid::now_v7())?;
    let _capability_change = service
        .peer(4, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let change = HolderChangeRequest::Move {
        attempt,
        database,
        from_epoch: position.epoch,
        to: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000004,
        ))?,
    };
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    control_request(&mut control, ControlRequest::HolderChange(change.clone())).await?;
    let phase = HolderChangePhase::Released {
        final_position: position,
    };
    let view = match control_request(
        &mut control,
        ControlRequest::WaitHolderChangePhase {
            attempt,
            phase: phase.clone(),
        },
    )
    .await?
    {
        ControlAnswer::HolderChangeView(Some(view)) => view,
        _ => return Err("released phase missing".into()),
    };
    assert_eq!(view.phase, phase);
    assert!(
        matches!(writer.command(WriterCommand::State).await?, WriterReply::State(WriterState::Released { final_position, .. }) if final_position==position)
    );
    let by = ActorRef::new("test-admin")?;
    assert!(matches!(
        control_request(
            &mut control,
            ControlRequest::CancelHolderChange(CancelHolderChange {
                attempt,
                by: by.clone(),
                reason: CancelReason::new("cancel after holder answered")?
            })
        )
        .await?,
        ControlAnswer::Cancel(CancelAnswer::Accepted {
            phase: HolderChangePhase::Released { .. },
            ..
        })
    ));
    writer.command(WriterCommand::ReleaseTransportCall).await?;
    assert!(
        matches!(control_request(&mut control, ControlRequest::WaitHolderChange(change.clone())).await?, ControlAnswer::HolderChange(HolderChangeAnswer::Cancelled { by: actual }) if actual==by)
    );
    let next = match writer
        .command(WriterCommand::TryWrite {
            label: "released holder resumed".into(),
        })
        .await?
    {
        WriterReply::Position(position) => position,
        _ => return Err("released holder did not resume".into()),
    };
    assert_eq!(next.epoch, position.epoch);
    writer
        .command(WriterCommand::WaitAcked { through: next })
        .await?;
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    assert_eq!(
        copy_rows(&mut reader, database)
            .await?
            .get("samples")
            .map(Vec::len),
        Some(303)
    );
    assert!(
        matches!(control_request(&mut control, ControlRequest::CopyStatus { database }).await?, ControlAnswer::CopyStatus(CopyStatusAnswer::Status(status)) if status.fence==position.epoch && status.acked==next)
    );
    assert!(matches!(
        control_request(&mut control, ControlRequest::HolderChange(change)).await?,
        ControlAnswer::HolderChange(HolderChangeAnswer::Cancelled { .. })
    ));
    writer.stop().await?;
    service.stop(&mut control).await?;
    Ok(())
}

#[tokio::test]
async fn takeover_supersedes_a_cancelling_move_with_an_unanswered_withdrawal() -> TestResult {
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start(&root.path().join("service")).await?;
    let endpoint = format!("ws://127.0.0.1:{}/session", service.port);
    let mut writer =
        WriterProcess::start(&root.path().join("writer"), &endpoint, "adopt", None).await?;
    let database = writer.database;
    writer.command(WriterCommand::Register).await?;
    writer.command(WriterCommand::WaitReady).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    // Prepare registry work before holding withdrawal. Preview drives the
    // registry engine; doing it during the hold can let an unheld retry finish
    // cancellation before the takeover is even admitted.
    let _capability_takeover = service
        .peer(6, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let preview = match control_request(
        &mut control,
        ControlRequest::PreviewTakeover {
            database,
            attempt: durable_steps::Uuid::now_v7(),
        },
    )
    .await?
    {
        ControlAnswer::TakeoverPreview(preview) => preview,
        _ => return Err("takeover preview missing".into()),
    };
    writer.command(WriterCommand::HoldReleaseAnswer).await?;
    let epoch = Epoch::new(1)?;
    let attempt = TransferId::new(durable_steps::Uuid::now_v7())?;
    let _capability_change = service
        .peer(4, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let change = HolderChangeRequest::Move {
        attempt,
        database,
        from_epoch: epoch,
        to: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000004,
        ))?,
    };
    control_request(&mut control, ControlRequest::HolderChange(change.clone())).await?;
    assert!(matches!(
        writer.command(WriterCommand::WaitTransportCall).await?,
        WriterReply::TransportCall(WriterTransportObservation::Released { .. })
    ));
    control_request(
        &mut control,
        ControlRequest::CancelHolderChange(CancelHolderChange {
            attempt,
            by: ActorRef::new("test-admin")?,
            reason: CancelReason::new("withdraw this move")?,
        }),
    )
    .await?;
    writer.command(WriterCommand::HoldWithdrawalAnswer).await?;
    writer.command(WriterCommand::ReleaseTransportCall).await?;
    assert!(matches!(
        writer.command(WriterCommand::WaitTransportCall).await?,
        WriterReply::TransportCall(WriterTransportObservation::Withdrawn {
            answer: WithdrawAnswer::Withdrawn,
            ..
        })
    ));
    let view =
        match control_request(&mut control, ControlRequest::HolderChangeView { attempt }).await? {
            ControlAnswer::HolderChangeView(Some(view)) => view,
            _ => return Err("cancelling move missing".into()),
        };
    assert_eq!(view.phase, HolderChangePhase::Cancelling);
    assert!(view.outcome.is_none());
    let takeover = HolderChangeRequest::Takeover {
        attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
        database,
        preview,
        to: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000006,
        ))?,
    };
    control_request(&mut control, ControlRequest::HolderChange(takeover.clone())).await?;
    let granted =
        match control_request(&mut control, ControlRequest::WaitHolderChange(takeover)).await? {
            ControlAnswer::HolderChange(HolderChangeAnswer::Granted { epoch, .. }) => epoch,
            _ => return Err("takeover did not grant".into()),
        };
    assert!(granted > Epoch::new(2)?);
    let old_move = control_request(&mut control, ControlRequest::WaitHolderChange(change)).await?;
    assert!(
        matches!(
            old_move,
            ControlAnswer::HolderChange(HolderChangeAnswer::Refused(
                HolderChangeRefusal::Superseded { .. }
            ))
        ),
        "old move after takeover: {}",
        serde_json::to_string(&old_move)?
    );
    writer.command(WriterCommand::ReleaseTransportCall).await?;
    writer
        .command(WriterCommand::WaitSuperseded { current: granted })
        .await?;
    assert!(matches!(
        writer
            .command(WriterCommand::TryWrite {
                label: "old writer after takeover".into()
            })
            .await?,
        WriterReply::WriteRefused { .. }
    ));
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    assert_eq!(
        copy_rows(&mut reader, database)
            .await?
            .get("samples")
            .map(Vec::len),
        Some(301)
    );
    writer.stop().await?;
    service.stop(&mut control).await?;
    Ok(())
}
