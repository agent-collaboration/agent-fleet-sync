//! Holder release is supplied at the socket boundary. Copy calls, service,
//! journal, damage, restart and fencing are the production native path.
#[path = "support/copy_fixture.rs"]
#[allow(dead_code)]
mod fixture;
#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_service::CopyCallKind;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::*;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::path::Path;
use support::*;

async fn wait_copy(
    control: &mut fleet_sync_transport_websocket::WebSocketConnection,
    attempt: TransferId,
) -> TestResult {
    let view = match control_request(
        control,
        ControlRequest::CopyFault(CopyFaultRequest::WaitHolderChange { attempt }),
    )
    .await?
    {
        ControlAnswer::HolderChangeView(Some(view)) => view,
        _ => return Err("missing copy wait".into()),
    };
    assert!(view.outcome.is_none());
    let waiting = view.waiting.ok_or("missing pending copy call")?;
    assert_eq!(waiting.reach, durable_steps::storage::CallReach::NoEffect);
    assert!(matches!(
        waiting.copy_unavailable,
        Some(CopyUnavailableReason::Damaged { .. })
    ));
    Ok(())
}
async fn inspect_pending(root: &Path, database: DatabaseId, step: &str) -> TestResult {
    let mut registry = TursoConnectOptions::new()
        .filename(root.join("service/registry.db"))
        .create_if_missing(false)
        .connect()
        .await?;
    let row = query::<Turso>(
        "SELECT kind,intent,may_have_reached FROM _fleet_sync_steps WHERE step_name=?",
    )
    .bind(step.to_owned())
    .fetch_one(&mut registry)
    .await?;
    assert_eq!(row.try_get::<String, _>("kind")?, "call_pending");
    let intent: serde_json::Value = serde_json::from_str(&row.try_get::<String, _>("intent")?)?;
    assert_eq!(intent["reach"], "no_effect");
    assert_eq!(
        intent["retry_reason"]["code"], "copy_damaged",
        "stored pending intent: {intent}"
    );
    assert_eq!(row.try_get::<i64, _>("may_have_reached")?, 1);
    let floor = query::<Turso>("SELECT epoch FROM _fleet_sync_epoch_floor")
        .fetch_one(&mut registry)
        .await?
        .try_get::<String, _>("epoch")?;
    assert_eq!(floor, "2");
    registry.close().await?;
    assert_eq!(fence_count(root, database).await?, 0);
    Ok(())
}
async fn fence_count(root: &Path, database: DatabaseId) -> TestResult<i64> {
    let mut copy = TursoConnectOptions::new()
        .filename(fixture::copy_file_path(root, database)?)
        .create_if_missing(false)
        .connect()
        .await?;
    let count = query::<Turso>("SELECT count(*) AS count FROM _fleet_sync_fence_receipts")
        .fetch_one(&mut copy)
        .await?
        .try_get("count")?;
    copy.close().await?;
    Ok(count)
}

#[tokio::test]
async fn actual_move_ack_and_fence_calls_retry_damage_across_restart_without_ending_the_attempt()
-> TestResult {
    for (kind, step) in [
        (CopyCallKind::Acknowledged, "await_acked"),
        (CopyCallKind::Fence, "fence"),
    ] {
        let root = tempfile::tempdir()?;
        let service_root = root.path().join("service");
        let mut service = ServiceProcess::start(&service_root).await?;
        let mut control = service
            .connect("control", "Bearer consumer-control")
            .await?;
        let mut holder = service
            .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
            .await?;
        let target = service
            .peer(4, SessionId::new(durable_steps::Uuid::now_v7())?)
            .await?;
        let database = DatabaseId::new(durable_steps::Uuid::now_v7())?;
        let attempt = TransferId::new(durable_steps::Uuid::now_v7())?;
        let change = HolderChangeRequest::Move {
            attempt,
            database,
            from_epoch: Epoch::new(1)?,
            to: PeerId::new(durable_steps::Uuid::from_u128(
                0x019eef00000070008000000000000004,
            ))?,
        };
        let observed: TestResult<String> = async {
            assert!(matches!(request(&mut holder, OperationRequest::Lease(LeaseRequest::Register(Registration {
                attempt: RequestId::new(durable_steps::Uuid::now_v7())?, database, writer: fixture::writer_peer()?, origin: Origin::Empty,
            }))).await?, OperationAnswer::Lease(LeaseAnswer::Register(RegistrationAnswer::Registered { .. }))));
            request(&mut holder, OperationRequest::Lease(LeaseRequest::Renew(RenewRequest { attempt: RequestId::new(durable_steps::Uuid::now_v7())?, database, epoch: Epoch::new(1)? }))).await?;
            control_request(&mut control, ControlRequest::CopyFault(CopyFaultRequest::HoldCall { database, kind })).await?;
            assert!(matches!(control_request(&mut control, ControlRequest::HolderChange(change.clone())).await?, ControlAnswer::HolderChange(HolderChangeAnswer::Pending { transfer }) if transfer == attempt));
            let frame = loop {
                match server_frame(&mut holder).await? {
                    ServerFrame::Request { frame, body: ServerRequest::Crate(ServerCall::Release { request, .. }) } => {
                        assert_eq!(request.database, database);
                        assert_eq!(request.transfer, attempt);
                        break frame;
                    }
                    ServerFrame::Notice { .. } => {}
                    _ => return Err("holder did not receive its release call".into()),
                }
            };
            holder.send(serde_json::to_vec(&ClientFrame::<SampleProtocol>::Answer { frame,
                body: ClientAnswer::Crate(ServerCallAnswer::Release(ReleaseAnswer::Released { final_position: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0)) }))
            })?.into()).await?;
            assert!(matches!(control_request(&mut control, ControlRequest::CopyFault(CopyFaultRequest::WaitCall { database })).await?, ControlAnswer::CopyCall(call) if call.step.as_str() == step));
            let original = match control_request(&mut control, ControlRequest::CopyFault(CopyFaultRequest::ReplaceMarker { database, replacement: "{damaged-at-real-copy-call".into() })).await? {
                ControlAnswer::CopyMarker(original) => original,
                _ => return Err("copy marker fault missing".into()),
            };
            control_request(&mut control, ControlRequest::CopyFault(CopyFaultRequest::ReleaseCall { database })).await?;
            wait_copy(&mut control, attempt).await?;
            // Freeze the receiver decision before shutdown can create a newer
            // NotSent attempt with no receiver-reason metadata.
            control_request(&mut control, ControlRequest::Schema(SchemaControlRequest::HoldRegistryDrive)).await?;
            Ok(original)
        }.await;
        holder.close(CloseReason::ClientClosed).await;
        target.close(CloseReason::ClientClosed).await;
        service.kill().await?;
        drop(control);
        let original = observed?;
        inspect_pending(&service_root, database, step).await?;
        let mut reopened = ServiceProcess::start(&service_root).await?;
        let mut control = reopened
            .connect("control", "Bearer consumer-control")
            .await?;
        let retained: TestResult = async {
            wait_copy(&mut control, attempt).await?;
            control_request(
                &mut control,
                ControlRequest::Schema(SchemaControlRequest::HoldRegistryDrive),
            )
            .await?;
            Ok(())
        }
        .await;
        reopened.kill().await?;
        drop(control);
        retained?;
        inspect_pending(&service_root, database, step).await?;
        fixture::replace_marker(
            &fixture::copy_file_path(&service_root, database)?,
            &original,
        )
        .await?;
        let repaired = ServiceProcess::start(&service_root).await?;
        let mut control = repaired
            .connect("control", "Bearer consumer-control")
            .await?;
        let resumed: TestResult = async {
            let granted = match control_request(&mut control, ControlRequest::WaitHolderChange(change.clone())).await? {
                ControlAnswer::HolderChange(answer) => answer,
                _ => return Err("repaired move did not answer".into()),
            };
            assert!(matches!(granted, HolderChangeAnswer::Granted { epoch, .. } if epoch == Epoch::new(2)?));
            assert!(matches!(control_request(&mut control, ControlRequest::HolderChange(change.clone())).await?, ControlAnswer::HolderChange(replay) if replay == granted));
            assert!(matches!(control_request(&mut control, ControlRequest::CopyStatus { database }).await?, ControlAnswer::CopyStatus(CopyStatusAnswer::Status(status)) if status.fence == Epoch::new(2)?));
            Ok(())
        }.await;
        repaired.stop(&mut control).await?;
        resumed?;
        assert_eq!(fence_count(&service_root, database).await?, 1);
    }
    Ok(())
}
