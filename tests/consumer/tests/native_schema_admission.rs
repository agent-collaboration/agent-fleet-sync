#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::*;
use fleet_sync_transport_websocket::WebSocketConnection;
use std::time::Duration;
use support::{lifecycle_import::import_backup, *};

fn claimant() -> TestResult<PeerId> {
    Ok(PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000004,
    ))?)
}
async fn attach(
    service: &ServiceProcess,
    session: SessionId,
    maximum: u64,
) -> TestResult<WebSocketConnection> {
    let mut socket = service.connect("session", "Bearer peer-4").await?;
    socket
        .send(
            serde_json::to_vec(&ClientFrame::<SampleProtocol>::Hello {
                session_id: session,
                schema_support: SchemaSupport {
                    max_version: maximum,
                },
            })?
            .into(),
        )
        .await?;
    Ok(socket)
}
async fn welcomed(socket: &mut WebSocketConnection, session: SessionId) -> TestResult {
    assert!(
        matches!(server_frame(socket).await?, ServerFrame::Welcome { session_id, .. } if session_id == session)
    );
    Ok(())
}
fn claim(database: DatabaseId) -> TestResult<HolderChangeRequest> {
    Ok(HolderChangeRequest::Claim {
        attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
        database,
        claimant: claimant()?,
    })
}
fn attempt(change: &HolderChangeRequest) -> TransferId {
    match change {
        HolderChangeRequest::Claim { attempt, .. }
        | HolderChangeRequest::Move { attempt, .. }
        | HolderChangeRequest::Takeover { attempt, .. } => *attempt,
    }
}
async fn outcome(
    control: &mut WebSocketConnection,
    change: HolderChangeRequest,
) -> TestResult<HolderChangeAnswer> {
    match control_request(control, ControlRequest::WaitHolderChange(change)).await? {
        ControlAnswer::HolderChange(answer) => Ok(answer),
        _ => Err("holder-change answer missing".into()),
    }
}
async fn frozen(
    control: &mut WebSocketConnection,
    change: &HolderChangeRequest,
    maximum: u64,
) -> TestResult {
    assert!(
        matches!(schema_request(control, SchemaControlRequest::Admission { attempt: attempt(change) }).await?, SchemaControlAnswer::AdmittedTarget { schema: Some(support) } if support.max_version == maximum)
    );
    Ok(())
}

#[tokio::test]
async fn native_admission_freezes_before_drive_and_keeps_that_value_across_restart() -> TestResult {
    for restart in [false, true] {
        let root = tempfile::tempdir()?;
        let path = root.path().join("service");
        let mut service = ServiceProcess::start(&path).await?;
        let mut control = service
            .connect("control", "Bearer consumer-control")
            .await?;
        let (import, position) =
            import_backup(&service, &mut control, &root.path().join("first.db")).await?;
        let session = SessionId::new(durable_steps::Uuid::now_v7())?;
        let mut old = attach(&service, session, 1).await?;
        welcomed(&mut old, session).await?;
        schema_request(&mut control, SchemaControlRequest::HoldRegistryDrive).await?;
        let change = claim(import.database)?;
        assert!(matches!(
            control_request(&mut control, ControlRequest::HolderChange(change.clone())).await?,
            ControlAnswer::HolderChange(HolderChangeAnswer::Pending { .. })
        ));
        frozen(&mut control, &change, 1).await?;
        let mut newer = attach(&service, session, 0).await?;
        welcomed(&mut newer, session).await?;
        assert!(
            matches!(schema_request(&mut control, SchemaControlRequest::Projection { peer: claimant()? }).await?, SchemaControlAnswer::Projection(Some(report)) if report.attach_seq == AttachSequence::new(2)? && report.support.max_version == 0)
        );
        frozen(&mut control, &change, 1).await?;
        if restart {
            service.kill().await?;
            drop(control);
            drop(newer);
            service = ServiceProcess::start(&path).await?;
            control = service
                .connect("control", "Bearer consumer-control")
                .await?;
        } else {
            schema_request(&mut control, SchemaControlRequest::ReleaseRegistryDrive).await?;
        }
        let granted = outcome(&mut control, change.clone()).await?;
        assert!(
            matches!(granted, HolderChangeAnswer::Granted { epoch, start, .. } if epoch == Epoch::new(2)? && start == position)
        );
        frozen(&mut control, &change, 1).await?;
        assert!(
            matches!(control_request(&mut control, ControlRequest::HolderChange(change)).await?, ControlAnswer::HolderChange(answer) if answer == granted)
        );
        let (second, _) =
            import_backup(&service, &mut control, &root.path().join("second.db")).await?;
        let fresh = claim(second.database)?;
        control_request(&mut control, ControlRequest::HolderChange(fresh.clone())).await?;
        assert!(
            matches!(outcome(&mut control, fresh.clone()).await?, HolderChangeAnswer::Refused(HolderChangeRefusal::SchemaTooNew { store, supported: Some(0) }) if store.version == 1)
        );
        frozen(&mut control, &fresh, 0).await?;
        service.stop(&mut control).await?;
    }
    Ok(())
}

#[tokio::test]
async fn pending_new_report_orders_admin_admission_before_attach_and_own_claim_after_welcome()
-> TestResult {
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start(&root.path().join("service")).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let (import, position) =
        import_backup(&service, &mut control, &root.path().join("first.db")).await?;
    let session = SessionId::new(durable_steps::Uuid::now_v7())?;
    let mut old = attach(&service, session, 1).await?;
    welcomed(&mut old, session).await?;
    schema_request(
        &mut control,
        SchemaControlRequest::HoldReport {
            peer: claimant()?,
            attach_seq: AttachSequence::new(2)?,
        },
    )
    .await?;
    let mut newer = attach(&service, session, 0).await?;
    assert!(
        matches!(schema_request(&mut control, SchemaControlRequest::WaitReport).await?, SchemaControlAnswer::Report(report) if report.attach_seq == AttachSequence::new(2)? && report.support.max_version == 0)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), newer.receive())
            .await
            .is_err()
    );
    assert!(
        matches!(schema_request(&mut control, SchemaControlRequest::Projection { peer: claimant()? }).await?, SchemaControlAnswer::Projection(Some(report)) if report.attach_seq == AttachSequence::new(1)? && report.support.max_version == 1)
    );
    let before_acceptance = claim(import.database)?;
    control_request(
        &mut control,
        ControlRequest::HolderChange(before_acceptance.clone()),
    )
    .await?;
    frozen(&mut control, &before_acceptance, 1).await?;
    assert!(
        matches!(outcome(&mut control, before_acceptance).await?, HolderChangeAnswer::Granted { start, .. } if start == position)
    );
    schema_request(&mut control, SchemaControlRequest::ReleaseReport).await?;
    welcomed(&mut newer, session).await?;
    let (second, _) = import_backup(&service, &mut control, &root.path().join("second.db")).await?;
    let own = claim(second.database)?;
    let own_attempt = RequestId::new(attempt(&own).as_uuid())?;
    request(
        &mut newer,
        OperationRequest::Lease(LeaseRequest::Claim {
            attempt: own_attempt,
            database: second.database,
        }),
    )
    .await?;
    assert!(
        matches!(outcome(&mut control, own.clone()).await?, HolderChangeAnswer::Refused(HolderChangeRefusal::SchemaTooNew { store, supported: Some(0) }) if store.version == 1)
    );
    frozen(&mut control, &own, 0).await?;
    service.stop(&mut control).await?;
    Ok(())
}
