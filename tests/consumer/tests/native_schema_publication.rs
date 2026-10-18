#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_session::{ConnectionRegistration, TransportConnection};
use fleet_sync_test_consumer::*;
use fleet_sync_tokio_runner::SchemaPublicationSnapshot;
use fleet_sync_transport_websocket::WebSocketConnection;
use std::time::Duration;
use support::*;

fn peer(value: u128) -> TestResult<PeerId> {
    Ok(PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000000 | value,
    ))?)
}
async fn hello(
    service: &ServiceProcess,
    identity: u128,
    session: SessionId,
    maximum: u64,
) -> TestResult<WebSocketConnection> {
    let mut socket = service
        .connect("session", &format!("Bearer peer-{identity}"))
        .await?;
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
async fn welcome(socket: &mut WebSocketConnection, session: SessionId) -> TestResult {
    assert!(
        matches!(server_frame(socket).await?, ServerFrame::Welcome { session_id, .. } if session_id == session)
    );
    Ok(())
}
async fn no_frame(socket: &mut WebSocketConnection) -> TestResult {
    // Before release the operation-owned gate remains held; after completion
    // this checks that no extra frame was queued by the finished task.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), socket.receive())
            .await
            .is_err()
    );
    Ok(())
}
async fn held_report(control: &mut WebSocketConnection) -> TestResult<ReportSchemaSupport> {
    match schema_request(control, SchemaControlRequest::WaitReport).await? {
        SchemaControlAnswer::Report(report) => Ok(report),
        _ => Err("held report missing".into()),
    }
}
async fn projection(
    control: &mut WebSocketConnection,
    identity: PeerId,
) -> TestResult<Option<ReportSchemaSupport>> {
    match schema_request(control, SchemaControlRequest::Projection { peer: identity }).await? {
        SchemaControlAnswer::Projection(report) => Ok(report),
        _ => Err("projection answer missing".into()),
    }
}
async fn snapshot(
    control: &mut WebSocketConnection,
    report: &ReportSchemaSupport,
) -> TestResult<SchemaPublicationSnapshot> {
    match schema_request(control, SchemaControlRequest::Snapshot(report.clone())).await? {
        SchemaControlAnswer::Snapshot(snapshot) => Ok(snapshot),
        _ => Err("publication snapshot missing".into()),
    }
}
fn pending_connection(
    snapshot: &SchemaPublicationSnapshot,
    report: &ReportSchemaSupport,
) -> TestResult<ConnectionId> {
    snapshot.registrations.iter().find_map(|(connection, registration)| {
        matches!(registration, ConnectionRegistration::Attaching { session_id, attach_seq, schema_support } if *session_id == report.session_id && *attach_seq == report.attach_seq && *schema_support == report.support).then_some(*connection)
    }).ok_or_else(|| "matching Attaching registration missing".into())
}

#[tokio::test]
async fn a_held_report_blocks_welcome_and_requests_without_blocking_other_input_or_close()
-> TestResult {
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start(&root.path().join("service")).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    schema_request(
        &mut control,
        SchemaControlRequest::HoldReport {
            peer: peer(4)?,
            attach_seq: AttachSequence::new(1)?,
        },
    )
    .await?;
    let session = SessionId::new(durable_steps::Uuid::now_v7())?;
    let mut target = hello(&service, 4, session, 5).await?;
    let report = held_report(&mut control).await?;
    assert_eq!(report.peer, peer(4)?);
    assert_eq!(report.session_id, session);
    assert_eq!(report.support, SchemaSupport { max_version: 5 });
    assert_eq!(projection(&mut control, peer(4)?).await?, None);
    let connection = pending_connection(&snapshot(&mut control, &report).await?, &report)?;
    no_frame(&mut target).await?;
    let mut other = service.connect("session", "Bearer peer-5").await?;
    assert!(matches!(
        control_request(&mut control, ControlRequest::WaitOpening { peer: peer(5)? }).await?,
        ControlAnswer::Opening { .. }
    ));
    control_request(&mut control, ControlRequest::Revoke { peer: peer(5)? }).await?;
    assert!(matches!(
        server_frame(&mut other).await?,
        ServerFrame::Closed {
            reason: CloseReason::CredentialRejected(AuthRefusal::Revoked)
        }
    ));
    target
        .send(
            serde_json::to_vec(&ClientFrame::<SampleProtocol>::Request {
                frame: FrameId::new(durable_steps::Uuid::now_v7())?,
                body: OperationRequest::Consumer(SampleRequest::Echo {
                    text: "must wait for publication".into(),
                }),
            })?
            .into(),
        )
        .await?;
    assert!(matches!(
        server_frame(&mut target).await?,
        ServerFrame::Closed {
            reason: CloseReason::MalformedFrame
        }
    ));
    assert!(matches!(
        control_request(&mut control, ControlRequest::RequestCount).await?,
        ControlAnswer::RequestCount(0)
    ));
    schema_request(
        &mut control,
        SchemaControlRequest::WaitClosed { connection },
    )
    .await?;
    schema_request(&mut control, SchemaControlRequest::ReleaseReport).await?;
    schema_request(
        &mut control,
        SchemaControlRequest::WaitPublication(report.clone()),
    )
    .await?;
    assert!(
        snapshot(&mut control, &report)
            .await?
            .registrations
            .iter()
            .all(|(current, _)| *current != connection)
    );
    assert_eq!(projection(&mut control, peer(4)?).await?, Some(report));
    service.stop(&mut control).await?;
    Ok(())
}

#[tokio::test]
async fn finalization_rollback_discards_registration_welcome_and_local_step_then_retries_once()
-> TestResult {
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start(&root.path().join("service")).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    schema_request(
        &mut control,
        SchemaControlRequest::FailFinalize { peer: peer(4)? },
    )
    .await?;
    let session = SessionId::new(durable_steps::Uuid::now_v7())?;
    let mut target = hello(&service, 4, session, 5).await?;
    assert!(
        matches!(schema_request(&mut control, SchemaControlRequest::WaitFinalizeRollback).await?, SchemaControlAnswer::RolledBack { peer: current } if current == peer(4)?)
    );
    let report = projection(&mut control, peer(4)?)
        .await?
        .ok_or("registry acceptance missing before finalization")?;
    let before = snapshot(&mut control, &report).await?;
    let connection = pending_connection(&before, &report)?;
    assert_eq!(before.attach_steps, 0);
    no_frame(&mut target).await?;
    let other = service.connect("session", "Bearer peer-5").await?;
    assert!(matches!(
        control_request(&mut control, ControlRequest::WaitOpening { peer: peer(5)? }).await?,
        ControlAnswer::Opening { .. }
    ));
    other.close(CloseReason::ClientClosed).await;
    schema_request(&mut control, SchemaControlRequest::ReleaseFinalize).await?;
    welcome(&mut target, session).await?;
    schema_request(
        &mut control,
        SchemaControlRequest::WaitPublication(report.clone()),
    )
    .await?;
    let after = snapshot(&mut control, &report).await?;
    assert!(
        after
            .registrations
            .iter()
            .any(|(current, registration)| *current == connection
                && *registration
                    == ConnectionRegistration::Attached {
                        session_id: session,
                        schema_support: SchemaSupport { max_version: 5 }
                    })
    );
    assert_eq!(after.attach_steps, 0); // Finished task prunes both local and call steps.
    no_frame(&mut target).await?;
    service.stop(&mut control).await?;
    Ok(())
}

#[tokio::test]
async fn closed_and_replaced_connections_are_not_resurrected_by_a_delayed_finalization()
-> TestResult {
    for replace in [false, true] {
        let root = tempfile::tempdir()?;
        let service = ServiceProcess::start(&root.path().join("service")).await?;
        let mut control = service
            .connect("control", "Bearer consumer-control")
            .await?;
        schema_request(
            &mut control,
            SchemaControlRequest::HoldReport {
                peer: peer(4)?,
                attach_seq: AttachSequence::new(1)?,
            },
        )
        .await?;
        let session = SessionId::new(durable_steps::Uuid::now_v7())?;
        let mut first = hello(&service, 4, session, 5).await?;
        let old = held_report(&mut control).await?;
        let connection = pending_connection(&snapshot(&mut control, &old).await?, &old)?;
        let mut replacement = if replace {
            let mut next = hello(&service, 4, session, 4).await?;
            assert!(matches!(
                server_frame(&mut first).await?,
                ServerFrame::Closed {
                    reason: CloseReason::ConnectionReplaced
                }
            ));
            welcome(&mut next, session).await?;
            Some(next)
        } else {
            first.close(CloseReason::ClientClosed).await;
            None
        };
        schema_request(
            &mut control,
            SchemaControlRequest::WaitClosed { connection },
        )
        .await?;
        let accepted = projection(&mut control, peer(4)?).await?;
        if replace {
            assert!(
                matches!(accepted.as_ref(), Some(report) if report.attach_seq == AttachSequence::new(2)? && report.support.max_version == 4)
            );
        }
        schema_request(&mut control, SchemaControlRequest::ReleaseReport).await?;
        schema_request(
            &mut control,
            SchemaControlRequest::WaitPublication(old.clone()),
        )
        .await?;
        assert!(
            snapshot(&mut control, &old)
                .await?
                .registrations
                .iter()
                .all(|(current, _)| *current != connection)
        );
        if let Some(next) = replacement.as_mut() {
            assert_eq!(projection(&mut control, peer(4)?).await?, accepted);
            no_frame(next).await?;
        } else {
            assert_eq!(projection(&mut control, peer(4)?).await?, Some(old));
        }
        service.stop(&mut control).await?;
    }
    Ok(())
}

#[tokio::test]
async fn restart_before_finalization_keeps_the_next_attach_sequence_and_never_welcomes_the_old_connection()
-> TestResult {
    let root = tempfile::tempdir()?;
    let path = root.path().join("service");
    let mut service = ServiceProcess::start(&path).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    schema_request(
        &mut control,
        SchemaControlRequest::HoldReport {
            peer: peer(4)?,
            attach_seq: AttachSequence::new(1)?,
        },
    )
    .await?;
    let session = SessionId::new(durable_steps::Uuid::now_v7())?;
    let first = hello(&service, 4, session, 5).await?;
    let old = held_report(&mut control).await?;
    let old_connection = pending_connection(&snapshot(&mut control, &old).await?, &old)?;
    service.kill().await?;
    drop(first);
    drop(control);
    service = ServiceProcess::start(&path).await?;
    control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut next = hello(&service, 4, session, 4).await?;
    welcome(&mut next, session).await?;
    let accepted = projection(&mut control, peer(4)?)
        .await?
        .ok_or("new report missing after restart")?;
    assert_eq!(accepted.attach_seq, AttachSequence::new(2)?);
    assert_eq!(accepted.support.max_version, 4);
    let before = snapshot(&mut control, &old)
        .await?
        .registrations
        .into_iter()
        .find(|(connection, _)| *connection == old_connection);
    schema_request(
        &mut control,
        SchemaControlRequest::WaitPublication(old.clone()),
    )
    .await?;
    let after = snapshot(&mut control, &old)
        .await?
        .registrations
        .into_iter()
        .find(|(connection, _)| *connection == old_connection);
    assert_eq!(after, before);
    assert!(after.is_none_or(|(_, registration)| !matches!(
        registration,
        ConnectionRegistration::Attached { .. } | ConnectionRegistration::Attaching { .. }
    )));
    assert_eq!(projection(&mut control, peer(4)?).await?, Some(accepted));
    no_frame(&mut next).await?;
    service.stop(&mut control).await?;
    Ok(())
}
