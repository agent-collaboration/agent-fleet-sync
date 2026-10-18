#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_session::*;
use fleet_sync_test_consumer::*;
use fleet_sync_transport_websocket::WebSocketTransport;
use support::*;

async fn hello(connection: &mut impl TransportConnection, session: SessionId) -> TestResult {
    connection
        .send(
            serde_json::to_vec(&ClientFrame::<SampleProtocol>::Hello {
                session_id: session,
                schema_support: SchemaSupport { max_version: 100 },
            })?
            .into(),
        )
        .await?;
    Ok(())
}
#[tokio::test]
async fn restored_permission_starts_a_new_session_without_reviving_old_bindings_or_verifications()
-> TestResult {
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start(root.path()).await?;
    let peer = PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000002,
    ))?;
    let old_session = SessionId::new(durable_steps::Uuid::now_v7())?;
    let mut attached = service.peer(2, old_session).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut opening = service.connect("session", "Bearer peer-2").await?;
    control_request(&mut control, ControlRequest::WaitOpening { peer }).await?;
    control_request(&mut control, ControlRequest::HoldNextVerification { peer }).await?;
    let endpoint = Endpoint::new(&format!("ws://127.0.0.1:{}/session", service.port))?;
    let mut verifying = tokio::task::JoinSet::new();
    verifying.spawn(async move {
        WebSocketTransport::default()
            .connect(&endpoint, &PeerCredential::new("Bearer peer-2")?)
            .await
    });
    control_request(&mut control, ControlRequest::WaitVerification { peer }).await?;
    control_request(&mut control, ControlRequest::Revoke { peer }).await?;
    assert!(matches!(
        server_frame(&mut attached).await?,
        ServerFrame::Closed {
            reason: CloseReason::CredentialRejected(AuthRefusal::Revoked)
        }
    ));
    assert!(service.connect("session", "Bearer peer-2").await.is_err());
    control_request(&mut control, ControlRequest::AllowPeer { peer }).await?;
    // This transport was already Opening before revoke; allowing the peer back
    // cannot turn that old verified binding into a new admission.
    let _ = hello(&mut opening, SessionId::new(durable_steps::Uuid::now_v7())?).await;
    assert!(matches!(
        server_frame(&mut opening).await?,
        ServerFrame::Closed {
            reason: CloseReason::CredentialRejected(AuthRefusal::Revoked)
        }
    ));
    control_request(&mut control, ControlRequest::ResumeVerification).await?;
    assert!(
        verifying
            .join_next()
            .await
            .ok_or("verification task missing")??
            .is_err(),
        "pre-revocation verification attached after permission restoration"
    );
    let new_session = SessionId::new(durable_steps::Uuid::now_v7())?;
    let mut fresh = service
        .peer(2, new_session)
        .await
        .map_err(|error| format!("freshly reauthorized session was refused: {error}"))?;
    assert!(
        matches!(request(&mut fresh, OperationRequest::Consumer(SampleRequest::Echo { text: "same running host".into() })).await?, OperationAnswer::Consumer(SampleAnswer::Echo { peer: observed, text }) if observed==peer && text=="same running host")
    );
    let mut ended = service.connect("session", "Bearer peer-2").await?;
    hello(&mut ended, old_session).await?;
    assert!(matches!(
        server_frame(&mut ended).await?,
        ServerFrame::Closed {
            reason: CloseReason::SessionEnded
        }
    ));
    fresh.close(CloseReason::ClientClosed).await;
    service.stop(&mut control).await
}

#[tokio::test]
async fn another_peers_revocation_keeps_an_in_flight_verification_valid() -> TestResult {
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start(root.path()).await?;
    let peer = PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000004,
    ))?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    control_request(&mut control, ControlRequest::HoldNextVerification { peer }).await?;
    let endpoint = Endpoint::new(&format!("ws://127.0.0.1:{}/session", service.port))?;
    let mut verifying = tokio::task::JoinSet::new();
    verifying.spawn(async move {
        WebSocketTransport::default()
            .connect(&endpoint, &PeerCredential::new("Bearer peer-4")?)
            .await
    });
    control_request(&mut control, ControlRequest::WaitVerification { peer }).await?;
    control_request(
        &mut control,
        ControlRequest::Revoke {
            peer: PeerId::new(durable_steps::Uuid::from_u128(
                0x019eef00000070008000000000000002,
            ))?,
        },
    )
    .await?;
    control_request(&mut control, ControlRequest::ResumeVerification).await?;
    let mut connection = verifying
        .join_next()
        .await
        .ok_or("verification task missing")???;
    let session = SessionId::new(durable_steps::Uuid::now_v7())?;
    hello(&mut connection, session).await?;
    assert!(
        matches!(server_frame(&mut connection).await?, ServerFrame::Welcome { session_id, .. } if session_id==session)
    );
    connection.close(CloseReason::ClientClosed).await;
    service.stop(&mut control).await
}
