use crate::support::{ServiceProcess, server_frame};
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::{SampleProtocol, TestResult};
use fleet_sync_transport_websocket::WebSocketConnection;
use serde_json::Value;
use std::path::Path;

pub async fn start_service(root: &Path) -> TestResult<ServiceProcess> {
    ServiceProcess::start_as(root, durable_steps::Uuid::now_v7().as_u128()).await
}

pub async fn attach_peer(
    service: &ServiceProcess,
    peer: PeerId,
    support: u64,
) -> TestResult<WebSocketConnection> {
    let mut socket = service
        .connect(
            "session",
            &format!("Bearer peer-{}", peer.as_uuid().as_u128()),
        )
        .await?;
    let session_id = SessionId::new(durable_steps::Uuid::now_v7())?;
    let hello: ClientFrame<SampleProtocol> = ClientFrame::Hello {
        session_id,
        schema_support: SchemaSupport {
            max_version: support,
        },
    };
    socket.send(serde_json::to_vec(&hello)?.into()).await?;
    match server_frame(&mut socket).await? {
        ServerFrame::Welcome {
            session_id: returned,
            ..
        } if returned == session_id => Ok(socket),
        _ => Err("matching Welcome absent".into()),
    }
}

pub async fn receive_json(socket: &mut WebSocketConnection) -> TestResult<Value> {
    loop {
        let bytes = tokio::time::timeout(std::time::Duration::from_secs(15), socket.receive())
            .await?
            .ok_or("session closed without answer")??;
        let frame: Value = serde_json::from_slice(&bytes)?;
        if frame.get("kind").and_then(Value::as_str) != Some("notice") {
            return Ok(frame);
        }
    }
}

pub fn new_peer() -> TestResult<PeerId> {
    Ok(PeerId::new(durable_steps::Uuid::now_v7())?)
}
