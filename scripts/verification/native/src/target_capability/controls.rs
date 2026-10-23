//! Existing native controls observe the real publication and registry owners.
pub(super) use crate::{Observations, fixture_service::*, import_fixture::*, support::*};
pub(super) use fleet_sync_protocol::*;
pub(super) use fleet_sync_session::{ConnectionRegistration, TransportConnection};
pub(super) use fleet_sync_test_consumer::{
    ControlAnswer, ControlRequest, SampleProtocol, SchemaControlAnswer, SchemaControlRequest,
    TestResult,
};
pub(super) use fleet_sync_tokio_runner::SchemaPublicationSnapshot;
pub(super) use fleet_sync_transport_websocket::WebSocketConnection;
pub(super) use serde_json::json;
pub(super) use std::{path::Path, time::Duration};

pub(super) async fn hello(
    service: &ServiceProcess,
    peer: PeerId,
    session: SessionId,
    maximum: u64,
) -> TestResult<WebSocketConnection> {
    let mut socket = service
        .connect(
            "session",
            &format!("Bearer peer-{}", peer.as_uuid().as_u128()),
        )
        .await?;
    let frame: ClientFrame<SampleProtocol> = ClientFrame::Hello {
        session_id: session,
        schema_support: SchemaSupport {
            max_version: maximum,
        },
    };
    socket.send(serde_json::to_vec(&frame)?.into()).await?;
    Ok(socket)
}

pub(super) async fn welcome(socket: &mut WebSocketConnection, session: SessionId) -> TestResult {
    let frame = server_frame(socket).await?;
    println!("{}", json!({"case":"schema-welcome-wire","frame":frame}));
    match frame {
        ServerFrame::Welcome { session_id, .. } if session_id == session => Ok(()),
        _ => Err("matching schema Welcome missing".into()),
    }
}

pub(super) async fn quiet(socket: &mut WebSocketConnection) -> TestResult<bool> {
    // The operation-owned hold/completion establishes the window. This bound
    // samples the wire; elapsed time never releases a gate or signals readiness.
    match tokio::time::timeout(Duration::from_millis(100), socket.receive()).await {
        Err(_) => Ok(true),
        Ok(frame) => {
            println!(
                "{}",
                json!({"case":"unexpected-schema-frame","frame":format!("{frame:?}")})
            );
            Ok(false)
        }
    }
}

pub(super) async fn projection(
    control: &mut WebSocketConnection,
    peer: PeerId,
) -> TestResult<Option<ReportSchemaSupport>> {
    match schema_request(control, SchemaControlRequest::Projection { peer }).await? {
        SchemaControlAnswer::Projection(report) => Ok(report),
        _ => Err("schema projection missing".into()),
    }
}
pub(super) async fn held_report(
    control: &mut WebSocketConnection,
) -> TestResult<ReportSchemaSupport> {
    match schema_request(control, SchemaControlRequest::WaitReport).await? {
        SchemaControlAnswer::Report(report) => Ok(report),
        _ => Err("held report missing".into()),
    }
}
pub(super) async fn snapshot(
    control: &mut WebSocketConnection,
    report: &ReportSchemaSupport,
) -> TestResult<SchemaPublicationSnapshot> {
    match schema_request(control, SchemaControlRequest::Snapshot(report.clone())).await? {
        SchemaControlAnswer::Snapshot(snapshot) => Ok(snapshot),
        _ => Err("publication snapshot missing".into()),
    }
}
pub(super) fn attaching(
    snapshot: &SchemaPublicationSnapshot,
    report: &ReportSchemaSupport,
) -> TestResult<ConnectionId> {
    snapshot.registrations.iter().find_map(|(connection, registration)| {
        matches!(registration, ConnectionRegistration::Attaching { session_id, attach_seq, schema_support }
            if *session_id == report.session_id && *attach_seq == report.attach_seq && *schema_support == report.support)
            .then_some(*connection)
    }).ok_or_else(|| "matching Attaching registration missing".into())
}
pub(super) fn new_session() -> TestResult<SessionId> {
    Ok(SessionId::new(durable_steps::Uuid::now_v7())?)
}
pub(super) fn claim_change(
    database: DatabaseId,
    claimant: PeerId,
) -> TestResult<HolderChangeRequest> {
    Ok(HolderChangeRequest::Claim {
        database,
        claimant,
        attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
    })
}
pub(super) fn attempt(change: &HolderChangeRequest) -> TransferId {
    match change {
        HolderChangeRequest::Claim { attempt, .. }
        | HolderChangeRequest::Move { attempt, .. }
        | HolderChangeRequest::Takeover { attempt, .. } => *attempt,
    }
}
pub(super) async fn admit(
    control: &mut WebSocketConnection,
    change: &HolderChangeRequest,
) -> TestResult {
    match control_request(control, ControlRequest::HolderChange(change.clone())).await? {
        ControlAnswer::HolderChange(HolderChangeAnswer::Pending { transfer })
            if transfer == attempt(change) =>
        {
            Ok(())
        }
        _ => Err("holder change did not reach its admission window".into()),
    }
}
pub(super) async fn outcome(
    control: &mut WebSocketConnection,
    change: &HolderChangeRequest,
) -> TestResult<HolderChangeAnswer> {
    match control_request(control, ControlRequest::WaitHolderChange(change.clone())).await? {
        ControlAnswer::HolderChange(answer) => Ok(answer),
        _ => Err("canonical holder-change answer missing".into()),
    }
}
pub(super) async fn frozen(
    control: &mut WebSocketConnection,
    change: &HolderChangeRequest,
) -> TestResult<Option<SchemaSupport>> {
    match schema_request(
        control,
        SchemaControlRequest::Admission {
            attempt: attempt(change),
        },
    )
    .await?
    {
        SchemaControlAnswer::AdmittedTarget { schema } => Ok(schema),
        _ => Err("admitted target schema missing".into()),
    }
}
pub(super) fn is_too_new(answer: &HolderChangeAnswer, supported: u64) -> bool {
    matches!(answer, HolderChangeAnswer::Refused(HolderChangeRefusal::SchemaTooNew { store, supported: Some(actual) }) if store.version == 5 && *actual == supported)
}
