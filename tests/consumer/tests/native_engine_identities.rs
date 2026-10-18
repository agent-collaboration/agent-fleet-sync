#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::*;
use sqlx_core::{connection::ConnectOptions, row::Row};
use sqlx_turso::{Turso, TursoConnectOptions};
use support::*;

#[tokio::test]
async fn real_session_stores_v7_keys_and_refuses_a_v4_seed_before_receipt() -> TestResult {
    let directory = tempfile::tempdir()?;
    let service_path = directory.path().join("service");
    let process = ServiceProcess::start(&service_path).await?;
    let mut control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    let session = SessionId::new(durable_steps::Uuid::now_v7())?;
    let mut socket = process.peer(2, session).await?;
    let peer = PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000002,
    ))?;
    let database = DatabaseId::new(durable_steps::Uuid::now_v7())?;
    let answer = request(
        &mut socket,
        OperationRequest::Lease(LeaseRequest::Register(Registration {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database,
            writer: peer,
            origin: Origin::Seeded {
                position: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0)),
            },
        })),
    )
    .await?;
    assert!(matches!(
        answer,
        OperationAnswer::Lease(LeaseAnswer::Register(RegistrationAnswer::Registered { .. }))
    ));
    let frame = FrameId::new(durable_steps::Uuid::now_v7())?;
    let offered = serde_json::json!({"kind":"request","value":{"frame":frame,"body":{
        "kind":"replication","value":{"kind":"seedBegin","value":{
            "database":database,"attempt":"019eef00-0000-4000-8000-000000000011"
        }}
    }}});
    socket.send(serde_json::to_vec(&offered)?.into()).await?;
    loop {
        match server_frame(&mut socket).await? {
            ServerFrame::Notice { .. } => continue,
            ServerFrame::Answer {
                frame: correlated,
                body: OperationAnswer::InvalidShape { field },
            } => {
                assert_eq!(correlated, frame);
                assert_eq!(field, "body");
                break;
            }
            _ => return Err("unexpected response to invalid seed identity".into()),
        }
    }
    assert!(matches!(
        request(&mut socket, OperationRequest::Consumer(SampleRequest::Echo { text: "after invalid seed identity".into() })).await?,
        OperationAnswer::Consumer(SampleAnswer::Echo { text, .. }) if text == "after invalid seed identity"
    ));
    process.stop(&mut control).await?;
    // The owning process is stopped before these independent file readbacks.
    let mut sessions = TursoConnectOptions::new()
        .filename(service_path.join("service/sessions.db"))
        .create_if_missing(false)
        .connect()
        .await?;
    let rows = sqlx::query::<Turso>("SELECT peer, session_id FROM _fleet_sync_sessions")
        .fetch_all(&mut sessions)
        .await?;
    assert_eq!(rows.len(), 1);
    for row in rows {
        let stored_peer: String = row.try_get("peer")?;
        let stored_session: String = row.try_get("session_id")?;
        assert_eq!(stored_peer, peer.to_string());
        assert_eq!(stored_session, session.to_string());
        assert!(canonical_uuid_v7(&stored_peer) && canonical_uuid_v7(&stored_session));
    }
    let receiver = ReceiverId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000003,
    ))?;
    let mut copy = TursoConnectOptions::new()
        .filename(
            service_path
                .join("service")
                .join(format!("copy-{receiver}-{database}.db")),
        )
        .create_if_missing(false)
        .connect()
        .await?;
    let row = sqlx::query::<Turso>("SELECT (SELECT count(*) FROM _fleet_sync_seed_attempts)+(SELECT count(*) FROM _fleet_sync_seed) AS count")
        .fetch_one(&mut copy).await?;
    assert_eq!(row.try_get::<i64, _>("count")?, 0);
    Ok(())
}
