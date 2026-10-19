#[expect(
    dead_code,
    unused_imports,
    reason = "shared native helpers serve other journeys"
)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{SampleProtocol, TestResult};
use sqlx_turso::sqlx::{ConnectOptions, Connection};
use support::*;

fn call() -> TestResult<CallId> {
    Ok(CallId {
        operation: OperationId::new(durable_steps::Uuid::now_v7())?,
        step: StepName::new("status owner control")?,
    })
}

#[tokio::test]
async fn status_stream_corruption_keeps_the_service_error_path_and_mailbox_healthy() -> TestResult {
    let directory = tempfile::tempdir()?;
    let process = ServiceProcess::start(directory.path()).await?;
    let mut control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    let peer = PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000002,
    ))?;
    let mut socket = process
        .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    assert!(matches!(
        request(
            &mut socket,
            OperationRequest::Delivery(DeliveryRequest::StatusFetch {
                call: call()?,
                after: StatusCursor::new(0),
                limit: 10,
            })
        )
        .await?,
        OperationAnswer::<SampleProtocol>::Delivery(DeliveryAnswer::StatusFetch(_))
    ));
    drop(socket);
    process.stop(&mut control).await?;
    let mut connection = sqlx_turso::TursoConnectOptions::new()
        .filename(
            directory
                .path()
                .join("service")
                .join(format!("delivery-status-{}.db", peer.as_uuid())),
        )
        .create_if_missing(false)
        .connect()
        .await?;
    sqlx_core::query::query::<sqlx_turso::Turso>(
        "INSERT INTO _fleet_sync_status_state(singleton,record) VALUES(1,'{}')",
    )
    .execute(&mut connection)
    .await?;
    connection.close().await?;
    let process = ServiceProcess::start(directory.path()).await?;
    let mut control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut socket = process
        .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let failure = request(
        &mut socket,
        OperationRequest::Delivery(DeliveryRequest::StatusFetch {
            call: call()?,
            after: StatusCursor::new(0),
            limit: 10,
        }),
    )
    .await;
    let failure = failure
        .err()
        .ok_or("a status-owner failure must remain a service error")?;
    assert_eq!(
        failure.downcast_ref::<fleet_sync_session::ConnectionClosed>(),
        Some(&fleet_sync_session::ConnectionClosed::Transport)
    );
    drop(socket);
    let mut socket = process
        .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    assert!(
        matches!(request(&mut socket, OperationRequest::Delivery(DeliveryRequest::Query {
        call: call()?, mailbox: peer, items: vec![],
    })).await?, OperationAnswer::<SampleProtocol>::Delivery(DeliveryAnswer::Query(answer)) if answer.statuses.is_empty())
    );
    drop(socket);
    process.stop(&mut control).await?;
    Ok(())
}
