#[expect(
    dead_code,
    unused_imports,
    reason = "shared native helpers serve other journeys"
)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{SampleDelivery, SampleProtocol, TestResult};
use sqlx_turso::sqlx::{ConnectOptions, Connection};
use support::*;

fn call() -> TestResult<CallId> {
    Ok(CallId {
        operation: OperationId::new(durable_steps::Uuid::now_v7())?,
        step: StepName::new("status owner control")?,
    })
}

#[tokio::test]
async fn status_stream_corruption_keeps_the_service_error_path_and_relay_custody_healthy()
-> TestResult {
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
    let identity = ItemId::new(durable_steps::Uuid::now_v7())?;
    let recipient = PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000004,
    ))?;
    let item = DeliveryItem::<SampleDelivery>::new(
        identity,
        format!("{peer}|authorized sender"),
        Recipient {
            peer: recipient,
            address: "inbox".into(),
        },
        None,
        "healthy custody across status corruption".into(),
    )?;
    let digest = item.digest().clone();
    assert!(
        matches!(request(&mut socket,OperationRequest::Delivery(DeliveryRequest::Submit {call:call()?,item:DeliveryEnvelope::Item(item)})).await?,OperationAnswer::<SampleProtocol>::Delivery(DeliveryAnswer::Submit(SubmitResult::Accepted {status:DeliveryStatus::Sent,version,..})) if version==StatusVersion::new(1))
    );
    drop(socket);
    process.stop(&mut control).await?;
    let mut connection = sqlx_turso::TursoConnectOptions::new()
        .filename(directory.path().join("service").join("registry.db"))
        .create_if_missing(false)
        .connect()
        .await?;
    sqlx_core::query::query::<sqlx_turso::Turso>(
        "INSERT INTO _fleet_sync_status_state(submitter_kind,submitter_id,owner_route,head,floor) VALUES('machine',?1,?2,-1,0)",
    )
    .bind(peer.as_uuid().to_string())
    .bind(serde_json::to_string("fixture-route")?)
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
    let answer = request(
        &mut socket,
        OperationRequest::Delivery(DeliveryRequest::Query {
            call: call()?,
            mailbox: recipient,
            items: vec![identity],
        }),
    )
    .await?;
    let OperationAnswer::<SampleProtocol>::Delivery(DeliveryAnswer::Query(answer)) = answer else {
        return Err("healthy custody query did not return item status".into());
    };
    assert_eq!(answer.statuses.len(), 1);
    let status = answer
        .statuses
        .first()
        .ok_or("healthy item status missing")?;
    assert_eq!(status.item, identity);
    let ItemStatusState::Known {
        digest: stored_digest,
        status,
        version,
        cancel,
    } = &status.state
    else {
        return Err("healthy custody became unknown".into());
    };
    assert_eq!(stored_digest, &digest);
    assert_eq!(status, &DeliveryStatus::Sent);
    assert_eq!(version, &StatusVersion::new(1));
    assert!(cancel.is_none());
    drop(socket);
    process.stop(&mut control).await?;
    Ok(())
}
