#![cfg(feature = "test-support")]
#[path = "support/delivery_node.rs"]
#[expect(dead_code, reason = "this ordering test never kills a hop process")]
mod node;
#[expect(
    dead_code,
    unused_imports,
    reason = "native helpers are shared with the other consumer journeys"
)]
mod support;
use axum::{
    Router,
    extract::ws::{WebSocket, WebSocketUpgrade},
    routing::get,
};
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::{
    SampleProtocol, TestResult,
    delivery_node::{CustodySide, NodeCommand, peer},
};
use fleet_sync_transport_websocket::AxumConnection;
use node::*;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, oneshot};

#[tokio::test]
async fn a_publication_before_welcome_uses_the_same_frame_pump_and_is_acknowledged() -> TestResult {
    let directory = tempfile::tempdir()?;
    let item = item_id(701)?;
    let (completed, completion) = oneshot::channel::<TestResult>();
    let completed = Arc::new(Mutex::new(Some(completed)));
    let router = Router::new().route(
        "/session",
        get(move |upgrade: WebSocketUpgrade| {
            let completed = completed.clone();
            async move {
                upgrade.on_upgrade(move |socket| async move {
                    let result = publish_before_welcome(socket, item).await;
                    if let Some(completed) = completed.lock().await.take() {
                        let _ = completed.send(result);
                    }
                })
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let port = listener.local_addr()?.port();
    let (shutdown, stopped) = oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let mut node = DeliveryNode::start(directory.path(), 2).await?;
    let mut completion = Some(completion);
    let result: TestResult = async {
        node.command(NodeCommand::Enqueue {
            item,
            recipient: peer(4)?,
            body: "the handshake must preserve this item's canonical outcome".into(),
            valid_until: None,
        })
        .await?;
        node.command(NodeCommand::Connect { port }).await?;
        let delivered = node.wait(item, CustodySide::Outbox, "delivered").await?;
        assert_eq!(delivered["outbox"]["state"]["value"]["version"], 3);
        tokio::time::timeout(
            Duration::from_secs(10),
            completion.take().ok_or("completion already consumed")?,
        )
        .await???;
        Ok(())
    }
    .await;
    if result.is_ok() {
        node.stop().await?;
    } else {
        node.kill().await?;
        if let Some(completion) = completion.take() {
            let _ = tokio::time::timeout(Duration::from_secs(10), completion).await;
        }
    }
    let _ = shutdown.send(());
    tokio::time::timeout(Duration::from_secs(10), server).await???;
    result
}

async fn publish_before_welcome(socket: WebSocket, item: ItemId) -> TestResult {
    let mut socket = AxumConnection::new(socket, Duration::from_secs(5));
    let bytes = socket.receive().await.ok_or("no Hello")??;
    let ClientFrame::<SampleProtocol>::Hello { session_id, .. } = serde_json::from_slice(&bytes)?
    else {
        return Err("first client frame was not Hello".into());
    };
    let frame = FrameId::new(durable_steps::Uuid::now_v7())?;
    let publication = ServerFrame::<SampleProtocol>::Request {
        frame,
        body: ServerRequest::Crate(ServerCall::PublishStatus {
            call: CallId {
                operation: OperationId(durable_steps::Uuid::now_v7()),
                step: StepName::new("publish-before-welcome")?,
            },
            publication: StatusPublication {
                item,
                status: DeliveryStatus::Delivered {
                    acceptance: "accepted once".into(),
                },
                version: StatusVersion::new(3),
                cancel: None,
            },
        }),
    };
    socket
        .send(serde_json::to_vec(&publication)?.into())
        .await?;
    // A request is valid before Welcome (SessionClient.receive pumps every frame).
    loop {
        let bytes = tokio::time::timeout(Duration::from_secs(5), socket.receive())
            .await?
            .ok_or("client closed before publication acknowledgment")??;
        match serde_json::from_slice::<ClientFrame<SampleProtocol>>(&bytes)? {
            ClientFrame::Answer {
                frame: answered,
                body:
                    ClientAnswer::Crate(ServerCallAnswer::Published(StatusPublicationResult::Applied)),
            } if answered == frame => break,
            ClientFrame::Request { .. } => {}
            _ => return Err("wrong publication acknowledgment".into()),
        }
    }
    socket
        .send(
            serde_json::to_vec(&ServerFrame::<SampleProtocol>::Welcome {
                session_id,
                databases: Vec::new(),
            })?
            .into(),
        )
        .await?;
    socket.close(CloseReason::ClientClosed).await;
    Ok(())
}
