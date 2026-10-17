use super::{session_fixture_contract::*, session_fixture_host::*};
use axum::{
    Router,
    extract::{
        State,
        ws::{WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use fleet_sync_protocol::*;
use fleet_sync_session::*;
use fleet_sync_transport_websocket::AxumConnection;
use std::{future::IntoFuture, io::Write, path::Path, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinSet,
    time::timeout,
};

struct AcceptedSocket {
    socket: WebSocket,
    peer: Option<VerifiedPeer>,
    done: oneshot::Sender<()>,
}
#[derive(Clone)]
struct UpgradeState {
    host: FixtureHost,
    accepted: mpsc::Sender<AcceptedSocket>,
}

async fn session_upgrade(
    State(state): State<UpgradeState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let material = headers
        .get("authorization")
        .and_then(|header| header.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let verified = match state.host.verify(material).await {
        Ok(peer) => peer,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };
    upgrade.on_upgrade(move |socket| async move {
        let (done, finished) = oneshot::channel();
        if state
            .accepted
            .send(AcceptedSocket {
                socket,
                peer: Some(verified),
                done,
            })
            .await
            .is_ok()
        {
            let _ = finished.await;
        }
    })
}
async fn control_upgrade(
    State(state): State<UpgradeState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    if headers
        .get("authorization")
        .and_then(|header| header.to_str().ok())
        != Some("Bearer control")
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    upgrade.on_upgrade(move |socket| async move {
        let (done, finished) = oneshot::channel();
        if state
            .accepted
            .send(AcceptedSocket {
                socket,
                peer: None,
                done,
            })
            .await
            .is_ok()
        {
            let _ = finished.await;
        }
    })
}

pub async fn run_server(path: &Path) -> TestResult {
    let host = FixtureHost::open_and_migrate(path).await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (accepted, mut incoming) = mpsc::channel(16);
    let app = Router::new()
        .route("/session", get(session_upgrade))
        .route("/control", get(control_upgrade))
        .with_state(UpgradeState {
            host: host.clone(),
            accepted,
        });
    let mut server_stop = host.stop.subscribe();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            if !*server_stop.borrow() {
                let _ = server_stop.changed().await;
            }
        })
        .into_future();
    tokio::pin!(server);
    let mut tasks = JoinSet::new();
    let mut stop = host.stop.subscribe();
    let mut server_finished = false;
    println!("READY {} {}", std::process::id(), address.port());
    std::io::stdout().flush()?;
    loop {
        tokio::select! {
            result = &mut server => { result?; if *stop.borrow() { server_finished = true; break; } return Err("fixture server stopped before its command".into()); }
            changed = stop.changed() => { changed?; if *stop.borrow() { break; } }
            accepted = incoming.recv() => {
                if let Some(accepted) = accepted {
                    let current = host.clone();
                    tasks.spawn(async move {
                        let result = handle_socket(current, accepted.socket, accepted.peer).await;
                        let _ = accepted.done.send(());
                        result
                    });
                }
            }
            joined = tasks.join_next(), if !tasks.is_empty() => { if let Some(result) = joined { result??; } }
        }
    }
    incoming.close();
    while let Ok(accepted) = incoming.try_recv() {
        drop(accepted.socket);
        let _ = accepted.done.send(());
    }
    timeout(Duration::from_secs(5), async {
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    if !server_finished {
        timeout(Duration::from_secs(5), server).await??;
    }
    Ok(())
}

async fn handle_socket(
    host: FixtureHost,
    socket: WebSocket,
    peer: Option<VerifiedPeer>,
) -> TestResult {
    let mut connection = AxumConnection::new(socket, Duration::from_secs(2));
    let Some(peer) = peer else {
        return handle_control(host, connection).await;
    };
    let binding = ConnectionBinding::new(ConnectionId::new(uuid::Uuid::now_v7())?, peer);
    let (sender, mut commands) = mpsc::channel(32);
    host.database.lock().await.sockets.insert(
        binding.connection,
        SocketPeer {
            peer: binding.peer,
            sender,
        },
    );
    host.dispatch(host.apply(binding, RuleInput::Opened).await?)
        .await?;
    let mut clock = host.clock.subscribe();
    let mut stop = host.stop.subscribe();
    let mut close_reason = None;
    loop {
        if *stop.borrow() {
            break;
        }
        tokio::select! {
            incoming = connection.receive() => {
                match incoming {
                    Some(Ok(frame)) => host.dispatch(host.apply(binding, RuleInput::Frame(frame)).await?).await?,
                    Some(Err(_)) | None => break,
                }
            }
            command = commands.recv() => {
                match command {
                    Some(SocketCommand::Send(bytes)) => { if connection.send(bytes).await.is_err() { break; } }
                    Some(SocketCommand::Close(reason)) => { close_reason = Some(reason); break; }
                    Some(SocketCommand::Drop) | None => break,
                }
            }
            changed = clock.changed() => {
                changed?;
                if binding.expires_at <= *clock.borrow_and_update() {
                    // The host enforces an idle binding deadline. The session rules
                    // alone decide the resulting logical session state.
                    host.apply(binding, RuleInput::Closed(CloseCause::CredentialExpired)).await?;
                    let reason = CloseReason::CredentialRejected(AuthRefusal::Expired);
                    connection.send(serde_json::to_vec(&ServerFrame::<FixtureProtocol>::Closed { reason: reason.clone() })?.into()).await?;
                    close_reason = Some(reason);
                    break;
                }
            }
            changed = stop.changed() => { changed?; if *stop.borrow() { break; } }
        }
    }
    if let Some(reason) = close_reason {
        connection.close(reason).await;
    } else {
        drop(connection);
    }
    let held = {
        let mut database = host.database.lock().await;
        if database.hold_next_close {
            database.hold_next_close = false;
            let (release, released) = oneshot::channel();
            let (done, completed) = oneshot::channel();
            database.held_closes.push(HeldClose {
                release,
                done: completed,
            });
            host.close_held.send_replace(true);
            Some((released, done))
        } else {
            None
        }
    };
    let completed = if let Some((released, done)) = held {
        if !*stop.borrow() {
            tokio::select! { _ = released => {}, _ = stop.changed() => {} }
        }
        Some(done)
    } else {
        None
    };
    host.apply(binding, RuleInput::Closed(CloseCause::Dropped))
        .await?;
    host.database
        .lock()
        .await
        .sockets
        .remove(&binding.connection);
    if let Some(done) = completed {
        let _ = done.send(());
    }
    Ok(())
}

async fn handle_control(host: FixtureHost, mut connection: AxumConnection) -> TestResult {
    let mut stop = host.stop.subscribe();
    loop {
        if *stop.borrow() {
            break;
        }
        let bytes = tokio::select! { incoming = connection.receive() => match incoming { Some(Ok(bytes)) => bytes, _ => break }, _ = stop.changed() => break };
        let request: ControlRequest = serde_json::from_slice(&bytes)?;
        let stopping = matches!(request, ControlRequest::Stop);
        let snapshot = host.control(request).await?;
        connection
            .send(serde_json::to_vec(&snapshot)?.into())
            .await?;
        if stopping {
            break;
        }
    }
    connection.close(CloseReason::ClientClosed).await;
    Ok(())
}
