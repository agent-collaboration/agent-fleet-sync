use crate::{
    Consumer, ConsumerStore, FixtureAuthenticator, SampleProtocol, TestResult,
    control::{ControlAnswer, ControlRequest, ControlState},
};
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
use fleet_sync_lease::{LeasePolicy, RegistryConfig};
use fleet_sync_protocol::{CloseReason, ReceiverId};
use fleet_sync_service::{FleetService, ServiceOptions};
use fleet_sync_session::TransportConnection;
use fleet_sync_transport_websocket::AxumConnection;
use std::{path::Path, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
};

struct AcceptedControl {
    socket: WebSocket,
    done: oneshot::Sender<()>,
}
#[derive(Clone)]
struct ControlUpgrade {
    accepted: mpsc::Sender<AcceptedControl>,
}
async fn upgrade(
    State(state): State<ControlUpgrade>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    if headers
        .get("authorization")
        .and_then(|header| header.to_str().ok())
        != Some("Bearer consumer-control")
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    upgrade.on_upgrade(move |socket| async move {
        let (done, finished) = oneshot::channel();
        if state
            .accepted
            .send(AcceptedControl { socket, done })
            .await
            .is_ok()
        {
            let _ = finished.await;
        }
    })
}
pub async fn run_server(directory: &Path, port: u16) -> TestResult {
    run_server_with_receiver(directory, port, 3).await
}
pub async fn run_server_with_receiver(directory: &Path, port: u16, receiver: u128) -> TestResult {
    run_server_with_policy(
        directory,
        port,
        receiver,
        Duration::from_secs(600),
        Duration::from_secs(60),
    )
    .await
}
pub async fn run_server_with_policy(
    directory: &Path,
    port: u16,
    receiver: u128,
    lease_duration: Duration,
    move_renewal_within: Duration,
) -> TestResult {
    #[cfg(feature = "test-support")]
    let events = crate::service_events::ServiceEvents::install()?;
    tokio::fs::create_dir_all(directory).await?;
    let store = ConsumerStore::open(&directory.join("consumer.db")).await?;
    let consumer = Arc::new(Consumer::new(store.clone())?);
    let receiver = ReceiverId::new(durable_steps::Uuid::from_u128(receiver))?;
    let options = ServiceOptions {
        delivery: Some(crate::delivery_limits()?),
        directory: directory.join("service"),
        receiver,
        registry: RegistryConfig {
            lease: LeasePolicy {
                lease_duration,
                renew_interval: Duration::from_secs(30).min(lease_duration / 3),
            },
            authoritative_receiver: receiver,
            target_schema_max: 100,
            release_unsent_for: Duration::from_secs(5),
            move_needs_renewal_within: Some(move_renewal_within),
        },
        call_timeout: Duration::from_secs(10),
        socket_timeout: Duration::from_secs(5),
    };
    let authenticator = Arc::new(FixtureAuthenticator::default());
    let service = FleetService::<SampleProtocol, _, _>::open(
        options,
        consumer.clone(),
        authenticator.clone(),
    )
    .await?;
    let (shutdown, stopped) = watch::channel(false);
    let (source_events, mut source_incoming) =
        mpsc::channel::<fleet_sync_service::AttachedSource<SampleProtocol>>(8);
    let control = Arc::new(ControlState {
        #[cfg(feature = "test-support")]
        delivery_directory: directory.join("service"),
        #[cfg(feature = "test-support")]
        delivery_locks: Default::default(),
        #[cfg(feature = "test-support")]
        events,
        consumer,
        authenticator,
        service: service.handle(),
        shutdown: shutdown.clone(),
        source_events,
    });
    let (accepted, mut incoming) = mpsc::channel::<AcceptedControl>(16);
    let routes = Router::new()
        .route("/control", get(upgrade))
        .with_state(ControlUpgrade { accepted });
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    println!(
        "READY {} {}",
        std::process::id(),
        listener.local_addr()?.port()
    );
    let serving = service.serve(listener, routes, stopped.clone());
    tokio::pin!(serving);
    let mut tasks = JoinSet::new();
    let result = loop {
        tokio::select! {
            result=&mut serving=>break result,
            Some(mut source)=source_incoming.recv()=>{
                let mut shutdown=stopped.clone();
                tasks.spawn(async move {
                    loop {
                        tokio::select! {
                            event=source.events.recv()=>match event {
                                Some(fleet_sync_session::ClientEvent::Notice { position, .. })=>source.requests.acknowledge_notice(position).await?,
                                Some(_)=>{},
                                None=>break,
                            },
                            _=shutdown.changed()=>break,
                        }
                    }
                    Ok(())
                });
            },
            Some(accepted)=incoming.recv()=>{
                let control=control.clone();let stopped=stopped.clone();
                tasks.spawn(async move {
                    let result=serve_control(accepted.socket,control,stopped).await;
                    let _=accepted.done.send(());
                    result
                });
            },
            Some(result)=tasks.join_next(),if !tasks.is_empty()=>{result??;},
        }
    };
    shutdown.send_replace(true);
    incoming.close();
    while let Ok(accepted) = incoming.try_recv() {
        drop(accepted.socket);
        let _ = accepted.done.send(());
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    store.close().await;
    result.map_err(Into::into)
}
async fn serve_control(
    socket: WebSocket,
    state: Arc<ControlState>,
    mut shutdown: watch::Receiver<bool>,
) -> TestResult {
    let mut socket = AxumConnection::new(socket, Duration::from_secs(5));
    loop {
        let frame = tokio::select! {
            frame=socket.receive()=>frame,
            _=shutdown.changed()=>break,
        };
        let Some(frame) = frame else {
            break;
        };
        let request: ControlRequest = serde_json::from_slice(&frame?)?;
        let answer = tokio::select! {
            answer=state.dispatch(request)=>answer,
            _=shutdown.changed()=>ControlAnswer::Error{message:"consumer shutting down".into()},
        };
        socket.send(serde_json::to_vec(&answer)?.into()).await?;
        if *shutdown.borrow() {
            break;
        }
    }
    socket.close(CloseReason::ClientClosed).await;
    Ok(())
}
