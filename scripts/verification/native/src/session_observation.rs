//! Adapted from the adopted X7 wire observation, using the repository consumer.
//! Reconnect is held so another socket cannot masquerade as containment.
use fleet_sync_protocol::*;
use fleet_sync_session::*;
use fleet_sync_test_consumer::{ConsumerStore, SampleProtocol, TestResult};
use fleet_sync_tokio_runner::{SessionPump, SessionRequests};
use fleet_sync_transport_websocket::{WebSocketConnection, WebSocketTransport};
use runtime_primitives::{Backoff, UuidV7Ids};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
    time::timeout,
};

#[derive(Default)]
pub struct WireObservation {
    pub lose_next_applied: AtomicBool,
    pub corrupt_next_unknown_reason: AtomicBool,
    pub connections: AtomicU64,
    pub frames: Mutex<Vec<Value>>,
}
struct ObservedTransport(Arc<WireObservation>);
struct ObservedConnection {
    socket: WebSocketConnection,
    observation: Arc<WireObservation>,
}
impl SessionTransport for ObservedTransport {
    type Connection = ObservedConnection;
    async fn connect(
        &self,
        endpoint: &Endpoint,
        credential: &PeerCredential,
    ) -> Result<Self::Connection, ConnectFailure> {
        let number = self.0.connections.fetch_add(1, Ordering::SeqCst) + 1;
        if number > 1 {
            self.0
                .frames
                .lock()
                .await
                .push(json!({"event":"reconnect-held","connection":number}));
            std::future::pending::<()>().await;
        }
        let socket = WebSocketTransport::default()
            .connect(endpoint, credential)
            .await?;
        self.0
            .frames
            .lock()
            .await
            .push(json!({"event":"connected","connection":number}));
        Ok(ObservedConnection {
            socket,
            observation: self.0.clone(),
        })
    }
}
impl TransportConnection for ObservedConnection {
    async fn send(&mut self, bytes: Bytes) -> Result<(), ConnectionClosed> {
        let frame: Value =
            serde_json::from_slice(&bytes).map_err(|_| ConnectionClosed::UnsupportedFrame)?;
        self.observation
            .frames
            .lock()
            .await
            .push(json!({"direction":"out","frame":frame}));
        self.socket.send(bytes).await
    }
    async fn receive(&mut self) -> Option<Result<Bytes, ConnectionClosed>> {
        let received = self.socket.receive().await;
        let frame = match &received {
            Some(Ok(bytes)) => serde_json::from_slice::<Value>(bytes).ok(),
            _ => None,
        };
        self.observation.frames.lock().await.push(json!({"direction":"in","frame":frame,"transportEnded":!matches!(received,Some(Ok(_)))}));
        if frame.as_ref().is_some_and(|frame| {
            frame["kind"] == "answer"
                && frame["value"]["body"]["kind"] == "replication"
                && frame["value"]["body"]["value"]["kind"] == "outcomeUnknown"
        }) && self
            .observation
            .corrupt_next_unknown_reason
            .swap(false, Ordering::SeqCst)
        {
            let mut changed = frame?;
            changed["value"]["body"]["value"]["value"]["reason"]["kind"] =
                json!("futureStoreBoundary");
            self.observation
                .frames
                .lock()
                .await
                .push(json!({"event":"unknown-reason-injected","frame":changed}));
            return Some(
                serde_json::to_vec(&changed)
                    .map(Into::into)
                    .map_err(|_| ConnectionClosed::UnsupportedFrame),
            );
        }
        if frame.as_ref().is_some_and(|frame| {
            frame["kind"] == "answer"
                && frame["value"]["body"]["kind"] == "replication"
                && frame["value"]["body"]["value"]["kind"] == "applied"
        }) && self
            .observation
            .lose_next_applied
            .swap(false, Ordering::SeqCst)
        {
            self.observation
                .frames
                .lock()
                .await
                .push(json!({"event":"committed-answer-dropped"}));
            return Some(Err(ConnectionClosed::Transport));
        }
        received
    }
    async fn close(self, reason: CloseReason) {
        self.socket.close(reason).await;
    }
}

pub struct ObservedSession {
    pub requests: SessionRequests<SampleProtocol>,
    pub wire: Arc<WireObservation>,
    stop: watch::Sender<bool>,
    pumping: JoinHandle<TestResult>,
    events: JoinHandle<()>,
    store: ConsumerStore,
}
impl ObservedSession {
    pub async fn start(endpoint: &str, peer: PeerId, path: &Path) -> TestResult<Self> {
        let wire = Arc::new(WireObservation::default());
        let store = ConsumerStore::open(path).await?;
        let client = SessionClient::<_, _, SampleProtocol>::load(
            ObservedTransport(wire.clone()),
            store.clone(),
            &UuidV7Ids,
            SchemaSupport { max_version: 100 },
            Backoff {
                initial: Duration::from_millis(10),
                max: Duration::from_millis(50),
                factor: 2,
            },
        )
        .await?;
        let (requests, mut incoming, pump) = SessionPump::new(
            client,
            Endpoint::new(endpoint)?,
            PeerCredential::new(format!("Bearer peer-{}", peer.as_uuid().as_u128()))?,
            Duration::from_secs(3),
        );
        let (stop, stopped) = watch::channel(false);
        let pumping = tokio::spawn(async move {
            pump.run(stopped).await?;
            Ok(())
        });
        match timeout(Duration::from_secs(15), incoming.recv()).await? {
            Some(ClientEvent::Welcome { .. }) => {}
            _ => {
                stop.send_replace(true);
                timeout(Duration::from_secs(10), pumping).await???;
                return Err("observed session has no Welcome".into());
            }
        }
        let observed = wire.clone();
        let acknowledgement = requests.clone();
        let events = tokio::spawn(async move {
            while let Some(event) = incoming.recv().await {
                match event {
                    ClientEvent::Notice { position, .. } => {
                        let _ = acknowledgement.acknowledge_notice(position).await;
                    }
                    ClientEvent::Disconnected { .. } => observed
                        .frames
                        .lock()
                        .await
                        .push(json!({"event":"disconnected"})),
                    ClientEvent::Unknown { .. } => observed
                        .frames
                        .lock()
                        .await
                        .push(json!({"event":"unknown"})),
                    _ => {}
                }
            }
        });
        Ok(Self {
            requests,
            wire,
            stop,
            pumping,
            events,
            store,
        })
    }
    pub async fn stop(self) -> TestResult<Vec<Value>> {
        self.stop.send_replace(true);
        timeout(Duration::from_secs(10), self.pumping).await???;
        timeout(Duration::from_secs(10), self.events).await??;
        self.store.close().await;
        Ok(self.wire.frames.lock().await.clone())
    }
}

pub fn sent_pieces(frames: &[Value]) -> Vec<Value> {
    frames
        .iter()
        .filter_map(|row| {
            let body = &row["frame"]["value"]["body"];
            (row["direction"] == "out"
                && body["kind"] == "replication"
                && body["value"]["kind"] == "piece")
                .then(|| body["value"]["value"].clone())
        })
        .collect()
}
