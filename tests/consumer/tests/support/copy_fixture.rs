use crate::support::ServiceProcess;
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::*;
use fleet_sync_session::*;
use fleet_sync_test_consumer::{
    ConsumerStore, SampleProtocol, SampleSchema, TestResult, insert_sample,
};
use fleet_sync_tokio_runner::{SessionPump, SessionRequests, WriterSessionCalls};
use fleet_sync_transport_websocket::{WebSocketConnection, WebSocketTransport};
use fleet_sync_writer::*;
use runtime_primitives::{Backoff, Clock, SystemClock, UuidV7Ids};
use serde::Serialize;
use serde_json::{Value, json};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
};

pub fn writer_peer() -> TestResult<PeerId> {
    Ok(PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000002,
    ))?)
}
pub fn local_receiver() -> TestResult<ReceiverId> {
    Ok(ReceiverId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000003,
    ))?)
}
pub fn lease_policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(600),
        renew_interval: Duration::from_secs(30),
    }
}

#[derive(Default)]
struct Wire {
    frames: Mutex<Vec<(bool, Value)>>,
    connections: AtomicUsize,
    welcomes: AtomicUsize,
    ready: tokio::sync::Notify,
    unknown_reason: AtomicBool,
}
struct RecordingTransport(Arc<Wire>);
struct RecordingConnection {
    inner: WebSocketConnection,
    wire: Arc<Wire>,
}
impl SessionTransport for RecordingTransport {
    type Connection = RecordingConnection;
    async fn connect(
        &self,
        endpoint: &Endpoint,
        credential: &PeerCredential,
    ) -> Result<Self::Connection, ConnectFailure> {
        let inner = WebSocketTransport::default()
            .connect(endpoint, credential)
            .await?;
        self.0.connections.fetch_add(1, Ordering::SeqCst);
        Ok(RecordingConnection {
            inner,
            wire: self.0.clone(),
        })
    }
}
impl TransportConnection for RecordingConnection {
    async fn send(&mut self, frame: Bytes) -> Result<(), ConnectionClosed> {
        if let Ok(value) = serde_json::from_slice(&frame) {
            self.wire.frames.lock().await.push((true, value));
        }
        self.inner.send(frame).await
    }
    async fn receive(&mut self) -> Option<Result<Bytes, ConnectionClosed>> {
        let result = self.inner.receive().await;
        if let Some(Ok(bytes)) = &result
            && let Ok(value) = serde_json::from_slice::<Value>(bytes)
        {
            self.wire.frames.lock().await.push((false, value.clone()));
            if value["value"]["body"]["value"]["kind"] == "outcomeUnknown"
                && self.wire.unknown_reason.swap(false, Ordering::SeqCst)
            {
                // Fault at the client's byte boundary, after the real producer
                // and socket. An undecodable reply cannot prove no effect.
                let mut changed = value;
                changed["value"]["body"]["value"]["value"]["reason"]["kind"] =
                    "futureStoreBoundary".into();
                if let Ok(bytes) = serde_json::to_vec(&changed) {
                    return Some(Ok(bytes.into()));
                }
            }
        }
        result
    }
    async fn close(self, reason: CloseReason) {
        self.inner.close(reason).await;
    }
}

pub struct CopySession {
    pub requests: SessionRequests<SampleProtocol>,
    wire: Arc<Wire>,
    stop: watch::Sender<bool>,
    pumping: JoinHandle<Result<(), ClientError>>,
    events: JoinHandle<()>,
    store: ConsumerStore,
}
impl CopySession {
    pub fn inject_unknown_reason(&self) {
        self.wire.unknown_reason.store(true, Ordering::SeqCst);
    }
    pub async fn wait_welcomes(&self, expected: usize) -> TestResult {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let ready = self.wire.ready.notified();
                tokio::pin!(ready);
                ready.as_mut().enable();
                if self.wire.welcomes.load(Ordering::SeqCst) >= expected {
                    return;
                }
                ready.await;
            }
        })
        .await?;
        Ok(())
    }
    pub async fn start(port: u16, path: &Path) -> TestResult<Self> {
        let wire = Arc::new(Wire::default());
        let store = ConsumerStore::open(path).await?;
        let client = SessionClient::<_, _, SampleProtocol>::load(
            RecordingTransport(wire.clone()),
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
            Endpoint::new(&format!("ws://127.0.0.1:{port}/session"))?,
            PeerCredential::new("Bearer peer-2")?,
            Duration::from_secs(3),
        );
        let (stop, stopped) = watch::channel(false);
        let pumping = tokio::spawn(pump.run(stopped));
        if !matches!(
            tokio::time::timeout(Duration::from_secs(15), incoming.recv()).await,
            Ok(Some(ClientEvent::Welcome { .. }))
        ) {
            stop.send_replace(true);
            let stopped = pumping.await;
            store.close().await;
            stopped??;
            return Err("copy fixture did not receive Welcome".into());
        }
        wire.welcomes.store(1, Ordering::SeqCst);
        let acknowledging = requests.clone();
        let readiness = wire.clone();
        let events = tokio::spawn(async move {
            while let Some(event) = incoming.recv().await {
                match event {
                    ClientEvent::Welcome { .. } => {
                        readiness.welcomes.fetch_add(1, Ordering::SeqCst);
                        readiness.ready.notify_waiters();
                    }
                    ClientEvent::Notice { position, .. } => {
                        let _ = acknowledging.acknowledge_notice(position).await;
                    }
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
    pub fn connection_count(&self) -> usize {
        self.wire.connections.load(Ordering::SeqCst)
    }
    pub async fn outgoing_replication(&self) -> Vec<Value> {
        self.wire
            .frames
            .lock()
            .await
            .iter()
            .filter_map(|(outgoing, frame)| {
                let body = &frame["value"]["body"];
                (*outgoing && body["kind"] == "replication").then(|| body["value"].clone())
            })
            .collect()
    }
    pub async fn outgoing_pieces(&self, database: DatabaseId) -> Vec<Value> {
        self.wire
            .frames
            .lock()
            .await
            .iter()
            .filter_map(|(outgoing, frame)| {
                let body = &frame["value"]["body"];
                (*outgoing
                    && body["kind"] == "replication"
                    && body["value"]["kind"] == "piece"
                    && body["value"]["value"]["database"] == json!(database))
                .then(|| body["value"]["value"].clone())
            })
            .collect()
    }
    pub async fn copy_unavailable_answer(&self, database: DatabaseId) -> Option<Value> {
        let frames = self.wire.frames.lock().await;
        frames
            .iter()
            .filter(|(outgoing, frame)| {
                *outgoing
                    && frame["value"]["body"]["kind"] == "replication"
                    && frame["value"]["body"]["value"]["kind"] == "piece"
                    && frame["value"]["body"]["value"]["value"]["database"] == json!(database)
            })
            .find_map(|(_, offered)| {
                frames
                    .iter()
                    .find(|(outgoing, reply)| {
                        !*outgoing
                            && reply["kind"] == "answer"
                            && reply["value"]["frame"] == offered["value"]["frame"]
                            && reply["value"]["body"]["kind"] == "replication"
                            && reply["value"]["body"]["value"]["kind"] == "copyUnavailable"
                    })
                    .map(|(_, reply)| reply["value"]["body"]["value"]["value"]["reason"].clone())
            })
    }
    pub async fn stop(self) -> TestResult {
        self.stop.send_replace(true);
        let pumping = self.pumping.await;
        let events = self.events.await;
        self.store.close().await;
        pumping??;
        events?;
        Ok(())
    }
}

pub struct CopyService {
    pub session: CopySession,
    pub control: WebSocketConnection,
    process: ServiceProcess,
}
impl CopyService {
    pub async fn kill(mut self) -> TestResult {
        let session = self.session.stop().await;
        let killed = self.process.kill().await;
        drop(self.control);
        killed?;
        session
    }
    pub async fn start(root: &Path, client: &Path) -> TestResult<Self> {
        let mut process = ServiceProcess::start(root).await?;
        let control = match process.connect("control", "Bearer consumer-control").await {
            Ok(control) => control,
            Err(error) => {
                process.kill().await?;
                return Err(error);
            }
        };
        let session = match CopySession::start(process.port, client).await {
            Ok(session) => session,
            Err(error) => {
                let mut control = control;
                process.stop(&mut control).await?;
                return Err(error);
            }
        };
        Ok(Self {
            session,
            control,
            process,
        })
    }
    pub async fn stop(self) -> TestResult {
        let mut control = self.control;
        let session = self.session.stop().await;
        let service = self.process.stop(&mut control).await;
        session?;
        service
    }
}

pub struct Annotation;
impl PieceAnnotation for Annotation {
    type Protocol = SampleProtocol;
    fn annotate(&self, _: &[CapturedTransaction<'_>]) -> Option<String> {
        None
    }
}
pub type CopyReplicationSender = ReplicationSender<
    SampleSchema,
    Annotation,
    (),
    WriterSessionCalls<SampleSchema, SampleProtocol>,
>;
pub fn replication_sender(
    writer: Arc<ReplicatedDatabase<SampleSchema>>,
    session: &CopySession,
) -> TestResult<CopyReplicationSender> {
    Ok(ReplicationSender::new(
        writer.clone(),
        Arc::new(Annotation),
        Arc::new(()),
        Arc::new(WriterSessionCalls::new(
            writer,
            local_receiver()?,
            session.requests.clone(),
        )),
        SenderOptions::default(),
    ))
}
pub async fn register_writer(
    writer: &ReplicatedDatabase<SampleSchema>,
    session: &CopySession,
) -> TestResult<LeaseStatus> {
    let lease = match session
        .requests
        .request(OperationRequest::Lease(LeaseRequest::Register(
            Registration {
                attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
                database: writer.database(),
                writer: writer_peer()?,
                origin: Origin::Empty,
            },
        )))
        .await?
    {
        OperationAnswer::Lease(LeaseAnswer::Register(RegistrationAnswer::Registered {
            lease,
            ..
        })) => lease,
        _ => return Err("copy fixture registration failed".into()),
    };
    let started = SystemClock.now();
    match session
        .requests
        .request(OperationRequest::Lease(LeaseRequest::Renew(RenewRequest {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database: writer.database(),
            epoch: lease.epoch,
        })))
        .await?
    {
        OperationAnswer::Lease(LeaseAnswer::Renew(answer)) => {
            writer.observe_renewal(answer, started).await?
        }
        _ => return Err("copy fixture renewal failed".into()),
    }
    Ok(lease)
}
pub async fn write_sample(writer: &ReplicatedDatabase<SampleSchema>, label: &str) -> TestResult {
    let mut write = writer.begin_write().await?;
    insert_sample(
        &mut write,
        durable_steps::Uuid::now_v7().to_string(),
        label.into(),
        9_007_199_254_740_993,
        vec![0, 255, 128],
    )
    .await?;
    write.commit().await?;
    Ok(())
}
pub fn copy_file_path(service_root: &Path, database: DatabaseId) -> TestResult<PathBuf> {
    Ok(service_root
        .join("service")
        .join(format!("copy-{}-{database}.db", local_receiver()?)))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CopySnapshot {
    pub record: Option<String>,
    pub rows: Vec<(String, String)>,
    pub receipts: Vec<(String, String)>,
    pub fences: Vec<(String, String)>,
    pub hooks: Vec<(String, String)>,
}
pub async fn read_copy_snapshot(path: &Path) -> TestResult<CopySnapshot> {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .connect()
        .await?;
    let record = query::<Turso>("SELECT record FROM _fleet_sync_copy WHERE singleton=1")
        .fetch_optional(&mut connection)
        .await?
        .map(|row| row.try_get("record"))
        .transpose()?;
    let rows = query::<Turso>("SELECT id,label FROM samples ORDER BY id")
        .fetch_all(&mut connection)
        .await?
        .into_iter()
        .map(|row| Ok((row.try_get("id")?, row.try_get("label")?)))
        .collect::<Result<_, sqlx_core::Error>>()?;
    let receipts =
        query::<Turso>("SELECT identity,digest FROM _fleet_sync_receipts ORDER BY identity")
            .fetch_all(&mut connection)
            .await?
            .into_iter()
            .map(|row| Ok((row.try_get("identity")?, row.try_get("digest")?)))
            .collect::<Result<_, sqlx_core::Error>>()?;
    let fences =
        query::<Turso>("SELECT transfer,result FROM _fleet_sync_fence_receipts ORDER BY transfer")
            .fetch_all(&mut connection)
            .await?
            .into_iter()
            .map(|row| Ok((row.try_get("transfer")?, row.try_get("result")?)))
            .collect::<Result<_, sqlx_core::Error>>()?;
    let hooks = query::<Turso>("SELECT event_id,annotation FROM hook_events ORDER BY event_id")
        .fetch_all(&mut connection)
        .await?
        .into_iter()
        .map(|row| Ok((row.try_get("event_id")?, row.try_get("annotation")?)))
        .collect::<Result<_, sqlx_core::Error>>()?;
    connection.close().await?;
    Ok(CopySnapshot {
        record,
        rows,
        receipts,
        fences,
        hooks,
    })
}
pub async fn replace_marker(path: &Path, record: &str) -> TestResult {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .connect()
        .await?;
    let changed = query::<Turso>("UPDATE _fleet_sync_copy SET record=? WHERE singleton=1")
        .bind(record.to_owned())
        .execute(&mut connection)
        .await?
        .rows_affected();
    connection.close().await?;
    if changed != 1 {
        return Err("copy marker fault/repair must affect exactly one row".into());
    }
    Ok(())
}

pub async fn pending_follow_ups(path: &Path) -> TestResult<Vec<OperationId>> {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .connect()
        .await?;
    let pending = query::<Turso>("SELECT operation_id FROM _fleet_sync_operations WHERE kind='fleet-sync.apply-follow-up' AND state='running' ORDER BY operation_id")
        .fetch_all(&mut connection).await?.into_iter()
        .map(|row| OperationId::try_from(row.try_get::<String,_>("operation_id")?).map_err(sqlx_core::Error::protocol))
        .collect::<Result<Vec<_>, _>>();
    connection.close().await?;
    Ok(pending?)
}
