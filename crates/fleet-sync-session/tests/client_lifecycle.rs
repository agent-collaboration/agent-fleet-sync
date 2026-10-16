#![allow(clippy::unwrap_used)]
use fleet_sync_protocol::*;
use fleet_sync_session::*;
use runtime_primitives::{Backoff, Clock, IdSource, WakeFailure, WakeScheduler};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::mpsc, time::timeout};
use uuid::Uuid;

#[derive(Clone, Default)]
struct MemoryClientStore(Arc<Mutex<Option<ClientSessionState>>>);
impl SessionClientStore for MemoryClientStore {
    async fn load(&self) -> Result<Option<ClientSessionState>, StoreError> {
        Ok(self.0.lock().unwrap().clone())
    }
    async fn save(&self, state: &ClientSessionState) -> Result<(), StoreError> {
        *self.0.lock().unwrap() = Some(state.clone());
        Ok(())
    }
}
#[derive(Default)]
struct TestHost {
    now: AtomicI64,
    ids: AtomicU64,
    wakes: Mutex<Vec<SyncTime>>,
}
impl Clock for TestHost {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(self.now.load(Ordering::SeqCst))
    }
}
impl IdSource for TestHost {
    fn new_id(&self) -> Uuid {
        Uuid::from_u128(u128::from(self.ids.fetch_add(1, Ordering::SeqCst) + 1))
    }
}
impl WakeScheduler for TestHost {
    async fn wake_at(&self, at: SyncTime) -> Result<(), WakeFailure> {
        self.wakes.lock().unwrap().push(at);
        Ok(())
    }
}

struct ChannelConnection {
    inbound: mpsc::Receiver<Bytes>,
    outbound: mpsc::Sender<Bytes>,
}
struct ChannelTransport(Mutex<VecDeque<ChannelConnection>>);
impl SessionTransport for ChannelTransport {
    type Connection = ChannelConnection;
    async fn connect(
        &self,
        _endpoint: &Endpoint,
        _credential: &PeerCredential,
    ) -> Result<Self::Connection, ConnectFailure> {
        self.0
            .lock()
            .unwrap()
            .pop_front()
            .ok_or(ConnectFailure::Unavailable)
    }
}
impl TransportConnection for ChannelConnection {
    async fn send(&mut self, frame: Bytes) -> Result<(), ConnectionClosed> {
        self.outbound
            .send(frame)
            .await
            .map_err(|_| ConnectionClosed::Transport)
    }
    async fn receive(&mut self) -> Option<Result<Bytes, ConnectionClosed>> {
        self.inbound.recv().await.map(Ok)
    }
    async fn close(self, _reason: CloseReason) {}
}
fn channel() -> (
    ChannelConnection,
    mpsc::Sender<Bytes>,
    mpsc::Receiver<Bytes>,
) {
    let (server_send, inbound) = mpsc::channel(8);
    let (outbound, server_receive) = mpsc::channel(8);
    (
        ChannelConnection { inbound, outbound },
        server_send,
        server_receive,
    )
}
fn backoff() -> Backoff {
    Backoff {
        initial: Duration::from_millis(10),
        max: Duration::from_millis(80),
        factor: 2,
    }
}
fn endpoint() -> Endpoint {
    Endpoint::new("ws://localhost/session").unwrap()
}
fn credential() -> PeerCredential {
    PeerCredential::new("Bearer fixture").unwrap()
}
fn frame_id(value: u128) -> FrameId {
    FrameId::new(Uuid::from_u128(value)).unwrap()
}
async fn send_server(sender: &mpsc::Sender<Bytes>, frame: ServerFrame) {
    sender
        .send(serde_json::to_vec(&frame).unwrap().into())
        .await
        .unwrap();
}
type Client = SessionClient<ChannelTransport, MemoryClientStore>;
async fn read_bytes(reader: &mut mpsc::Receiver<Bytes>) -> Bytes {
    timeout(Duration::from_secs(1), reader.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn event(client: &mut Client, host: &TestHost) -> ClientEvent {
    timeout(Duration::from_secs(1), client.receive(host))
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn pending_frames_become_unknown_and_resume_keeps_the_persisted_session() {
    let host = TestHost::default();
    let store = MemoryClientStore::default();
    let (first, send_first, mut read_first) = channel();
    let (second, send_second, mut read_second) = channel();
    let mut client = Client::load(
        ChannelTransport(Mutex::new(VecDeque::from([first, second]))),
        store.clone(),
        &host,
        SchemaSupport { max_version: 1 },
        backoff(),
    )
    .await
    .unwrap();
    let id = client.state().session_id;
    assert_eq!(store.load().await.unwrap().unwrap().session_id, id);
    client
        .connect(&endpoint(), &credential(), &host)
        .await
        .unwrap();
    assert!(
        matches!(serde_json::from_slice::<ClientFrame>(&read_bytes(&mut read_first).await).unwrap(), ClientFrame::Hello { session_id, .. } if session_id == id)
    );
    send_server(
        &send_first,
        ServerFrame::Welcome {
            session_id: id,
            databases: vec![],
        },
    )
    .await;
    assert!(matches!(
        event(&mut client, &host).await,
        ClientEvent::Welcome { .. }
    ));
    client
        .send_request(frame_id(10), OperationRequest::Consumer(()), &host)
        .await
        .unwrap();
    client
        .send_request(frame_id(11), OperationRequest::Consumer(()), &host)
        .await
        .unwrap();
    drop(send_first);
    assert!(
        matches!(event(&mut client, &host).await, ClientEvent::Unknown { frames } if frames == vec![frame_id(10), frame_id(11)])
    );
    assert!(
        matches!(event(&mut client, &host).await, ClientEvent::Disconnected { retry_at: Some(at), .. } if at.unix_millis() == 10)
    );
    assert!(matches!(
        client.connect(&endpoint(), &credential(), &host).await,
        Err(ClientError::RetryAt { .. })
    ));
    host.now.store(10, Ordering::SeqCst);
    client
        .connect(&endpoint(), &credential(), &host)
        .await
        .unwrap();
    assert!(
        matches!(serde_json::from_slice::<ClientFrame>(&read_bytes(&mut read_second).await).unwrap(), ClientFrame::Hello { session_id, .. } if session_id == id)
    );
    send_server(
        &send_second,
        ServerFrame::Welcome {
            session_id: id,
            databases: vec![],
        },
    )
    .await;
    assert!(matches!(
        event(&mut client, &host).await,
        ClientEvent::Welcome { .. }
    ));
    assert_eq!(
        host.wakes.lock().unwrap().as_slice(),
        &[SyncTime::from_unix_millis(10)]
    );
    client.close().await;
}

#[tokio::test]
async fn answers_correlate_by_frame_and_bidirectional_requests_share_the_transport() {
    let host = TestHost::default();
    let (connection, send, mut read) = channel();
    let mut client = Client::load(
        ChannelTransport(Mutex::new(VecDeque::from([connection]))),
        MemoryClientStore::default(),
        &host,
        SchemaSupport { max_version: 1 },
        backoff(),
    )
    .await
    .unwrap();
    client
        .connect(&endpoint(), &credential(), &host)
        .await
        .unwrap();
    read_bytes(&mut read).await;
    client
        .send_request(frame_id(10), OperationRequest::Consumer(()), &host)
        .await
        .unwrap();
    client
        .send_request(frame_id(11), OperationRequest::Consumer(()), &host)
        .await
        .unwrap();
    read_bytes(&mut read).await;
    read_bytes(&mut read).await;
    send_server(
        &send,
        ServerFrame::Answer {
            frame: frame_id(11),
            body: OperationAnswer::Consumer(()),
        },
    )
    .await;
    send_server(
        &send,
        ServerFrame::Answer {
            frame: frame_id(10),
            body: OperationAnswer::Consumer(()),
        },
    )
    .await;
    assert!(
        matches!(event(&mut client, &host).await, ClientEvent::Answer { frame, .. } if frame == frame_id(11))
    );
    assert!(
        matches!(event(&mut client, &host).await, ClientEvent::Answer { frame, .. } if frame == frame_id(10))
    );
    send_server(
        &send,
        ServerFrame::Request {
            frame: frame_id(12),
            body: (),
        },
    )
    .await;
    assert!(
        matches!(event(&mut client, &host).await, ClientEvent::Request { frame, .. } if frame == frame_id(12))
    );
    client.send_answer(frame_id(12), (), &host).await.unwrap();
    assert!(
        matches!(serde_json::from_slice::<ClientFrame>(&read_bytes(&mut read).await).unwrap(), ClientFrame::Answer { frame, .. } if frame == frame_id(12))
    );
    client.close().await;
}

#[tokio::test]
async fn notice_cursor_advances_only_after_handling_and_survives_client_recreation() {
    let host = TestHost::default();
    let store = MemoryClientStore::default();
    let (connection, send, mut read) = channel();
    let mut client = Client::load(
        ChannelTransport(Mutex::new(VecDeque::from([connection]))),
        store.clone(),
        &host,
        SchemaSupport { max_version: 1 },
        backoff(),
    )
    .await
    .unwrap();
    client
        .connect(&endpoint(), &credential(), &host)
        .await
        .unwrap();
    read_bytes(&mut read).await;
    send_server(
        &send,
        ServerFrame::Notice {
            position: NoticePosition::new(1),
            body: Notice::Consumer(()),
        },
    )
    .await;
    assert!(matches!(
        event(&mut client, &host).await,
        ClientEvent::Notice { .. }
    ));
    assert_eq!(
        store.load().await.unwrap().unwrap().notice_cursor.value(),
        0
    );
    assert!(
        client
            .acknowledge_notice(NoticePosition::new(2))
            .await
            .is_err()
    );
    client
        .acknowledge_notice(NoticePosition::new(1))
        .await
        .unwrap();
    let id = client.state().session_id;
    client.close().await;
    let recreated = Client::load(
        ChannelTransport(Mutex::new(VecDeque::new())),
        store,
        &host,
        SchemaSupport { max_version: 1 },
        backoff(),
    )
    .await
    .unwrap();
    assert_eq!(recreated.state().session_id, id);
    assert_eq!(recreated.state().notice_cursor.value(), 1);
}

#[tokio::test]
async fn ended_session_stops_reconnect_until_a_new_id_is_saved() {
    let host = TestHost::default();
    let store = MemoryClientStore::default();
    let (connection, send, mut read) = channel();
    let mut client = Client::load(
        ChannelTransport(Mutex::new(VecDeque::from([connection]))),
        store.clone(),
        &host,
        SchemaSupport { max_version: 1 },
        backoff(),
    )
    .await
    .unwrap();
    let old = client.state().session_id;
    client
        .connect(&endpoint(), &credential(), &host)
        .await
        .unwrap();
    read_bytes(&mut read).await;
    send_server(
        &send,
        ServerFrame::Closed {
            reason: CloseReason::SessionEnded,
        },
    )
    .await;
    assert!(matches!(
        event(&mut client, &host).await,
        ClientEvent::Disconnected { retry_at: None, .. }
    ));
    assert!(matches!(
        client.connect(&endpoint(), &credential(), &host).await,
        Err(ClientError::SessionEnded)
    ));
    assert!(host.wakes.lock().unwrap().is_empty());
    client.start_new_session(&host).await.unwrap();
    assert_ne!(client.state().session_id, old);
    assert_eq!(
        store.load().await.unwrap().unwrap().session_id,
        client.state().session_id
    );
}

#[tokio::test]
async fn failed_send_reports_unknown_instead_of_an_answer() {
    let host = TestHost::default();
    let (connection, _send, mut read) = channel();
    let mut client = Client::load(
        ChannelTransport(Mutex::new(VecDeque::from([connection]))),
        MemoryClientStore::default(),
        &host,
        SchemaSupport { max_version: 1 },
        backoff(),
    )
    .await
    .unwrap();
    client
        .connect(&endpoint(), &credential(), &host)
        .await
        .unwrap();
    read_bytes(&mut read).await;
    drop(read);
    assert!(matches!(
        client
            .send_request(frame_id(10), OperationRequest::Consumer(()), &host)
            .await,
        Err(ClientError::Connection(_))
    ));
    assert!(
        matches!(event(&mut client, &host).await, ClientEvent::Unknown { frames } if frames == vec![frame_id(10)])
    );
}

#[test]
fn credential_debug_is_redacted_and_endpoints_refuse_embedded_credentials() {
    assert_eq!(format!("{:?}", credential()), "PeerCredential([redacted])");
    assert!(Endpoint::new("ws://user:private@localhost/session").is_err());
    assert!(Endpoint::new("not a URL").is_err());
}
