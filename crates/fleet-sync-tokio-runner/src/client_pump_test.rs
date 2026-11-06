#![allow(clippy::unwrap_used, clippy::panic)]
use super::*;
use crate::{
    CredentialRejectedRecovery, SessionEndedRecovery, SessionLinkEvent, SessionRecoveryPolicy,
};
use fleet_sync_protocol::*;
use fleet_sync_session::{
    Bytes, ClientSessionState, ConnectFailure, ConnectionClosed, StoreError, TransportConnection,
};
use runtime_primitives::{Backoff, SyncTime, UuidV7Ids};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::time::{Instant, timeout};

#[derive(Clone, Default)]
struct MemoryStore(Arc<Mutex<Option<ClientSessionState>>>);
impl SessionClientStore for MemoryStore {
    async fn load(&self) -> Result<Option<ClientSessionState>, StoreError> {
        Ok(self.0.lock().unwrap().clone())
    }
    async fn save(&self, state: &ClientSessionState) -> Result<(), StoreError> {
        *self.0.lock().unwrap() = Some(state.clone());
        Ok(())
    }
}
#[derive(Clone)]
struct TestClock(Instant);
impl Clock for TestClock {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(i64::try_from(self.0.elapsed().as_millis()).unwrap())
    }
}
struct ScriptedTransport {
    connections: Mutex<VecDeque<Result<ScriptedConnection, ConnectFailure>>>,
    attempts: Arc<AtomicUsize>,
}
impl SessionTransport for ScriptedTransport {
    type Connection = ScriptedConnection;
    async fn connect(
        &self,
        _: &Endpoint,
        _: &PeerCredential,
    ) -> Result<Self::Connection, ConnectFailure> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.connections
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Err(ConnectFailure::Unavailable))
    }
}
struct ScriptedConnection {
    incoming: mpsc::Receiver<Bytes>,
    outgoing: mpsc::Sender<Bytes>,
    closed: mpsc::UnboundedSender<CloseReason>,
}
impl TransportConnection for ScriptedConnection {
    async fn send(&mut self, frame: Bytes) -> Result<(), ConnectionClosed> {
        self.outgoing
            .send(frame)
            .await
            .map_err(|_| ConnectionClosed::Transport)
    }
    async fn receive(&mut self) -> Option<Result<Bytes, ConnectionClosed>> {
        self.incoming.recv().await.map(Ok)
    }
    async fn close(self, reason: CloseReason) {
        self.closed.send(reason).unwrap();
    }
}
struct ServerSocket {
    send: mpsc::Sender<Bytes>,
    received: mpsc::Receiver<Bytes>,
    closed: mpsc::UnboundedReceiver<CloseReason>,
}
fn socket() -> (ScriptedConnection, ServerSocket) {
    let (send, incoming) = mpsc::channel(8);
    let (outgoing, received) = mpsc::channel(8);
    let (closed, closure) = mpsc::unbounded_channel();
    (
        ScriptedConnection {
            incoming,
            outgoing,
            closed,
        },
        ServerSocket {
            send,
            received,
            closed: closure,
        },
    )
}
impl ServerSocket {
    async fn receive(&mut self) -> ClientFrame {
        serde_json::from_slice(
            &timeout(Duration::from_secs(10), self.received.recv())
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap()
    }
    async fn send(&self, frame: ServerFrame) {
        self.send
            .send(serde_json::to_vec(&frame).unwrap().into())
            .await
            .unwrap();
    }
    async fn welcome(&mut self) -> SessionId {
        let ClientFrame::Hello { session_id, .. } = self.receive().await else {
            panic!("expected Hello")
        };
        self.send(ServerFrame::Welcome {
            session_id,
            databases: vec![],
        })
        .await;
        session_id
    }
    async fn answer_request(&mut self, requests: &SessionRequests<NoConsumer>) {
        let request = requests.request(OperationRequest::Consumer(()));
        tokio::pin!(request);
        tokio::select! {
            biased;
            _ = &mut request => panic!("request returned before server answer"),
            frame = self.receive() => {
                let ClientFrame::Request { frame, .. } = frame else { panic!("expected Request") };
                self.send(ServerFrame::Answer { frame, body: OperationAnswer::Consumer(()) }).await;
            }
        }
        assert!(matches!(request.await, Ok(OperationAnswer::Consumer(()))));
    }
}
fn backoff() -> Backoff {
    Backoff {
        initial: Duration::from_secs(1),
        max: Duration::from_secs(4),
        factor: 2,
    }
}
fn policy() -> SessionRecoveryPolicy {
    SessionRecoveryPolicy {
        credential_rejected: CredentialRejectedRecovery::Retry { backoff: backoff() },
        session_ended: SessionEndedRecovery::StartNewSession,
        welcome_timeout: Some(Duration::from_secs(5)),
    }
}
type Pump = SessionPump<ScriptedTransport, MemoryStore, NoConsumer>;
async fn setup(
    connections: Vec<Result<ScriptedConnection, ConnectFailure>>,
) -> (
    SessionRequests<NoConsumer>,
    mpsc::Receiver<ClientEvent>,
    Pump,
    Arc<AtomicUsize>,
    MemoryStore,
) {
    let attempts = Arc::new(AtomicUsize::new(0));
    let store = MemoryStore::default();
    let client = SessionClient::load(
        ScriptedTransport {
            connections: Mutex::new(connections.into()),
            attempts: attempts.clone(),
        },
        store.clone(),
        &UuidV7Ids,
        SchemaSupport { max_version: 1 },
        backoff(),
    )
    .await
    .unwrap();
    let (requests, events, pump) = SessionPump::new(
        client,
        Endpoint::new("ws://localhost/session").unwrap(),
        PeerCredential::new("Bearer fixture").unwrap(),
        Duration::from_secs(10),
    );
    (requests, events, pump, attempts, store)
}
async fn next_link(
    events: &mut tokio::sync::broadcast::Receiver<SessionLinkEvent>,
) -> SessionLinkEvent {
    timeout(Duration::from_secs(10), events.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn welcomed(events: &mut mpsc::Receiver<ClientEvent>) {
    assert!(matches!(
        timeout(Duration::from_secs(10), events.recv())
            .await
            .unwrap()
            .unwrap(),
        ClientEvent::Welcome { .. }
    ));
}

#[tokio::test(start_paused = true)]
async fn credential_refusal_retries_with_backoff_and_preserves_request_handle() {
    let (connection, mut server) = socket();
    let (requests, mut incoming, pump, attempts, _) = setup(vec![
        Err(ConnectFailure::CredentialRejected(AuthRefusal::Unknown)),
        Ok(connection),
    ])
    .await;
    let pump = pump.with_recovery_policy(policy());
    let state = pump.link_state();
    let mut links = pump.observe_link();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(pump.run_with_clock(stopped, TestClock(Instant::now())));
    assert_eq!(
        next_link(&mut links).await,
        SessionLinkEvent::Connecting { attempt: 1 }
    );
    assert_eq!(
        next_link(&mut links).await,
        SessionLinkEvent::CredentialRejected {
            reason: AuthRefusal::Unknown
        }
    );
    assert_eq!(
        state.borrow().status,
        crate::SessionLinkStatus::CredentialRejected {
            reason: AuthRefusal::Unknown,
            retry_at: Some(SyncTime::from_unix_millis(1000))
        }
    );
    assert!(matches!(
        requests.request(OperationRequest::Consumer(())).await,
        Err(CallFailure::NotSent { .. })
    ));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_millis(999)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_millis(1)).await;
    server.welcome().await;
    welcomed(&mut incoming).await;
    tokio::time::advance(Duration::from_secs(5)).await;
    server.answer_request(&requests).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn ended_session_gets_new_persisted_identity_and_same_requests_work() {
    let (first, mut first_server) = socket();
    let (second, mut second_server) = socket();
    let (requests, mut incoming, pump, _, store) = setup(vec![Ok(first), Ok(second)]).await;
    let mut links = pump.observe_link();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(
        pump.with_recovery_policy(policy())
            .run_with_clock(stopped, TestClock(Instant::now())),
    );
    let old = first_server.welcome().await;
    welcomed(&mut incoming).await;
    first_server
        .send(ServerFrame::Closed {
            reason: CloseReason::SessionEnded,
        })
        .await;
    let new = second_server.welcome().await;
    assert_ne!(old, new);
    assert_eq!(store.load().await.unwrap().unwrap().session_id, new);
    let mut restarted = false;
    while let Some(event) = timeout(Duration::from_secs(10), incoming.recv())
        .await
        .unwrap()
    {
        if matches!(event, ClientEvent::Welcome { .. }) {
            break;
        }
    }
    while !restarted {
        restarted = matches!(next_link(&mut links).await, SessionLinkEvent::SessionRestarted { old: a, new: b } if a == old && b == new);
    }
    second_server.answer_request(&requests).await;
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn revoked_upgrade_is_terminal_and_observed_even_with_recovery() {
    let (requests, _incoming, pump, attempts, _) = setup(vec![Err(
        ConnectFailure::CredentialRejected(AuthRefusal::Revoked),
    )])
    .await;
    let mut links = pump.observe_link();
    let (_stop, stopped) = watch::channel(false);
    let result = pump
        .with_recovery_policy(policy())
        .run_with_clock(stopped, TestClock(Instant::now()))
        .await;
    assert!(matches!(
        result,
        Err(ClientError::Connect(ConnectFailure::CredentialRejected(
            AuthRefusal::Revoked
        )))
    ));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(requests.is_closed());
    assert_eq!(
        next_link(&mut links).await,
        SessionLinkEvent::Connecting { attempt: 1 }
    );
    assert_eq!(next_link(&mut links).await, SessionLinkEvent::Revoked);
}

#[tokio::test(start_paused = true)]
async fn welcome_timeout_closes_socket_before_backoff_then_recovers() {
    let (first, mut first_server) = socket();
    let (second, mut second_server) = socket();
    let (requests, mut incoming, pump, attempts, _) = setup(vec![Ok(first), Ok(second)]).await;
    let mut links = pump.observe_link();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(
        pump.with_recovery_policy(policy())
            .run_with_clock(stopped, TestClock(Instant::now())),
    );
    assert!(matches!(
        first_server.receive().await,
        ClientFrame::Hello { .. }
    ));
    assert!(matches!(
        requests.request(OperationRequest::Consumer(())).await,
        Err(CallFailure::NotSent { .. })
    ));
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(
        timeout(Duration::from_secs(1), first_server.closed.recv())
            .await
            .unwrap(),
        Some(CloseReason::ClientClosed)
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    while next_link(&mut links).await != SessionLinkEvent::WelcomeTimeout {}
    tokio::time::advance(Duration::from_secs(1)).await;
    second_server.welcome().await;
    while let Some(event) = timeout(Duration::from_secs(10), incoming.recv())
        .await
        .unwrap()
    {
        if matches!(event, ClientEvent::Welcome { .. }) {
            break;
        }
    }
    second_server.answer_request(&requests).await;
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn slow_link_observer_never_blocks_repeated_recovery() {
    let (connection, mut server) = socket();
    let mut script: Vec<_> = (0..300)
        .map(|_| Err(ConnectFailure::CredentialRejected(AuthRefusal::Unknown)))
        .collect();
    script.push(Ok(connection));
    let (requests, mut incoming, pump, attempts, _) = setup(script).await;
    let mut unread = pump.observe_link();
    let state = pump.link_state();
    let (stop, stopped) = watch::channel(false);
    let clock = TestClock(Instant::now());
    let task = tokio::spawn(
        pump.with_recovery_policy(policy())
            .run_with_clock(stopped, clock.clone()),
    );
    let encoded = timeout(Duration::from_secs(2000), server.received.recv())
        .await
        .unwrap()
        .unwrap();
    let ClientFrame::Hello { session_id, .. } =
        serde_json::from_slice::<ClientFrame>(&encoded).unwrap()
    else {
        panic!("expected Hello")
    };
    server
        .send(ServerFrame::Welcome {
            session_id,
            databases: vec![],
        })
        .await;
    welcomed(&mut incoming).await;
    server.answer_request(&requests).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 301);
    assert_eq!(clock.now().unix_millis(), 1_195_000);
    assert_eq!(
        state.borrow().status,
        crate::SessionLinkStatus::Welcomed { session_id }
    );
    assert_eq!(
        state.borrow().last_drop_reason,
        Some(crate::SessionDropReason::CredentialRejected {
            reason: AuthRefusal::Unknown
        })
    );
    assert!(matches!(
        unread.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_))
    ));
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn credential_terminal_policy_also_stops_an_attached_expiry() {
    let (connection, mut server) = socket();
    let (requests, mut incoming, pump, attempts, _) = setup(vec![Ok(connection)]).await;
    let mut links = pump.observe_link();
    let configured = SessionRecoveryPolicy {
        credential_rejected: CredentialRejectedRecovery::Terminal,
        ..policy()
    };
    let (_stop, stopped) = watch::channel(false);
    let task = tokio::spawn(
        pump.with_recovery_policy(configured)
            .run_with_clock(stopped, TestClock(Instant::now())),
    );
    server.welcome().await;
    welcomed(&mut incoming).await;
    server
        .send(ServerFrame::Closed {
            reason: CloseReason::CredentialRejected(AuthRefusal::Expired),
        })
        .await;
    let result = timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(ClientError::SessionEnded)));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(requests.is_closed());
    let mut refusal_seen = false;
    while let Ok(event) = links.try_recv() {
        refusal_seen |= matches!(
            event,
            SessionLinkEvent::CredentialRejected {
                reason: AuthRefusal::Expired
            }
        );
    }
    assert!(refusal_seen);
}

#[tokio::test(start_paused = true)]
async fn constructor_without_policy_keeps_upgrade_refusal_terminal_and_reports_it() {
    let (requests, _incoming, pump, attempts, _) = setup(vec![Err(
        ConnectFailure::CredentialRejected(AuthRefusal::Expired),
    )])
    .await;
    let mut links = pump.observe_link();
    let (_stop, stopped) = watch::channel(false);
    let result = pump
        .run_with_clock(stopped, TestClock(Instant::now()))
        .await;
    assert!(matches!(
        result,
        Err(ClientError::Connect(ConnectFailure::CredentialRejected(
            AuthRefusal::Expired
        )))
    ));
    assert!(requests.is_closed());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        next_link(&mut links).await,
        SessionLinkEvent::Connecting { attempt: 1 }
    );
    assert_eq!(
        next_link(&mut links).await,
        SessionLinkEvent::CredentialRejected {
            reason: AuthRefusal::Expired
        }
    );
}

#[tokio::test(start_paused = true)]
async fn session_ended_can_remain_terminal_under_explicit_policy() {
    let (connection, mut server) = socket();
    let (requests, mut incoming, pump, attempts, store) = setup(vec![Ok(connection)]).await;
    let configured = SessionRecoveryPolicy {
        session_ended: SessionEndedRecovery::Terminal,
        ..policy()
    };
    let (_stop, stopped) = watch::channel(false);
    let task = tokio::spawn(
        pump.with_recovery_policy(configured)
            .run_with_clock(stopped, TestClock(Instant::now())),
    );
    let old = server.welcome().await;
    welcomed(&mut incoming).await;
    server
        .send(ServerFrame::Closed {
            reason: CloseReason::SessionEnded,
        })
        .await;
    assert!(matches!(
        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap(),
        Err(ClientError::SessionEnded)
    ));
    assert!(requests.is_closed());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(store.load().await.unwrap().unwrap().session_id, old);
}

#[tokio::test(start_paused = true)]
async fn attached_revocation_loses_pending_answer_without_replay_or_identity_replacement() {
    let (connection, mut server) = socket();
    let (requests, mut incoming, pump, attempts, store) = setup(vec![Ok(connection)]).await;
    let mut links = pump.observe_link();
    let (_stop, stopped) = watch::channel(false);
    let task = tokio::spawn(
        pump.with_recovery_policy(policy())
            .run_with_clock(stopped, TestClock(Instant::now())),
    );
    let old = server.welcome().await;
    welcomed(&mut incoming).await;
    let request = requests.request(OperationRequest::Consumer(()));
    tokio::pin!(request);
    tokio::select! {
        biased;
        _ = &mut request => panic!("request returned before revocation"),
        frame = server.receive() => assert!(matches!(frame, ClientFrame::Request { .. })),
    }
    server
        .send(ServerFrame::Closed {
            reason: CloseReason::CredentialRejected(AuthRefusal::Revoked),
        })
        .await;
    assert!(matches!(request.await, Err(CallFailure::AnswerLost)));
    assert!(matches!(
        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap(),
        Err(ClientError::SessionEnded)
    ));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(store.load().await.unwrap().unwrap().session_id, old);
    assert!(requests.is_closed());
    let mut revoked_seen = false;
    while let Ok(event) = links.try_recv() {
        revoked_seen |= event == SessionLinkEvent::Revoked;
    }
    assert!(revoked_seen);
}
