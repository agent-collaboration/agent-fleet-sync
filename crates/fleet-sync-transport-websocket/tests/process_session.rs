#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic_in_result_fn)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_session::*;
use fleet_sync_transport_websocket::{WebSocketConnection, WebSocketTransport};
use runtime_primitives::{Backoff, Clock, IdSource, WakeFailure, WakeScheduler};
use std::{
    path::Path,
    process::Stdio,
    sync::{
        Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};
use support::{session_fixture_contract::*, session_fixture_store::TursoClientStore};
use tokio::{
    io::{AsyncBufReadExt, BufReader, Lines},
    process::{Child, ChildStdout, Command},
    time::timeout,
};
use uuid::Uuid;

type Client = SessionClient<WebSocketTransport, TursoClientStore, FixtureProtocol>;
#[derive(Default)]
struct ClientHost {
    now: AtomicI64,
    wakes: Mutex<Vec<SyncTime>>,
}
impl Clock for ClientHost {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(self.now.load(Ordering::SeqCst))
    }
}
impl IdSource for ClientHost {
    fn new_id(&self) -> Uuid {
        Uuid::now_v7()
    }
}
impl WakeScheduler for ClientHost {
    async fn wake_at(&self, at: SyncTime) -> Result<(), WakeFailure> {
        self.wakes.lock().unwrap().push(at);
        Ok(())
    }
}

struct FixtureProcess {
    child: Child,
    port: u16,
    _stdout: Lines<BufReader<ChildStdout>>,
}
impl FixtureProcess {
    async fn start(path: &Path) -> TestResult<Self> {
        let mut child = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "session_host_process",
                "--ignored",
                "--nocapture",
            ])
            .env("FLEET_SYNC_FIXTURE_DATABASE", path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        assert_ne!(child.id(), Some(std::process::id()));
        let stdout = child.stdout.take().ok_or("fixture stdout missing")?;
        let mut lines = BufReader::new(stdout).lines();
        let (pid, port) = timeout(Duration::from_secs(15), async {
            while let Some(line) = lines.next_line().await? {
                if let Some(ready) = line.strip_prefix("READY ") {
                    let mut fields = ready.split_whitespace();
                    let pid = fields.next().ok_or("fixture pid missing")?.parse::<u32>()?;
                    let port = fields
                        .next()
                        .ok_or("fixture port missing")?
                        .parse::<u16>()?;
                    return Ok::<_, Box<dyn std::error::Error + Send + Sync>>((pid, port));
                }
            }
            Err("fixture exited before its READY signal".into())
        })
        .await??;
        assert_eq!(child.id(), Some(pid));
        Ok(Self {
            child,
            port,
            _stdout: lines,
        })
    }
    fn endpoint(&self, path: &str) -> Endpoint {
        Endpoint::new(&format!("ws://127.0.0.1:{}/{path}", self.port)).unwrap()
    }
}
async fn connect(
    process: &FixtureProcess,
    path: &str,
    credential: &str,
) -> TestResult<WebSocketConnection> {
    Ok(WebSocketTransport::default()
        .connect(&process.endpoint(path), &PeerCredential::new(credential)?)
        .await?)
}
async fn control(
    socket: &mut WebSocketConnection,
    request: ControlRequest,
) -> TestResult<FixtureSnapshot> {
    socket.send(serde_json::to_vec(&request)?.into()).await?;
    let bytes = timeout(Duration::from_secs(5), socket.receive())
        .await?
        .ok_or("control connection closed")??;
    Ok(serde_json::from_slice(&bytes)?)
}
async fn hello(
    socket: &mut WebSocketConnection,
    id: SessionId,
) -> TestResult<ServerFrame<FixtureProtocol>> {
    let hello: ClientFrame<FixtureProtocol> = ClientFrame::Hello {
        session_id: id,
        schema_support: SchemaSupport { max_version: 1 },
    };
    socket.send(serde_json::to_vec(&hello)?.into()).await?;
    server_frame(socket).await
}
async fn server_frame(
    socket: &mut WebSocketConnection,
) -> TestResult<ServerFrame<FixtureProtocol>> {
    let bytes = timeout(Duration::from_secs(5), socket.receive())
        .await?
        .ok_or("session connection closed")??;
    Ok(serde_json::from_slice(&bytes)?)
}
async fn client_event(
    client: &mut Client,
    host: &ClientHost,
) -> TestResult<ClientEvent<FixtureProtocol>> {
    Ok(timeout(Duration::from_secs(5), client.receive(host)).await??)
}
fn assert_status(databases: &[DatabaseStatus], sequence: u64, expires_at: i64) {
    assert!(
        matches!(databases, [DatabaseStatus::Ready { acked, lease, .. }] if acked.sequence.value() == sequence && lease.expires_at.unix_millis() == expires_at)
    );
}
#[allow(clippy::panic)]
fn assert_welcome(
    frame: ServerFrame<FixtureProtocol>,
    id: SessionId,
    sequence: u64,
    expires_at: i64,
) {
    match frame {
        ServerFrame::Welcome {
            session_id,
            databases,
        } => {
            assert_eq!(session_id, id);
            assert_status(&databases, sequence, expires_at);
        }
        _ => panic!("expected Welcome"),
    }
}
async fn increment(
    client: &mut Client,
    host: &ClientHost,
    operation: RequestId,
    amount: u64,
    expected: IncrementAnswer,
) -> TestResult {
    let frame = FrameId::new(Uuid::now_v7())?;
    client
        .send_request(
            frame,
            OperationRequest::Consumer(Increment { operation, amount }),
            host,
        )
        .await?;
    match client_event(client, host).await? {
        ClientEvent::Answer {
            frame: answer_frame,
            body: OperationAnswer::Consumer(answer),
        } => {
            assert_eq!(answer_frame, frame);
            assert_eq!(answer, expected);
        }
        _ => return Err("increment did not produce its matching answer".into()),
    }
    Ok(())
}

#[tokio::test]
async fn localhost_session_journey_uses_two_processes_and_real_websockets() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let server_path = temporary.path().join("server.db");
    let client_path = temporary.path().join("client.db");
    let mut process = FixtureProcess::start(&server_path).await?;
    let mut admin = connect(&process, "control", "Bearer control").await?;
    let host = ClientHost::default();
    let store = TursoClientStore::open_and_migrate(&client_path).await?;
    let mut client = Client::load(
        WebSocketTransport::default(),
        store.clone(),
        &host,
        SchemaSupport { max_version: 1 },
        Backoff {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(100),
            factor: 2,
        },
    )
    .await?;
    let original_id = client.state().session_id;
    client
        .connect(
            &process.endpoint("session"),
            &PeerCredential::new("Bearer fixture")?,
            &host,
        )
        .await?;
    match client_event(&mut client, &host).await? {
        ClientEvent::Welcome { databases } => assert_status(&databases, 0, 1_000),
        _ => return Err("initial Welcome missing".into()),
    }

    control(&mut admin, ControlRequest::DropNextAnswer).await?;
    let operation = RequestId::new(Uuid::now_v7())?;
    let frame = FrameId::new(Uuid::now_v7())?;
    client
        .send_request(
            frame,
            OperationRequest::Consumer(Increment {
                operation,
                amount: 7,
            }),
            &host,
        )
        .await?;
    assert!(
        matches!(client_event(&mut client, &host).await?, ClientEvent::Unknown { frames } if frames == vec![frame])
    );
    assert!(matches!(
        client_event(&mut client, &host).await?,
        ClientEvent::Disconnected { .. }
    ));
    let saved = control(&mut admin, ControlRequest::Snapshot).await?;
    assert_eq!(
        (saved.value, saved.receipt_count, saved.sessions.len()),
        (7, 1, 1)
    );
    host.now.store(10, Ordering::SeqCst);
    client
        .connect(
            &process.endpoint("session"),
            &PeerCredential::new("Bearer fixture")?,
            &host,
        )
        .await?;
    match client_event(&mut client, &host).await? {
        ClientEvent::Welcome { databases } => assert_status(&databases, 7, 1_000),
        _ => return Err("resume Welcome missing".into()),
    }
    increment(
        &mut client,
        &host,
        operation,
        7,
        IncrementAnswer::Applied { value: 7 },
    )
    .await?;
    increment(&mut client, &host, operation, 8, IncrementAnswer::Conflict).await?;
    let saved = control(&mut admin, ControlRequest::Snapshot).await?;
    assert_eq!((saved.value, saved.receipt_count), (7, 1));
    println!(
        "OBS lost committed answer -> Unknown -> resend: one receipt and one effect; conflict changes nothing"
    );

    control(&mut admin, ControlRequest::HoldNextClose).await?;
    let mut replacement = connect(&process, "session", "Bearer fixture").await?;
    assert_welcome(
        hello(&mut replacement, original_id).await?,
        original_id,
        7,
        1_000,
    );
    assert!(matches!(
        client_event(&mut client, &host).await?,
        ClientEvent::Disconnected {
            reason: Some(CloseReason::ConnectionReplaced),
            ..
        }
    ));
    let before_close = control(&mut admin, ControlRequest::Snapshot).await?;
    let current = before_close
        .active_connection(original_id)
        .ok_or("replacement not active")?;
    let after_close = control(&mut admin, ControlRequest::ReleaseHeldClose).await?;
    assert_eq!(after_close.active_connection(original_id), Some(current));
    assert_eq!(after_close.sessions.len(), 1);
    control(
        &mut admin,
        ControlRequest::SetLease {
            expires_at: SyncTime::from_unix_millis(2_000),
        },
    )
    .await?;
    let raw_frame = FrameId::new(Uuid::now_v7())?;
    let request: ClientFrame<FixtureProtocol> = ClientFrame::Request {
        frame: raw_frame,
        body: OperationRequest::Consumer(Increment {
            operation: RequestId::new(Uuid::now_v7())?,
            amount: 1,
        }),
    };
    replacement
        .send(serde_json::to_vec(&request)?.into())
        .await?;
    assert!(matches!(
        server_frame(&mut replacement).await?,
        ServerFrame::Answer {
            body: OperationAnswer::Consumer(IncrementAnswer::Applied { value: 8 }),
            ..
        }
    ));
    assert_welcome(
        hello(&mut replacement, original_id).await?,
        original_id,
        8,
        2_000,
    );
    let repeated = control(&mut admin, ControlRequest::Snapshot).await?;
    assert_eq!(repeated.active_connection(original_id), Some(current));
    assert_eq!(repeated.sessions.len(), 1);
    println!(
        "OBS replacement closes old socket; delayed old close is ignored; repeat Hello preserves attachment and reads new position and lease"
    );

    replacement.send(Bytes::from_static(b"not JSON")).await?;
    assert!(matches!(
        server_frame(&mut replacement).await?,
        ServerFrame::Closed {
            reason: CloseReason::MalformedFrame
        }
    ));
    let malformed = control(&mut admin, ControlRequest::Snapshot).await?;
    assert!(matches!(
        malformed.sessions.first().unwrap().state,
        SessionState::Detached { .. }
    ));
    let mut short = connect(&process, "session", "Bearer short").await?;
    assert_welcome(hello(&mut short, original_id).await?, original_id, 8, 2_000);
    control(
        &mut admin,
        ControlRequest::AdvanceClock {
            now: SyncTime::from_unix_millis(50),
        },
    )
    .await?;
    assert!(matches!(
        server_frame(&mut short).await?,
        ServerFrame::Closed {
            reason: CloseReason::CredentialRejected(AuthRefusal::Expired)
        }
    ));
    let mut renewed = connect(&process, "session", "Bearer fixture").await?;
    assert_welcome(
        hello(&mut renewed, original_id).await?,
        original_id,
        8,
        2_000,
    );
    println!(
        "OBS malformed input and idle credential expiry detach; fresh credentials resume the same logical session"
    );

    control(&mut admin, ControlRequest::Revoke).await?;
    assert!(matches!(
        server_frame(&mut renewed).await?,
        ServerFrame::Closed {
            reason: CloseReason::CredentialRejected(AuthRefusal::Revoked)
        }
    ));
    assert!(
        connect(&process, "session", "Bearer fixture")
            .await
            .is_err()
    );
    control(&mut admin, ControlRequest::Allow).await?;
    let mut ended = connect(&process, "session", "Bearer fixture").await?;
    assert!(matches!(
        hello(&mut ended, original_id).await?,
        ServerFrame::Closed {
            reason: CloseReason::SessionEnded
        }
    ));
    client.start_new_session(&host).await?;
    let new_id = client.state().session_id;
    assert_ne!(new_id, original_id);
    client
        .connect(
            &process.endpoint("session"),
            &PeerCredential::new("Bearer fixture")?,
            &host,
        )
        .await?;
    match client_event(&mut client, &host).await? {
        ClientEvent::Welcome { databases } => assert_status(&databases, 8, 2_000),
        _ => return Err("new session Welcome missing".into()),
    }
    let final_operation = RequestId::new(Uuid::now_v7())?;
    increment(
        &mut client,
        &host,
        final_operation,
        1,
        IncrementAnswer::Applied { value: 9 },
    )
    .await?;
    println!(
        "OBS revocation ends the session, rejects new connections and leaves the lease unchanged; allowed peer needs a new session ID"
    );

    process.child.kill().await?;
    drop(admin);
    drop(client);
    drop(store);
    process = FixtureProcess::start(&server_path).await?;
    admin = connect(&process, "control", "Bearer control").await?;
    let reopened = TursoClientStore::open_and_migrate(&client_path).await?;
    client = Client::load(
        WebSocketTransport::default(),
        reopened,
        &host,
        SchemaSupport { max_version: 1 },
        Backoff {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(100),
            factor: 2,
        },
    )
    .await?;
    assert_eq!(client.state().session_id, new_id);
    client
        .connect(
            &process.endpoint("session"),
            &PeerCredential::new("Bearer fixture")?,
            &host,
        )
        .await?;
    match client_event(&mut client, &host).await? {
        ClientEvent::Welcome { databases } => assert_status(&databases, 9, 2_000),
        _ => return Err("restart Welcome missing".into()),
    }
    increment(
        &mut client,
        &host,
        final_operation,
        1,
        IncrementAnswer::Applied { value: 9 },
    )
    .await?;
    let restarted = control(&mut admin, ControlRequest::Snapshot).await?;
    assert_eq!(
        (
            restarted.value,
            restarted.receipt_count,
            restarted.sessions.len()
        ),
        (9, 3, 2)
    );
    assert_eq!(
        restarted
            .sessions
            .iter()
            .filter(|record| matches!(record.state, SessionState::Active { .. }))
            .count(),
        1
    );
    println!(
        "OBS server kill and client-store reopen preserve session ID and operation receipts; replay after restart has no extra effect"
    );
    client.close().await;
    control(&mut admin, ControlRequest::Stop).await?;
    assert!(
        timeout(Duration::from_secs(10), process.child.wait())
            .await??
            .success()
    );
    Ok(())
}

/// The parent journey runs this ignored helper in another process; it is not a skipped journey.
#[tokio::test]
#[ignore = "spawned by the two-process journey"]
async fn session_host_process() -> TestResult {
    let path = std::env::var_os("FLEET_SYNC_FIXTURE_DATABASE")
        .ok_or("run this helper through the process journey")?;
    support::session_fixture_server::run_server(std::path::Path::new(&path)).await
}
