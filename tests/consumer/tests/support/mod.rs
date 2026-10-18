use fleet_sync_protocol::*;
#[cfg(feature = "test-support")]
pub mod delivery_mail;
mod key_contract;
#[allow(
    dead_code,
    reason = "shared fixture used only by lifecycle and target-capability journeys"
)]
pub mod lifecycle_import;
#[allow(dead_code)]
pub mod writer_process;
use fleet_sync_session::*;
use fleet_sync_test_consumer::{ControlAnswer, ControlRequest, SampleProtocol, TestResult};
use fleet_sync_transport_websocket::{WebSocketConnection, WebSocketTransport};
pub use key_contract::assert_key_refusals;
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader, Lines},
    process::{Child, ChildStdout, Command},
    time::timeout,
};

pub struct ServiceProcess {
    pub child: Child,
    pub port: u16,
    _stdout: Lines<BufReader<ChildStdout>>,
}
impl ServiceProcess {
    pub async fn start(path: &Path) -> TestResult<Self> {
        Self::start_as(path, 3).await
    }
    pub async fn start_as(path: &Path, receiver: u128) -> TestResult<Self> {
        Self::start_with_lease(path, receiver, 600_000).await
    }
    pub async fn start_with_lease(path: &Path, receiver: u128, lease_ms: u64) -> TestResult<Self> {
        Self::start_with_policy(path, receiver, lease_ms, 60_000).await
    }
    pub async fn start_with_policy(
        path: &Path,
        receiver: u128,
        lease_ms: u64,
        move_renewal_ms: u64,
    ) -> TestResult<Self> {
        Self::start_with_initialization_lock(path, receiver, lease_ms, move_renewal_ms, None).await
    }
    #[cfg(feature = "test-support")]
    #[allow(
        dead_code,
        reason = "startup fault is used by its dedicated native journey"
    )]
    pub async fn start_with_mailbox_init_lock(path: &Path, mailbox: PeerId) -> TestResult<Self> {
        Self::start_with_initialization_lock(path, 3, 600_000, 60_000, Some(mailbox)).await
    }
    async fn start_with_initialization_lock(
        path: &Path,
        receiver: u128,
        lease_ms: u64,
        move_renewal_ms: u64,
        mailbox: Option<PeerId>,
    ) -> TestResult<Self> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fleet-sync-test-consumer"));
        if let Some(mailbox) = mailbox {
            command.env(
                "FLEET_SYNC_TEST_LOCK_MAILBOX_INIT",
                mailbox.as_uuid().to_string(),
            );
        }
        let mut child = command
            .arg(path)
            .arg("0")
            .arg(receiver.to_string())
            .arg(lease_ms.to_string())
            .arg(move_renewal_ms.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let mut lines = BufReader::new(child.stdout.take().ok_or("no consumer stdout")?).lines();
        let (pid, port) = timeout(Duration::from_secs(20), async {
            while let Some(line) = lines.next_line().await? {
                if let Some(ready) = line.strip_prefix("READY ") {
                    let mut fields = ready.split_whitespace();
                    return Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                        fields.next().ok_or("no PID")?.parse::<u32>()?,
                        fields.next().ok_or("no port")?.parse::<u16>()?,
                    ));
                }
            }
            Err("consumer exited before READY".into())
        })
        .await??;
        assert_eq!(child.id(), Some(pid));
        assert_ne!(pid, std::process::id());
        Ok(Self {
            child,
            port,
            _stdout: lines,
        })
    }
    pub async fn connect(&self, path: &str, credential: &str) -> TestResult<WebSocketConnection> {
        Ok(WebSocketTransport::default()
            .connect(
                &Endpoint::new(&format!("ws://127.0.0.1:{}/{path}", self.port))?,
                &PeerCredential::new(credential)?,
            )
            .await?)
    }
    pub async fn peer(&self, peer: u128, session: SessionId) -> TestResult<WebSocketConnection> {
        self.peer_with_statuses(peer, session)
            .await
            .map(|(socket, _)| socket)
    }
    pub async fn peer_with_statuses(
        &self,
        peer: u128,
        session: SessionId,
    ) -> TestResult<(WebSocketConnection, Vec<DatabaseStatus>)> {
        let mut socket = self
            .connect("session", &format!("Bearer peer-{peer}"))
            .await?;
        let hello: ClientFrame<SampleProtocol> = ClientFrame::Hello {
            session_id: session,
            schema_support: SchemaSupport { max_version: 100 },
        };
        socket.send(serde_json::to_vec(&hello)?.into()).await?;
        match server_frame(&mut socket).await? {
            ServerFrame::Welcome {
                session_id,
                databases,
            } if session_id == session => Ok((socket, databases)),
            _ => Err("no matching Welcome".into()),
        }
    }
    pub async fn kill(&mut self) -> TestResult {
        self.child.kill().await?;
        self.child.wait().await?;
        Ok(())
    }
    pub async fn stop(mut self, control: &mut WebSocketConnection) -> TestResult {
        control_request(control, ControlRequest::Shutdown).await?;
        if !timeout(Duration::from_secs(15), self.child.wait())
            .await??
            .success()
        {
            return Err("consumer shutdown failed".into());
        }
        Ok(())
    }
}
pub async fn server_frame(
    socket: &mut WebSocketConnection,
) -> TestResult<ServerFrame<SampleProtocol>> {
    let bytes = timeout(Duration::from_secs(15), socket.receive())
        .await?
        .ok_or("peer closed before answer")??;
    Ok(serde_json::from_slice(&bytes)?)
}
pub async fn request(
    socket: &mut WebSocketConnection,
    body: OperationRequest<SampleProtocol>,
) -> TestResult<OperationAnswer<SampleProtocol>> {
    let frame = FrameId::new(durable_steps::Uuid::now_v7())?;
    socket
        .send(serde_json::to_vec(&ClientFrame::<SampleProtocol>::Request { frame, body })?.into())
        .await?;
    loop {
        match server_frame(socket).await? {
            ServerFrame::Answer {
                frame: answered,
                body,
            } if answered == frame => return Ok(body),
            ServerFrame::Notice { .. } => {}
            ServerFrame::Closed { reason } => {
                return Err(format!("peer closed while awaiting answer: {reason:?}").into());
            }
            _ => return Err("unexpected peer frame while awaiting answer".into()),
        }
    }
}
pub async fn control_request(
    socket: &mut WebSocketConnection,
    request: ControlRequest,
) -> TestResult<ControlAnswer> {
    let answer_timeout = if matches!(
        request,
        ControlRequest::WaitInstall { .. }
            | ControlRequest::WaitSourceUnattached { .. }
            | ControlRequest::WaitHolderChange(_)
            | ControlRequest::WaitClaimSettlement { .. }
    ) {
        Duration::from_secs(65)
    } else if matches!(request, ControlRequest::WaitAttempts { .. }) {
        Duration::from_secs(50)
    } else {
        Duration::from_secs(20)
    };
    socket.send(serde_json::to_vec(&request)?.into()).await?;
    let bytes = timeout(answer_timeout, socket.receive())
        .await?
        .ok_or("control closed")??;
    let answer = serde_json::from_slice(&bytes)?;
    if let ControlAnswer::Error { message } = answer {
        return Err(message.into());
    }
    Ok(answer)
}
pub async fn effects(
    socket: &mut WebSocketConnection,
    count: usize,
) -> TestResult<Vec<fleet_sync_test_consumer::EffectReceipt>> {
    match control_request(socket, ControlRequest::WaitEffects { count }).await? {
        ControlAnswer::Effects(effects) => Ok(effects),
        _ => Err("no effect receipts".into()),
    }
}
pub async fn copy_rows(
    socket: &mut WebSocketConnection,
    database: DatabaseId,
) -> TestResult<std::collections::BTreeMap<String, Vec<Vec<SqlValue>>>> {
    let mut rows = std::collections::BTreeMap::new();
    let mut cursor = None;
    loop {
        let page = match request(
            socket,
            OperationRequest::Replication(ReplicationRequest::SnapshotPage { database, cursor }),
        )
        .await?
        {
            OperationAnswer::Replication(ReplicationAnswer::SnapshotPage(SnapshotRead::Page(
                page,
            ))) => page,
            _ => return Err("copy snapshot did not return rows".into()),
        };
        rows.entry(page.table)
            .or_insert_with(Vec::new)
            .extend(page.rows);
        cursor = page.next;
        if cursor.is_none() {
            return Ok(rows);
        }
    }
}
