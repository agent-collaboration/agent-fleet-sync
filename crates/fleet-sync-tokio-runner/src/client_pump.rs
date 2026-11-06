use crate::{
    CredentialRejectedRecovery, SessionDropReason, SessionEndedRecovery, SessionLinkEvent,
    SessionLinkEventKind, SessionLinkState, SessionLinkStatus, SessionRecoveryPolicy,
};
use durable_steps::{CallFailure, NotSentReason, Uuid};
use fleet_sync_protocol::{
    AuthRefusal, ClientAnswer, CloseReason, ConsumerProtocol, FrameId, NoticePosition,
    OperationAnswer, OperationRequest, SessionId,
};
use fleet_sync_session::{
    ClientError, ClientEvent, ConnectFailure, ConnectionClosed, Endpoint, PeerCredential,
    SessionClient, SessionClientStore, SessionTransport,
};
use runtime_primitives::{Clock, SyncTime, SystemClock, TokioWakeTimer, UuidV7Ids, WakeScheduler};
use std::{collections::BTreeMap, time::Duration};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

enum CredentialRefusalOrigin {
    Upgrade,
    SessionFrame,
}

enum Command<P: ConsumerProtocol> {
    Request {
        body: OperationRequest<P>,
        answer: oneshot::Sender<Result<OperationAnswer<P>, CallFailure>>,
    },
    Answer {
        frame: FrameId,
        body: ClientAnswer<P>,
        done: oneshot::Sender<Result<(), ClientError>>,
    },
    Acknowledge {
        session_id: SessionId,
        position: NoticePosition,
        done: oneshot::Sender<Result<(), ClientError>>,
    },
}
pub struct SessionRequests<P: ConsumerProtocol> {
    sender: mpsc::Sender<Command<P>>,
    timeout: Duration,
}
impl<P: ConsumerProtocol> Clone for SessionRequests<P> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            timeout: self.timeout,
        }
    }
}
impl<P: ConsumerProtocol> SessionRequests<P> {
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
    /// A readiness or connection refusal returned before dispatch is
    /// `CallFailure::NotSent`. A local reply timeout or uncertain dispatch is
    /// `AnswerLost`, including other failures before a send can be confirmed.
    pub async fn request(
        &self,
        body: OperationRequest<P>,
    ) -> Result<OperationAnswer<P>, CallFailure> {
        let (answer, result) = oneshot::channel();
        self.sender
            .try_send(Command::Request { body, answer })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => CallFailure::NotSent {
                    reason: NotSentReason::Overloaded,
                },
                mpsc::error::TrySendError::Closed(_) => unavailable(),
            })?;
        tokio::time::timeout(self.timeout, result)
            .await
            .map_err(|_| CallFailure::AnswerLost)?
            .map_err(|_| CallFailure::AnswerLost)?
    }
    pub async fn answer(&self, frame: FrameId, body: ClientAnswer<P>) -> Result<(), ClientError> {
        let (done, result) = oneshot::channel();
        self.sender
            .send(Command::Answer { frame, body, done })
            .await
            .map_err(|_| ClientError::NotConnected)?;
        tokio::time::timeout(self.timeout, result)
            .await
            .map_err(|_| ClientError::NotConnected)?
            .map_err(|_| ClientError::NotConnected)?
    }
    pub async fn acknowledge_notice(
        &self,
        session_id: SessionId,
        position: NoticePosition,
    ) -> Result<(), ClientError> {
        let (done, result) = oneshot::channel();
        self.sender
            .send(Command::Acknowledge {
                session_id,
                position,
                done,
            })
            .await
            .map_err(|_| ClientError::NotConnected)?;
        tokio::time::timeout(self.timeout, result)
            .await
            .map_err(|_| ClientError::NotConnected)?
            .map_err(|_| ClientError::NotConnected)?
    }
}

/// One owner pumps the consumer's existing client. The caller handles forwarded
/// Welcome, server requests and notices, and acknowledges notices after handling.
/// Correlated request answers go only to their waiting caller.
/// Transport activation sends Hello; matching Welcome is the request readiness
/// point. Readiness and connection refusals before dispatch are NotSent;
/// timeouts and uncertain dispatch stay AnswerLost. Server calls can be answered
/// during attachment.
pub struct SessionPump<T: SessionTransport, S: SessionClientStore, P: ConsumerProtocol> {
    client: SessionClient<T, S, P>,
    endpoint: Endpoint,
    credential: PeerCredential,
    commands: mpsc::Receiver<Command<P>>,
    events: mpsc::Sender<ClientEvent<P>>,
    recovery: Option<SessionRecoveryPolicy>,
    connection: u64,
    link_events: broadcast::Sender<SessionLinkEvent>,
    link_state: watch::Sender<SessionLinkState>,
}
impl<T: SessionTransport, S: SessionClientStore, P: ConsumerProtocol> SessionPump<T, S, P> {
    pub fn new(
        client: SessionClient<T, S, P>,
        endpoint: Endpoint,
        credential: PeerCredential,
        timeout: Duration,
    ) -> (SessionRequests<P>, mpsc::Receiver<ClientEvent<P>>, Self) {
        let (sender, commands) = mpsc::channel(128);
        let (events, received) = mpsc::channel(128);
        (
            SessionRequests { sender, timeout },
            received,
            Self {
                client,
                endpoint,
                credential,
                commands,
                events,
                recovery: None,
                connection: 0,
                link_events: broadcast::channel(128).0,
                link_state: watch::channel(SessionLinkState {
                    since: SystemClock.now(),
                    ..SessionLinkState::default()
                })
                .0,
            },
        )
    }
    pub fn with_recovery_policy(mut self, policy: SessionRecoveryPolicy) -> Self {
        self.recovery = Some(policy);
        self
    }
    /// Subscribe before running. A slow receiver gets `RecvError::Lagged`;
    /// the pump never waits for it. Existing application events remain separate.
    pub fn observe_link(&self) -> broadcast::Receiver<SessionLinkEvent> {
        self.link_events.subscribe()
    }
    /// Current status and the last drop reason, retained across reconnection.
    pub fn link_state(&self) -> watch::Receiver<SessionLinkState> {
        self.link_state.subscribe()
    }
    pub async fn run(self, shutdown: watch::Receiver<bool>) -> Result<(), ClientError> {
        self.run_with_clock(shutdown, SystemClock).await
    }
    /// Supply the same time source used by the host's deterministic timer tests.
    pub async fn run_with_clock<C: Clock + Clone>(
        mut self,
        mut shutdown: watch::Receiver<bool>,
        clock: C,
    ) -> Result<(), ClientError> {
        let host = PumpClock::new(clock);
        let mut pending = BTreeMap::new();
        let mut connected = false;
        let mut welcome_deadline = None;
        let mut contact_flush_at = None;
        let mut attempt = 0_u32;
        // Fallible recovery stays inside the cleanup boundary.
        let result = async {
            'pumping: loop {
                if *shutdown.borrow() { break Ok(()); }
                if !connected {
                    if let Some(at) = self.client.reconnect_at() && host.now() < at {
                        host.wake_at(at).await?;
                        tokio::select! {
                            _ = host.timer.wait() => {},
                            command = self.commands.recv() => match command {
                                Some(command) => refuse_command(command),
                                None => break Ok(()),
                            },
                            _ = shutdown.changed() => break Ok(()),
                        }
                        continue;
                    }
                    attempt = attempt.saturating_add(1);
                    self.connection = self.connection.checked_add(1).ok_or(ClientError::Protocol)?;
                    self.report_link(SessionLinkEventKind::Connecting { attempt }, &host);
                    let connected_result = {
                        let connecting = self.client.connect(&self.endpoint, &self.credential, &host);
                        tokio::pin!(connecting);
                        loop {
                            tokio::select! {
                                result = &mut connecting => break result,
                                command = self.commands.recv() => match command {
                                    Some(command) => refuse_command(command),
                                    None => break 'pumping Ok(()),
                                },
                                _ = shutdown.changed() => break 'pumping Ok(()),
                            }
                        }
                    };
                    match connected_result {
                        Ok(()) => {
                            connected = true;
                            welcome_deadline = self.recovery.and_then(|policy| policy.welcome_timeout).map(|delay| host.now().after(delay));
                            contact_flush_at = Some(host.now().after(Duration::from_secs(10)));
                            for at in [welcome_deadline, contact_flush_at].into_iter().flatten() { host.wake_at(at).await?; }
                        }
                        Err(ClientError::Connect(ConnectFailure::CredentialRejected(reason))) => {
                            self.recover_credential_refusal(reason, CredentialRefusalOrigin::Upgrade, &host).await?;
                        }
                        Err(ClientError::Connection(error)) if self.client.reconnect_at().is_some() => {
                            self.report_link(SessionLinkEventKind::Disconnected { reason: transport_drop(None, Some(&error)) }, &host);
                        }
                        Err(ClientError::RetryAt { .. } | ClientError::Connect(_)) if self.client.reconnect_at().is_some() => {
                            self.report_link(SessionLinkEventKind::Disconnected { reason: transport_drop(None, None) }, &host);
                        }
                        Err(error) => {
                            self.report_link(SessionLinkEventKind::Disconnected { reason: transport_drop(None, None) }, &host);
                            break Err(error);
                        }
                    }
                    if !connected { continue; }
                }
                tokio::select! {
                    command = self.commands.recv() => match command {
                        Some(Command::Request { body, answer }) => {
                            if answer.is_closed() { continue; }
                            let frame = FrameId::new(Uuid::now_v7()).map_err(|_| ClientError::Protocol)?;
                            match self.client.send_request(frame, body, &host).await {
                                Ok(()) => { pending.insert(frame, answer); }
                                Err(ClientError::NotConnected) => { let _ = answer.send(Err(unavailable())); }
                                Err(_) => {
                                    let _ = answer.send(Err(CallFailure::AnswerLost));
                                    lose_pending(&mut pending);
                                    // Drain the client's queued disconnect before assigning
                                    // a new connection identity.
                                }
                            }
                        }
                        Some(Command::Answer { frame, body, done }) => { let _ = done.send(self.client.send_answer(frame, body, &host).await); }
                        Some(Command::Acknowledge { session_id, position, done }) => { let _ = done.send(self.client.acknowledge_notice(session_id, position).await); }
                        None => break Ok(()),
                    },
                    event = self.client.receive(&host) => match event {
                        Ok(event @ ClientEvent::Welcome { .. }) => {
                            welcome_deadline = None; attempt = 0;
                            self.report_link(SessionLinkEventKind::Welcomed { session_id: self.client.state().session_id }, &host);
                            if !forward(&self.events, event, &mut shutdown).await { break Ok(()); }
                        }
                        Ok(ClientEvent::Answer { frame, body }) => {
                            if let Some(answer) = pending.remove(&frame) { let _ = answer.send(Ok(body)); }
                            else if !forward(&self.events, ClientEvent::Answer { frame, body }, &mut shutdown).await { break Ok(()); }
                        }
                        Ok(ClientEvent::Unknown { frames }) => {
                            for frame in &frames { if let Some(answer) = pending.remove(frame) { let _ = answer.send(Err(CallFailure::AnswerLost)); } }
                            if !forward(&self.events, ClientEvent::Unknown { frames }, &mut shutdown).await { break Ok(()); }
                        }
                        Ok(ClientEvent::Disconnected { reason, .. }) => {
                            let was_welcomed = matches!(self.link_state.borrow().status, SessionLinkStatus::Welcomed { .. });
                            connected = false; welcome_deadline = None; contact_flush_at = None;
                            lose_pending(&mut pending);
                            self.report_link(SessionLinkEventKind::Disconnected { reason: transport_drop(reason.clone(), self.client.last_transport_close()) }, &host);
                            let recovery = self.recover_disconnected(&reason, was_welcomed, &host).await;
                            let retry_at = if recovery.is_ok() { self.client.reconnect_at() } else { None };
                            if !forward(&self.events, ClientEvent::Disconnected { reason, retry_at }, &mut shutdown).await { break Ok(()); }
                            recovery?;
                        }
                        Ok(event) => if !forward(&self.events, event, &mut shutdown).await { break Ok(()); },
                        Err(ClientError::Protocol) => {
                            lose_pending(&mut pending);
                            // Malformed input queues its disconnect in the client.
                        }
                        Err(ClientError::NotConnected | ClientError::Connection(_)) => {
                            connected = false; welcome_deadline = None; contact_flush_at = None;
                            lose_pending(&mut pending);
                            self.report_link(SessionLinkEventKind::Disconnected { reason: transport_drop(None, None) }, &host);
                        }
                        Err(error) => {
                            self.report_link(SessionLinkEventKind::Disconnected { reason: transport_drop(None, None) }, &host);
                            break Err(error);
                        }
                    },
                    _ = host.timer.wait(), if contact_flush_at.is_some() => {
                        let now = host.now();
                        if welcome_deadline.is_some_and(|at: SyncTime| now >= at) {
                            self.report_link(SessionLinkEventKind::WelcomeTimeout, &host);
                            self.client.retry_connection(None, &host).await?;
                            connected = false; welcome_deadline = None; contact_flush_at = None;
                            lose_pending(&mut pending);
                        } else {
                            if contact_flush_at.is_some_and(|at| now >= at) {
                                self.publish_contact();
                                contact_flush_at = Some(now.after(Duration::from_secs(10)));
                            }
                            for at in [welcome_deadline, contact_flush_at].into_iter().flatten() { host.wake_at(at).await?; }
                        }
                    },
                    _ = shutdown.changed() => break Ok(()),
                }
                pending.retain(|_, answer| !answer.is_closed());
            }
        }.await;
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            refuse_command(command);
        }
        lose_pending(&mut pending);
        if result.is_ok() {
            self.report_link(
                SessionLinkEventKind::Disconnected {
                    reason: SessionDropReason::Disconnected {
                        reason: Some(CloseReason::ClientClosed),
                    },
                },
                &host,
            );
        }
        self.client.close().await;
        result
    }

    fn report_link(&self, kind: SessionLinkEventKind, host: &impl Clock) {
        let last_frame_at = if matches!(kind, SessionLinkEventKind::Connecting { .. }) {
            None
        } else {
            self.client.last_frame_at()
        };
        let retry_at = if matches!(
            kind,
            SessionLinkEventKind::CredentialRejected {
                reason: AuthRefusal::WrongKind
            }
        ) || self.recovery.is_some_and(|policy| {
            policy.credential_rejected == CredentialRejectedRecovery::Terminal
        }) {
            None
        } else {
            self.client.reconnect_at()
        };
        self.link_state.send_if_modified(|state| {
            state.observe(&kind, retry_at, self.connection, host.now(), last_frame_at)
        });
        let _ = self.link_events.send(SessionLinkEvent {
            connection: self.connection,
            kind,
        });
    }

    fn publish_contact(&self) {
        let last_frame_at = self.client.last_frame_at();
        self.link_state.send_if_modified(|state| {
            if state.connection != self.connection || state.last_frame_at == last_frame_at {
                return false;
            }
            state.last_frame_at = last_frame_at;
            state.revision = state.revision.saturating_add(1);
            true
        });
    }

    async fn recover_credential_refusal(
        &mut self,
        reason: AuthRefusal,
        origin: CredentialRefusalOrigin,
        host: &(impl Clock + WakeScheduler),
    ) -> Result<(), ClientError> {
        if reason == AuthRefusal::Revoked {
            self.report_link(SessionLinkEventKind::Revoked, host);
            return Err(ClientError::Revoked);
        }
        if reason == AuthRefusal::WrongKind {
            self.report_link(SessionLinkEventKind::CredentialRejected { reason }, host);
            return Err(ClientError::CredentialRejected { reason });
        }
        match self.recovery.map(|policy| policy.credential_rejected) {
            Some(CredentialRejectedRecovery::Retry { backoff }) => {
                self.client.retry_connection(Some(backoff), host).await?
            }
            None if matches!(origin, CredentialRefusalOrigin::SessionFrame) => {}
            Some(CredentialRejectedRecovery::Terminal) | None => {
                self.report_link(SessionLinkEventKind::CredentialRejected { reason }, host);
                return Err(ClientError::CredentialRejected { reason });
            }
        }
        self.report_link(SessionLinkEventKind::CredentialRejected { reason }, host);
        Ok(())
    }

    async fn recover_disconnected(
        &mut self,
        reason: &Option<CloseReason>,
        was_welcomed: bool,
        host: &(impl Clock + WakeScheduler),
    ) -> Result<(), ClientError> {
        match reason {
            Some(CloseReason::CredentialRejected(reason)) => {
                self.recover_credential_refusal(
                    *reason,
                    CredentialRefusalOrigin::SessionFrame,
                    host,
                )
                .await
            }
            Some(CloseReason::SessionEnded) if was_welcomed => {
                self.report_link(SessionLinkEventKind::ReplacedByNewerSession, host);
                Err(ClientError::ReplacedByNewerSession)
            }
            Some(CloseReason::SessionEnded) => match self
                .recovery
                .unwrap_or_else(SessionRecoveryPolicy::terminal)
                .session_ended
            {
                SessionEndedRecovery::Terminal => Err(ClientError::SessionEnded),
                SessionEndedRecovery::StartNewSession => {
                    let old = self.client.state().session_id;
                    self.client.start_new_session(&UuidV7Ids).await?;
                    self.client.retry_connection(None, host).await?;
                    self.report_link(
                        SessionLinkEventKind::SessionRestarted {
                            old,
                            new: self.client.state().session_id,
                        },
                        host,
                    );
                    Ok(())
                }
            },
            _ => Ok(()),
        }
    }
}

fn transport_drop(
    reason: Option<CloseReason>,
    transport: Option<&ConnectionClosed>,
) -> SessionDropReason {
    match (reason, transport) {
        (None, Some(ConnectionClosed::IdleTimeout)) => SessionDropReason::IdleTimeout,
        (None, Some(ConnectionClosed::SendTimeout)) => SessionDropReason::SendTimeout,
        (reason, _) => SessionDropReason::Disconnected { reason },
    }
}

#[cfg(test)]
#[path = "client_pump_test.rs"]
mod tests;
fn unavailable() -> CallFailure {
    CallFailure::NotSent {
        reason: NotSentReason::Unreachable { last_seen: None },
    }
}
fn refuse_command<P: ConsumerProtocol>(command: Command<P>) {
    match command {
        Command::Request { answer, .. } => {
            let _ = answer.send(Err(unavailable()));
        }
        Command::Answer { done, .. } | Command::Acknowledge { done, .. } => {
            let _ = done.send(Err(ClientError::NotConnected));
        }
    }
}
fn lose_pending<P: ConsumerProtocol>(
    pending: &mut BTreeMap<FrameId, oneshot::Sender<Result<OperationAnswer<P>, CallFailure>>>,
) {
    for (_, answer) in std::mem::take(pending) {
        let _ = answer.send(Err(CallFailure::AnswerLost));
    }
}
async fn forward<P: ConsumerProtocol>(
    events: &mpsc::Sender<ClientEvent<P>>,
    event: ClientEvent<P>,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! { result=events.send(event)=>result.is_ok(), _=shutdown.changed()=>false }
}
struct PumpClock<C: Clock> {
    clock: C,
    timer: TokioWakeTimer<C>,
}
impl<C: Clock + Clone> PumpClock<C> {
    fn new(clock: C) -> Self {
        Self {
            timer: TokioWakeTimer::new(clock.clone()),
            clock,
        }
    }
}
impl<C: Clock> Clock for PumpClock<C> {
    fn now(&self) -> runtime_primitives::SyncTime {
        self.clock.now()
    }
}
impl<C: Clock> WakeScheduler for PumpClock<C> {
    async fn wake_at(
        &self,
        at: runtime_primitives::SyncTime,
    ) -> Result<(), runtime_primitives::WakeFailure> {
        self.timer.wake_at(at).await
    }
}
