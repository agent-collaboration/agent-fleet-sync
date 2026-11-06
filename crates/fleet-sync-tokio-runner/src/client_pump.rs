use crate::{
    CredentialRejectedRecovery, SessionEndedRecovery, SessionLinkEvent, SessionLinkState,
    SessionRecoveryPolicy,
};
use durable_steps::{CallFailure, NotSentReason, Uuid};
use fleet_sync_protocol::{
    AuthRefusal, ClientAnswer, CloseReason, ConsumerProtocol, FrameId, NoticePosition,
    OperationAnswer, OperationRequest,
};
use fleet_sync_session::{
    ClientError, ClientEvent, ConnectFailure, Endpoint, PeerCredential, SessionClient,
    SessionClientStore, SessionTransport,
};
use runtime_primitives::{Clock, SyncTime, SystemClock, TokioWakeTimer, UuidV7Ids, WakeScheduler};
use std::{collections::BTreeMap, time::Duration};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

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
    pub async fn acknowledge_notice(&self, position: NoticePosition) -> Result<(), ClientError> {
        let (done, result) = oneshot::channel();
        self.sender
            .send(Command::Acknowledge { position, done })
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
    recovery: SessionRecoveryPolicy,
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
                recovery: SessionRecoveryPolicy::terminal(),
                link_events: broadcast::channel(128).0,
                link_state: watch::channel(SessionLinkState::default()).0,
            },
        )
    }
    pub fn with_recovery_policy(mut self, policy: SessionRecoveryPolicy) -> Self {
        self.client
            .set_credential_recovery(policy.credential_rejected);
        self.recovery = policy;
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
        let mut attempt = 0_u32;
        // Keep every fallible recovery path inside the cleanup boundary.
        let result = async { 'pumping: loop {
            if *shutdown.borrow() {
                break Ok(());
            }
            if !connected {
                // Refused commands must not bypass the scheduled backoff.
                if let Some(at) = self.client.reconnect_at()
                    && host.now() < at
                {
                    host.wake_at(at).await?;
                    tokio::select! {
                        _=host.timer.wait()=>{},
                        command=self.commands.recv()=>match command {
                            Some(command)=>refuse_command(command),
                            None=>break Ok(()),
                        },
                        _=shutdown.changed()=>break Ok(()),
                    }
                    continue;
                }
                attempt = attempt.saturating_add(1);
                self.report_link(SessionLinkEvent::Connecting { attempt });
                let connected_result = {
                    let connecting = self.client.connect(&self.endpoint, &self.credential, &host);
                    tokio::pin!(connecting);
                    loop {
                        tokio::select! {
                            result=&mut connecting=>break result,
                            command=self.commands.recv()=>match command {
                                Some(command)=>refuse_command(command),
                                None=>break 'pumping Ok(()),
                            },
                            _=shutdown.changed()=>break 'pumping Ok(()),
                        }
                    }
                };
                match connected_result {
                    Ok(()) => {
                        connected = true;
                        welcome_deadline = self.recovery.welcome_timeout.map(|timeout| host.now().after(timeout));
                        if let Some(at) = welcome_deadline { host.wake_at(at).await?; }
                    },
                    Err(ClientError::Connect(ConnectFailure::CredentialRejected(reason))) => {
                        if reason == AuthRefusal::Revoked {
                            self.report_link(SessionLinkEvent::Revoked);
                            break Err(ConnectFailure::CredentialRejected(reason).into());
                        }
                        self.report_link(SessionLinkEvent::CredentialRejected { reason });
                        match self.recovery.credential_rejected {
                            CredentialRejectedRecovery::Terminal => break Err(ConnectFailure::CredentialRejected(reason).into()),
                            CredentialRejectedRecovery::Retry { .. } => {},
                        }
                    },
                    Err(
                        ClientError::RetryAt { .. }
                        | ClientError::Connect(_)
                        | ClientError::Connection(_),
                    ) if self.client.reconnect_at().is_some() => { self.report_link(SessionLinkEvent::Disconnected { reason: None }); }
                    Err(error) => { self.report_link(SessionLinkEvent::Disconnected { reason: None }); break Err(error); },
                }
                if !connected {
                    continue;
                }
            }
            tokio::select! {
                command=self.commands.recv()=>match command {
                    Some(Command::Request { body, answer })=>{
                        if answer.is_closed() {continue;}
                        let frame=FrameId::new(Uuid::now_v7()).map_err(|_|ClientError::Protocol)?;
                        match self.client.send_request(frame, body, &host).await {
                            Ok(())=>{pending.insert(frame,answer);}
                            Err(ClientError::NotConnected)=>{let _=answer.send(Err(unavailable()));}
                            Err(_)=>{let _=answer.send(Err(CallFailure::AnswerLost));connected=false;lose_pending(&mut pending);}
                        }
                    }
                    Some(Command::Answer { frame, body, done })=>{let _=done.send(self.client.send_answer(frame,body,&host).await);}
                    Some(Command::Acknowledge { position, done })=>{let _=done.send(self.client.acknowledge_notice(position).await);}
                    None=>break Ok(()),
                },
                event=self.client.receive(&host)=>match event {
                    Ok(event @ ClientEvent::Welcome { .. })=>{
                        welcome_deadline=None;attempt=0;
                        self.report_link(SessionLinkEvent::Welcomed { session_id:self.client.state().session_id });
                        if !forward(&self.events,event,&mut shutdown).await {break Ok(());}
                    }
                    Ok(ClientEvent::Answer { frame, body })=>{
                        if let Some(answer)=pending.remove(&frame) {let _=answer.send(Ok(body));}
                        else if !forward(&self.events,ClientEvent::Answer {frame,body},&mut shutdown).await {break Ok(());}
                    }
                    Ok(ClientEvent::Unknown { frames })=>{
                        for frame in &frames {if let Some(answer)=pending.remove(frame) {let _=answer.send(Err(CallFailure::AnswerLost));}}
                        if !forward(&self.events,ClientEvent::Unknown {frames},&mut shutdown).await {break Ok(());}
                    }
                    Ok(ClientEvent::Disconnected { reason, retry_at })=>{
                        connected=false;welcome_deadline=None;lose_pending(&mut pending);
                        self.report_link(SessionLinkEvent::Disconnected {reason:reason.clone()});
                        let recovery=self.recover_disconnected(&reason).await;
                        if !forward(&self.events,ClientEvent::Disconnected {reason,retry_at},&mut shutdown).await {break Ok(());}
                        recovery?;
                    }
                    Ok(event)=>if !forward(&self.events,event,&mut shutdown).await {break Ok(());},
                    Err(ClientError::NotConnected | ClientError::Connection(_) | ClientError::Protocol)=>{connected=false;welcome_deadline=None;lose_pending(&mut pending);self.report_link(SessionLinkEvent::Disconnected {reason:None});}
                    Err(error)=>{self.report_link(SessionLinkEvent::Disconnected {reason:None});break Err(error);},
                },
                _=host.timer.wait(), if welcome_deadline.is_some()=>{
                    if welcome_deadline.is_some_and(|at: SyncTime| host.now() >= at) {
                        self.report_link(SessionLinkEvent::WelcomeTimeout);
                        self.client.retry_connection(&host).await?;
                        connected=false;welcome_deadline=None;lose_pending(&mut pending);
                    } else if let Some(at)=welcome_deadline {host.wake_at(at).await?;}
                },
                _=shutdown.changed()=>break Ok(()),
            }
            pending.retain(|_, answer| !answer.is_closed());
        }}.await;
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            refuse_command(command);
        }
        lose_pending(&mut pending);
        if result.is_ok() {
            self.report_link(SessionLinkEvent::Disconnected {
                reason: Some(CloseReason::ClientClosed),
            });
        }
        self.client.close().await;
        result
    }
    fn report_link(&self, event: SessionLinkEvent) {
        self.link_state
            .send_modify(|state| state.observe(&event, self.client.reconnect_at()));
        let _ = self.link_events.send(event);
    }
    async fn recover_disconnected(
        &mut self,
        reason: &Option<CloseReason>,
    ) -> Result<(), ClientError> {
        match reason {
            Some(CloseReason::CredentialRejected(AuthRefusal::Revoked)) => {
                self.report_link(SessionLinkEvent::Revoked);
                Err(ClientError::SessionEnded)
            }
            Some(CloseReason::CredentialRejected(reason)) => {
                self.report_link(SessionLinkEvent::CredentialRejected { reason: *reason });
                if self.client.reconnect_at().is_none() {
                    Err(ClientError::SessionEnded)
                } else {
                    Ok(())
                }
            }
            Some(CloseReason::SessionEnded) => match self.recovery.session_ended {
                SessionEndedRecovery::Terminal => Err(ClientError::SessionEnded),
                SessionEndedRecovery::StartNewSession => {
                    let old = self.client.state().session_id;
                    self.client.start_new_session(&UuidV7Ids).await?;
                    self.report_link(SessionLinkEvent::SessionRestarted {
                        old,
                        new: self.client.state().session_id,
                    });
                    Ok(())
                }
            },
            _ => Ok(()),
        }
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
