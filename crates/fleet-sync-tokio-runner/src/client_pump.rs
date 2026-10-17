use durable_steps::{CallFailure, NotSentReason, Uuid};
use fleet_sync_protocol::{
    ClientAnswer, ConsumerProtocol, FrameId, NoticePosition, OperationAnswer, OperationRequest,
};
use fleet_sync_session::{
    ClientError, ClientEvent, Endpoint, PeerCredential, SessionClient, SessionClientStore,
    SessionTransport,
};
use runtime_primitives::{Clock, SystemClock, TokioWakeTimer, WakeScheduler};
use std::{collections::BTreeMap, time::Duration};
use tokio::sync::{mpsc, oneshot, watch};

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
pub struct SessionPump<T: SessionTransport, S: SessionClientStore, P: ConsumerProtocol> {
    client: SessionClient<T, S, P>,
    endpoint: Endpoint,
    credential: PeerCredential,
    commands: mpsc::Receiver<Command<P>>,
    events: mpsc::Sender<ClientEvent<P>>,
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
            },
        )
    }
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) -> Result<(), ClientError> {
        let host = PumpClock::new();
        let mut pending = BTreeMap::new();
        let mut connected = false;
        let result = loop {
            if *shutdown.borrow() {
                break Ok(());
            }
            if !connected {
                let connected_result = tokio::select! {
                    result=self.client.connect(&self.endpoint, &self.credential, &host)=>result,
                    _=shutdown.changed()=>break Ok(()),
                };
                match connected_result {
                    Ok(()) => connected = true,
                    Err(
                        ClientError::RetryAt { .. }
                        | ClientError::Connect(_)
                        | ClientError::Connection(_),
                    ) if self.client.reconnect_at().is_some() => {}
                    Err(error) => break Err(error),
                }
                if !connected {
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
            }
            tokio::select! {
                command=self.commands.recv()=>match command {
                    Some(Command::Request { body, answer })=>{
                        if answer.is_closed() {continue;}
                        let frame=FrameId::new(Uuid::now_v7()).map_err(|_|ClientError::Protocol)?;
                        match self.client.send_request(frame, body, &host).await {
                            Ok(())=>{pending.insert(frame,answer);}
                            Err(_)=>{let _=answer.send(Err(CallFailure::AnswerLost));connected=false;lose_pending(&mut pending);}
                        }
                    }
                    Some(Command::Answer { frame, body, done })=>{let _=done.send(self.client.send_answer(frame,body,&host).await);}
                    Some(Command::Acknowledge { position, done })=>{let _=done.send(self.client.acknowledge_notice(position).await);}
                    None=>break Ok(()),
                },
                event=self.client.receive(&host)=>match event {
                    Ok(ClientEvent::Answer { frame, body })=>{
                        if let Some(answer)=pending.remove(&frame) {let _=answer.send(Ok(body));}
                        else if !forward(&self.events,ClientEvent::Answer {frame,body},&mut shutdown).await {break Ok(());}
                    }
                    Ok(ClientEvent::Unknown { frames })=>{
                        for frame in &frames {if let Some(answer)=pending.remove(frame) {let _=answer.send(Err(CallFailure::AnswerLost));}}
                        if !forward(&self.events,ClientEvent::Unknown {frames},&mut shutdown).await {break Ok(());}
                    }
                    Ok(event @ ClientEvent::Disconnected { .. })=>{
                        connected=false;lose_pending(&mut pending);
                        if !forward(&self.events,event,&mut shutdown).await {break Ok(());}
                    }
                    Ok(event)=>if !forward(&self.events,event,&mut shutdown).await {break Ok(());},
                    Err(ClientError::NotConnected | ClientError::Connection(_) | ClientError::Protocol)=>{connected=false;lose_pending(&mut pending);}
                    Err(error)=>break Err(error),
                },
                _=shutdown.changed()=>break Ok(()),
            }
            pending.retain(|_, answer| !answer.is_closed());
        };
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            refuse_command(command);
        }
        lose_pending(&mut pending);
        self.client.close().await;
        result
    }
}
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
struct PumpClock {
    timer: TokioWakeTimer<SystemClock>,
}
impl PumpClock {
    fn new() -> Self {
        Self {
            timer: TokioWakeTimer::new(SystemClock),
        }
    }
}
impl Clock for PumpClock {
    fn now(&self) -> runtime_primitives::SyncTime {
        SystemClock.now()
    }
}
impl WakeScheduler for PumpClock {
    async fn wake_at(
        &self,
        at: runtime_primitives::SyncTime,
    ) -> Result<(), runtime_primitives::WakeFailure> {
        self.timer.wake_at(at).await
    }
}
