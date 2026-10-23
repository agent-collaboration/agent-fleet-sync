use super::*;
use durable_steps::{CallFailure, NotSentReason};
use fleet_sync_session::{Endpoint, PeerCredential, SessionTransport, TransportConnection};
use fleet_sync_transport_websocket::{WebSocketConnection, WebSocketTransport};
use tokio::sync::{mpsc, oneshot};

type Reply = oneshot::Sender<Result<DeliveryAnswer<SampleDelivery>, CallFailure>>;
enum Attachment {
    AwaitingWelcome { answer: oneshot::Sender<TestResult> },
    Attached,
}
struct SessionLink {
    socket: WebSocketConnection,
    session: SessionId,
    attachment: Attachment,
}
struct PendingRequest {
    frame: FrameId,
    hop: &'static str,
    call: CallId,
    item: Option<ItemId>,
    answer: Reply,
}
enum LinkCommand {
    Connect {
        port: u16,
        owner: PeerId,
        answer: oneshot::Sender<TestResult>,
    },
    Disconnect {
        answer: oneshot::Sender<()>,
    },
    Request {
        request: DeliveryRequest<SampleDelivery>,
        answer: Reply,
    },
}
pub(super) struct PeerLink {
    commands: mpsc::Sender<LinkCommand>,
}
pub(super) struct LinkActor {
    commands: mpsc::Receiver<LinkCommand>,
}
fn unreachable() -> CallFailure {
    CallFailure::NotSent {
        reason: NotSentReason::Unreachable { last_seen: None },
    }
}
impl PeerLink {
    pub fn new() -> (Self, LinkActor) {
        let (commands, incoming) = mpsc::channel(32);
        (Self { commands }, LinkActor { commands: incoming })
    }
    pub async fn connect(&self, port: u16, owner: PeerId) -> TestResult {
        let (answer, receipt) = oneshot::channel();
        self.commands
            .send(LinkCommand::Connect {
                port,
                owner,
                answer,
            })
            .await?;
        receipt.await?
    }
    pub async fn disconnect(&self) -> TestResult {
        let (answer, receipt) = oneshot::channel();
        self.commands
            .send(LinkCommand::Disconnect { answer })
            .await?;
        receipt.await?;
        Ok(())
    }
    pub async fn request(
        &self,
        request: DeliveryRequest<SampleDelivery>,
    ) -> Result<DeliveryAnswer<SampleDelivery>, CallFailure> {
        let (answer, receipt) = oneshot::channel();
        self.commands
            .send(LinkCommand::Request { request, answer })
            .await
            .map_err(|_| unreachable())?;
        receipt.await.map_err(|_| CallFailure::AnswerLost)?
    }
}
fn hop(request: &DeliveryRequest<SampleDelivery>) -> &'static str {
    match request {
        DeliveryRequest::Submit { .. } => "submit",
        DeliveryRequest::Fetch { .. } => "fetch",
        DeliveryRequest::Received { .. } => "received",
        DeliveryRequest::Report { .. } => "report",
        DeliveryRequest::Settle { .. } => "settle",
        DeliveryRequest::StatusFetch { .. } => "statusFetch",
        DeliveryRequest::StatusAck { .. } => "statusAck",
        DeliveryRequest::Query { .. } => "query",
        DeliveryRequest::Retire { .. } => "retire",
    }
}
async fn connect(port: u16, owner: PeerId) -> TestResult<(WebSocketConnection, SessionId)> {
    let credential = format!("Bearer peer-{}", owner.as_uuid().as_u128());
    let mut socket = WebSocketTransport::default()
        .connect(
            &Endpoint::new(&format!("ws://127.0.0.1:{port}/session"))?,
            &PeerCredential::new(credential)?,
        )
        .await?;
    let session = SessionId::new(durable_steps::Uuid::now_v7())?;
    socket
        .send(
            serde_json::to_vec(&ClientFrame::<SampleProtocol>::Hello {
                session_id: session,
                schema_support: SchemaSupport { max_version: 100 },
            })?
            .into(),
        )
        .await?;
    // SessionClient uses the same model: Hello starts the transport and its
    // receive pump handles both Welcome and any already-due server requests.
    Ok((socket, session))
}
async fn close(socket: &mut Option<SessionLink>, pending: &mut Vec<PendingRequest>) {
    if let Some(link) = socket.take() {
        if let Attachment::AwaitingWelcome { answer } = link.attachment {
            let _ = answer.send(Err("session closed before Welcome".into()));
        }
        link.socket.close(CloseReason::ClientClosed).await;
    }
    for pending in pending.drain(..) {
        let _ = pending.answer.send(Err(CallFailure::AnswerLost));
    }
}
impl LinkActor {
    pub async fn run(
        mut self,
        state: Weak<NodeState>,
        mut stopped: watch::Receiver<bool>,
    ) -> TestResult {
        let mut socket: Option<SessionLink> = None;
        let mut pending: Vec<PendingRequest> = Vec::new();
        loop {
            tokio::select! {
                changed=stopped.changed()=>{if changed.is_err() || *stopped.borrow() {break;}},
                command=self.commands.recv()=>match command {
                    None=>break,
                    Some(LinkCommand::Connect {port,owner,answer})=>{
                        close(&mut socket,&mut pending).await;
                        let result=connect(port,owner).await;
                        match result {
                            Ok((connected,session))=>socket=Some(SessionLink {socket:connected,session,attachment:Attachment::AwaitingWelcome {answer}}),
                            Err(error)=>{let _=answer.send(Err(error));}
                        }
                    }
                    Some(LinkCommand::Disconnect {answer})=>{
                        close(&mut socket,&mut pending).await;
                        let _=answer.send(());
                    }
                    Some(LinkCommand::Request {request,answer})=>{
                        if let Some(active)=socket.as_mut().filter(|link| matches!(&link.attachment,Attachment::Attached)) {
                            let frame=FrameId::new(durable_steps::Uuid::now_v7())?;
                            let kind=hop(&request);
                            let call=match &request {
                                DeliveryRequest::Submit {call,..}|DeliveryRequest::Fetch {call,..}|
                                DeliveryRequest::Received {call,..}|DeliveryRequest::Report {call,..}|
                                DeliveryRequest::Settle {call,..}|DeliveryRequest::StatusFetch {call,..}|DeliveryRequest::StatusAck {call,..}|
                                DeliveryRequest::Query {call,..}|DeliveryRequest::Retire {call,..}=>call.clone(),
                            };
                            let item=match &request {DeliveryRequest::Submit {item,..}=>Some(item.id()),DeliveryRequest::Report {report,..}=>Some(report.item),_=>None};
                            let bytes=serde_json::to_vec(&ClientFrame::<SampleProtocol>::Request {frame,body:OperationRequest::Delivery(request)})?;
                            match active.socket.send(bytes.into()).await {
                                Ok(())=>pending.push(PendingRequest {frame,hop:kind,call,item,answer}),
                                Err(_)=>{let _=answer.send(Err(CallFailure::AnswerLost));close(&mut socket,&mut pending).await;}
                            }
                        }else{let _=answer.send(Err(unreachable()));}
                    }
                },
                frame=async {match socket.as_mut() {Some(active)=>active.socket.receive().await,None=>std::future::pending().await}}=>{
                    let Some(Ok(bytes))=frame else {close(&mut socket,&mut pending).await;continue;};
                    let frame:ServerFrame<SampleProtocol>=serde_json::from_slice(&bytes)?;
                    let Some(state)=state.upgrade() else {break;};
                    match frame {
                        ServerFrame::Welcome {session_id,..} if socket.as_ref().is_some_and(|link|link.session==session_id)=>{
                            if let Some(link)=socket.as_mut() && let Attachment::AwaitingWelcome {answer}=std::mem::replace(&mut link.attachment,Attachment::Attached) {
                                let _=answer.send(Ok(()));
                            }
                        },
                        ServerFrame::Answer {frame,body:OperationAnswer::Delivery(body)}=>{
                            if let Some(index)=pending.iter().position(|pending|pending.frame==frame) {
                                let PendingRequest {hop:kind,call,item,answer,..}=pending.remove(index);
                                if matches!(&body, DeliveryAnswer::MailboxUnavailable {..}) {
                                    state.unavailable.lock().await.push((kind.into(),call));
                                    state.changed.notify_waiters();
                                }
                                if state.gate.after_item(kind,item).await {
                                    let _=answer.send(Err(CallFailure::AnswerLost));
                                    close(&mut socket,&mut pending).await;
                                }else{let _=answer.send(Ok(body));}
                            }
                        }
                        ServerFrame::Request {frame,body:ServerRequest::Crate(ServerCall::PublishStatus {publication,..})}=>{
                            if state.status_paused.load(std::sync::atomic::Ordering::SeqCst) {continue;}
                            let limits=delivery_limits()?;
                            let result=state.object.rule_transaction::<_,DeliveryError,_>(move|scope|Box::pin(async move {
                                Outbox::<_,SampleDelivery>::new(scope,limits).publish_status(publication).await
                            })).await;
                            let answer=match result {
                                Ok(())=>StatusPublicationResult::Applied,
                                Err(DeliveryError::Refused {reason})=>StatusPublicationResult::Refused {reason},
                                Err(error)=>return Err(error.into()),
                            };
                            state.changed.notify_waiters();
                            if state.gate.after("publication").await {close(&mut socket,&mut pending).await;continue;}
                            if let Some(active)=socket.as_mut() {
                                let bytes=serde_json::to_vec(&ClientFrame::<SampleProtocol>::Answer {frame,body:ClientAnswer::Crate(ServerCallAnswer::Published(answer))})?;
                                if active.socket.send(bytes.into()).await.is_err() {close(&mut socket,&mut pending).await;}
                            }
                        }
                        ServerFrame::Notice {..}=>{},
                        _=>return Err("unexpected delivery node frame".into()),
                    }
                },
            }
        }
        close(&mut socket, &mut pending).await;
        Ok(())
    }
}
