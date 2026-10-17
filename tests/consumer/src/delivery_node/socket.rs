use super::*;
use durable_steps::{CallFailure, NotSentReason};
use fleet_sync_session::{Endpoint, PeerCredential, SessionTransport, TransportConnection};
use fleet_sync_transport_websocket::{WebSocketConnection, WebSocketTransport};
use tokio::sync::{mpsc, oneshot};

type Reply = oneshot::Sender<Result<DeliveryAnswer<SampleDelivery>, CallFailure>>;
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
async fn close(
    socket: &mut Option<WebSocketConnection>,
    pending: &mut Vec<(FrameId, &'static str, Reply)>,
) {
    if let Some(socket) = socket.take() {
        socket.close(CloseReason::ClientClosed).await;
    }
    for (_, _, answer) in pending.drain(..) {
        let _ = answer.send(Err(CallFailure::AnswerLost));
    }
}
impl LinkActor {
    pub async fn run(
        mut self,
        state: Weak<NodeState>,
        mut stopped: watch::Receiver<bool>,
    ) -> TestResult {
        let mut socket: Option<WebSocketConnection> = None;
        let mut expected_session = None;
        let mut pending: Vec<(FrameId, &'static str, Reply)> = Vec::new();
        loop {
            tokio::select! {
                changed=stopped.changed()=>{if changed.is_err() || *stopped.borrow() {break;}},
                command=self.commands.recv()=>match command {
                    None=>break,
                    Some(LinkCommand::Connect {port,owner,answer})=>{
                        close(&mut socket,&mut pending).await;
                        let result=connect(port,owner).await;
                        let receipt=match result {Ok((connected,session))=>{socket=Some(connected);expected_session=Some(session);Ok(())},Err(error)=>Err(error)};
                        let _=answer.send(receipt);
                    }
                    Some(LinkCommand::Disconnect {answer})=>{
                        close(&mut socket,&mut pending).await;
                        let _=answer.send(());
                    }
                    Some(LinkCommand::Request {request,answer})=>{
                        if let Some(active)=socket.as_mut() {
                            let frame=FrameId::new(durable_steps::Uuid::now_v7())?;
                            let kind=hop(&request);
                            let bytes=serde_json::to_vec(&ClientFrame::<SampleProtocol>::Request {frame,body:OperationRequest::Delivery(request)})?;
                            match active.send(bytes.into()).await {
                                Ok(())=>pending.push((frame,kind,answer)),
                                Err(_)=>{let _=answer.send(Err(CallFailure::AnswerLost));close(&mut socket,&mut pending).await;}
                            }
                        }else{let _=answer.send(Err(unreachable()));}
                    }
                },
                frame=async {match socket.as_mut() {Some(active)=>active.receive().await,None=>std::future::pending().await}}=>{
                    let Some(Ok(bytes))=frame else {close(&mut socket,&mut pending).await;continue;};
                    let frame:ServerFrame<SampleProtocol>=serde_json::from_slice(&bytes)?;
                    let Some(state)=state.upgrade() else {break;};
                    match frame {
                        ServerFrame::Welcome {session_id,..} if expected_session==Some(session_id)=>{},
                        ServerFrame::Answer {frame,body:OperationAnswer::Delivery(body)}=>{
                            if let Some(index)=pending.iter().position(|(expected,_,_)|*expected==frame) {
                                let (_,kind,answer)=pending.remove(index);
                                if state.gate.after(kind).await {
                                    let _=answer.send(Err(CallFailure::AnswerLost));
                                    close(&mut socket,&mut pending).await;
                                }else{let _=answer.send(Ok(body));}
                            }
                        }
                        ServerFrame::Request {frame,body:ServerRequest::Crate(ServerCall::PublishStatus {publication,..})}=>{
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
                                if active.send(bytes.into()).await.is_err() {close(&mut socket,&mut pending).await;}
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
