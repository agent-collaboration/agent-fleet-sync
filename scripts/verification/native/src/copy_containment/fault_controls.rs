//! Own the real native service, session and fault controls for C6 journeys.
use super::*;
use crate::support::control_request;
use fleet_sync_test_consumer::{ControlAnswer, ControlRequest, CopyFaultRequest};
use fleet_sync_transport_websocket::WebSocketConnection;

pub(super) struct RunningCopy {
    pub session: ObservedSession,
    pub control: WebSocketConnection,
    process: ServiceProcess,
}
impl RunningCopy {
    pub async fn start(
        root: &Path,
        client: &Path,
        peer: PeerId,
        receiver: ReceiverId,
    ) -> TestResult<Self> {
        let mut process = ServiceProcess::start_as(root, receiver.as_uuid().as_u128()).await?;
        let mut control = match process.connect("control", "Bearer consumer-control").await {
            Ok(control) => control,
            Err(error) => {
                process.kill().await?;
                return Err(error);
            }
        };
        let session = match ObservedSession::start(
            &format!("ws://127.0.0.1:{}/session", process.port),
            peer,
            client,
        )
        .await
        {
            Ok(session) => session,
            Err(error) => {
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
    pub async fn stop(self) -> TestResult<Vec<Value>> {
        let mut control = self.control;
        let session = self.session.stop().await;
        let service = self.process.stop(&mut control).await;
        let frames = session?;
        service?;
        Ok(frames)
    }
    pub async fn kill(self) -> TestResult<Vec<Value>> {
        let mut process = self.process;
        let session = self.session.stop().await;
        let killed = process.kill().await;
        let frames = session?;
        killed?;
        Ok(frames)
    }
    pub async fn fault(&mut self, request: CopyFaultRequest) -> TestResult<ControlAnswer> {
        control_request(&mut self.control, ControlRequest::CopyFault(request)).await
    }
    pub async fn marker(
        &mut self,
        database: DatabaseId,
        replacement: String,
    ) -> TestResult<String> {
        match self
            .fault(CopyFaultRequest::ReplaceMarker {
                database,
                replacement,
            })
            .await?
        {
            ControlAnswer::CopyMarker(previous) => Ok(previous),
            _ => Err("actual copy owner did not replace marker".into()),
        }
    }
}
pub(super) async fn new_writer(
    root: &Path,
    peer: PeerId,
    provisional: bool,
) -> TestResult<Arc<ReplicatedDatabase<SampleSchema>>> {
    Ok(Arc::new(
        ReplicatedDatabase::create(
            root,
            SampleSchema::new()?,
            peer,
            policy(),
            WriterOptions {
                allow_provisional_writes: provisional,
            },
        )
        .await?,
    ))
}
pub(super) async fn bootstrap(
    writer: &Arc<ReplicatedDatabase<SampleSchema>>,
    peer: PeerId,
    receiver: ReceiverId,
    session: &ObservedSession,
) -> TestResult {
    let lease = register(writer, peer, session).await?;
    let sending = sender(writer.clone(), receiver, session);
    sending
        .add_copy(
            receiver,
            CopyRole::Authoritative,
            ReplicationPosition::new(lease.epoch, CommitSequence::new(0)),
        )
        .await?;
    sending.replicate_once(receiver).await?;
    Ok(())
}
pub(super) fn acknowledged(result: &Result<Vec<ReplicationEvents>, SenderError>) -> bool {
    result.as_ref().is_ok_and(|events| {
        events
            .iter()
            .any(|event| matches!(event, ReplicationEvents::Acknowledged { .. }))
    })
}
