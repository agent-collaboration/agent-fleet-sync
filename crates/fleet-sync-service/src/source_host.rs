use crate::{
    ServiceConsumer, ServiceError, ServiceHandle,
    call_router::{lost, unreachable},
    service_state::{ServiceState, SourceRunner},
};
use durable_steps::CallFailure;
use fleet_sync_protocol::{
    ConsumerProtocol, OperationAnswer, OperationRequest, ReceiverId, ReplicationAnswer,
    ReplicationRequest,
};
use fleet_sync_session::{
    ClientEvent, Endpoint, PeerAuthenticator, PeerCredential, SessionClient, SessionClientStore,
    SessionTransport,
};
use fleet_sync_tokio_runner::{SessionPump, SessionRequests};
use tokio::sync::{mpsc, watch};

/// The consumer handles server requests and notices on its existing session.
pub struct AttachedSource<P: ConsumerProtocol> {
    pub requests: SessionRequests<P>,
    pub events: mpsc::Receiver<ClientEvent<P>>,
}
impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>>
    ServiceHandle<P, C, A>
{
    /// Re-attach at startup. The consumer keeps its SessionClientStore; endpoint
    /// and credential stay in this volatile host owner and never enter a journal.
    pub async fn attach_source<T: SessionTransport, S: SessionClientStore>(
        &self,
        receiver: ReceiverId,
        client: SessionClient<T, S, P>,
        endpoint: Endpoint,
        credential: PeerCredential,
    ) -> Result<AttachedSource<P>, ServiceError> {
        let mut sources = self.state.sources.lock().await;
        if sources
            .get(&receiver)
            .is_some_and(|source| !source.is_closed())
        {
            return Err(ServiceError::rule(
                "source already has a running session owner",
            ));
        }
        let (requests, events, pump) = SessionPump::new(
            client,
            endpoint,
            credential,
            self.state.options.call_timeout,
        );
        let (stop, shutdown) = watch::channel(false);
        let mut runners = self.state.source_runners.lock().await;
        if *self.state.shutdown.borrow() {
            return Err(ServiceError::rule("service is shutting down"));
        }
        runners.push(SourceRunner {
            run: Box::pin(pump.run(shutdown)),
            stop,
        });
        drop(runners);
        sources.insert(receiver, requests.clone());
        drop(sources);
        self.state.source_added.notify_one();
        Ok(AttachedSource { requests, events })
    }
}
impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>>
    ServiceState<P, C, A>
{
    pub(crate) async fn source_request(
        &self,
        receiver: ReceiverId,
        request: ReplicationRequest<P::Annotation>,
    ) -> Result<ReplicationAnswer, CallFailure> {
        let source = self.sources.lock().await.get(&receiver).cloned();
        let Some(source) = source else {
            #[cfg(feature = "test-support")]
            self.install_page_probe.source_missing(receiver);
            return Err(unreachable());
        };
        match source
            .request(OperationRequest::Replication(request))
            .await?
        {
            OperationAnswer::Replication(ReplicationAnswer::CopyUnavailable { reason }) => {
                Err(fleet_sync_receiver::retryable_copy(reason))
            }
            OperationAnswer::Replication(ReplicationAnswer::OutcomeUnknown { .. }) => {
                Err(CallFailure::AnswerLost)
            }
            OperationAnswer::Replication(answer) => Ok(answer),
            _ => Err(lost("source answered another operation family")),
        }
    }
}
