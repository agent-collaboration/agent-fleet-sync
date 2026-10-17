use crate::{ServiceConsumer, ServiceError, service_state::ServiceState};
use durable_steps::{AtomicStore, StoreError};
use fleet_sync_protocol::*;
use fleet_sync_session::{PeerAuthenticator, SessionKey};
use fleet_sync_tokio_runner::{SessionHandler, SessionHostError};
use runtime_primitives::Clock;
use std::sync::Weak;

pub(crate) struct ServiceSessionHandler<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<Material = String>,
> {
    pub state: Weak<ServiceState<P, C, A>>,
}
impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>>
    SessionHandler<P> for ServiceSessionHandler<P, C, A>
{
    async fn gather(&self, peer: PeerId) -> Result<Vec<DatabaseStatus>, SessionHostError> {
        self.state
            .upgrade()
            .ok_or_else(|| unavailable("service closed"))?
            .gather(peer)
            .await
            .map_err(unavailable)
    }
    async fn request(
        &self,
        session: SessionKey,
        operation: OperationRequest<P>,
    ) -> Result<OperationAnswer<P>, SessionHostError> {
        let state = self
            .state
            .upgrade()
            .ok_or_else(|| unavailable("service closed"))?;
        let answer = match operation {
            OperationRequest::Replication(request) => OperationAnswer::Replication(
                state
                    .replicate(session.peer, request)
                    .await
                    .map_err(unavailable)?,
            ),
            OperationRequest::Lease(LeaseRequest::Register(request)) => {
                if request.writer != session.peer {
                    return Ok(OperationAnswer::Lease(LeaseAnswer::Register(
                        RegistrationAnswer::Refused(RegistrationRefusal::NotWriter {
                            current: None,
                        }),
                    )));
                }
                OperationAnswer::Lease(LeaseAnswer::Register(
                    state.register(request).await.map_err(unavailable)?,
                ))
            }
            OperationRequest::Lease(LeaseRequest::Renew(request)) => {
                OperationAnswer::Lease(LeaseAnswer::Renew(
                    state
                        .renew(request, session.peer)
                        .await
                        .map_err(unavailable)?,
                ))
            }
            OperationRequest::Lease(LeaseRequest::Claim { attempt, database }) => {
                let transfer = TransferId::new(attempt.as_uuid()).map_err(unavailable)?;
                let answer = if let Some(answer) = state
                    .retirement_holder_answer(database)
                    .await
                    .map_err(unavailable)?
                {
                    answer
                } else {
                    let request = HolderChangeRequest::Claim {
                        attempt: transfer,
                        database,
                        claimant: session.peer,
                    };
                    let requester = ActorRef::new(format!("peer:{}", session.peer.as_uuid()))
                        .map_err(unavailable)?;
                    state
                        .holder_change(request, requester)
                        .await
                        .map_err(unavailable)?
                };
                OperationAnswer::Lease(LeaseAnswer::Claim(answer))
            }
            OperationRequest::Consumer(request) => OperationAnswer::Consumer(
                state
                    .consumer
                    .request(session.peer, request)
                    .await
                    .map_err(unavailable)?,
            ),
            OperationRequest::Delivery(_) => return Err(unavailable("delivery is not installed")),
        };
        Ok(answer)
    }
}
fn unavailable(error: impl std::fmt::Display) -> SessionHostError {
    SessionHostError::Unavailable {
        message: error.to_string(),
    }
}

impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>>
    ServiceState<P, C, A>
{
    pub(crate) async fn gather(&self, peer: PeerId) -> Result<Vec<DatabaseStatus>, ServiceError> {
        let databases = self
            .registry
            .transaction(|scope| {
                Box::pin(async move {
                    fleet_sync_lease::database_ids(scope)
                        .await
                        .map_err(StoreError::backend)
                })
            })
            .await?;
        let mut summaries = Vec::new();
        for database in databases {
            let now = self.registry.host().now();
            let lease = self
                .registry
                .transaction(move |scope| {
                    Box::pin(async move {
                        match fleet_sync_lease::lease_status(scope, database, now).await {
                            Ok(lease) => Ok(lease),
                            Err(fleet_sync_lease::LeaseError::Retired) => Ok(None),
                            Err(error) => Err(StoreError::backend(error)),
                        }
                    })
                })
                .await?;
            let Some(lease) = lease.filter(|lease| lease.holder == peer) else {
                continue;
            };
            summaries.push(match self.database_status(database, lease.clone()).await {
                Ok(Some(status)) => status,
                Ok(None) => continue,
                Err(error) => DatabaseStatus::Unavailable {
                    database,
                    lease: Some(lease),
                    reason: CopyUnavailableReason::Damaged {
                        detail: error.to_string(),
                    },
                },
            });
        }
        Ok(summaries)
    }
}
