use crate::{
    ServiceConsumer, ServiceObject,
    call_router::{lost, unreachable},
    delivery_owner::DeliveryObject,
    service_state::ServiceState,
};
use durable_steps::{AtomicStore, CallFailure, CallRetryReason, StoreError};
use fleet_sync_delivery::{
    DeliveryError, PublicationTarget, PublishCall, PublishStatus, StatusStream,
};
use fleet_sync_protocol::*;
use fleet_sync_session::PeerAuthenticator;
use runtime_primitives::{Clock, WakeScheduler};
use serde_json::Value;
use sqlx_turso::Turso;

impl<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
> ServiceState<P, C, A>
{
    pub(crate) async fn delivery_publication(
        &self,
        target: Value,
        request: Value,
        call: CallId,
    ) -> Result<Value, CallFailure> {
        let target: PublishCall<P::Delivery> = serde_json::from_value(target).map_err(lost)?;
        let publication: StatusPublication<<P::Delivery as DeliveryTypes>::Acceptance> =
            serde_json::from_value(request).map_err(lost)?;
        let answer = match target.target {
            PublicationTarget::StatusStream { sender, route } => {
                let owner = self
                    .delivery_owner(DeliveryObject::Status(sender))
                    .await
                    .map_err(|_| unreachable())?;
                let now = owner.object.host().now();
                match owner.object.rule_transaction(move|scope|Box::pin(async move {StatusStream::<_,P::Delivery>::new(scope,sender,route).append_and_admit::<PublishStatus<ServiceObject,Turso,P::Delivery>>(&publication,now).await})).await {
                    Ok(_)=>{owner.object.host().wake_at(owner.object.host().now()).await.map_err(lost)?;StatusPublicationResult::Applied},
                    Err(DeliveryError::Refused{reason})=>StatusPublicationResult::Refused{reason},
                    Err(_)=>return Err(unreachable()),
                }
            }
            PublicationTarget::Peer { peer, .. } => {
                match self
                    .sessions()
                    .map_err(lost)?
                    .request_peer(
                        peer,
                        ServerRequest::Crate(ServerCall::PublishStatus { call, publication }),
                        self.options.call_timeout,
                    )
                    .await?
                {
                    ClientAnswer::Crate(ServerCallAnswer::Published(answer)) => answer,
                    _ => return Err(CallFailure::AnswerLost),
                }
            }
            PublicationTarget::Project { project, .. } => {
                // Resolve afresh on every attempt; the journal retains the
                // project target rather than a machine that may lose its lease.
                let unavailable = || CallFailure::Retryable {
                    reason: CallRetryReason {
                        code: "projectHolderUnavailable".into(),
                        detail: "project has no available current holder".into(),
                    },
                };
                let now = self.registry.host().now();
                let peer = self
                    .registry
                    .transaction(move |scope| {
                        Box::pin(async move {
                            Ok(fleet_sync_lease::lease_status(scope, project, now)
                                .await
                                .map_err(StoreError::backend)?
                                .filter(|lease| lease.state == LeaseState::Held)
                                .map(|lease| lease.holder))
                        })
                    })
                    .await
                    .map_err(|_| unavailable())?
                    .ok_or_else(unavailable)?;
                match self
                    .sessions()
                    .map_err(lost)?
                    .request_peer(
                        peer,
                        ServerRequest::Crate(ServerCall::PublishStatus { call, publication }),
                        self.options.call_timeout,
                    )
                    .await?
                {
                    ClientAnswer::Crate(ServerCallAnswer::Published(answer)) => answer,
                    _ => return Err(CallFailure::AnswerLost),
                }
            }
        };
        serde_json::to_value(answer).map_err(lost)
    }
}
