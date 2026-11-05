use crate::{ServiceConsumer, ServiceError, delivery_relay::*, service_state::ServiceState};
use durable_steps::AtomicStore;
use fleet_sync_delivery::{
    DeliveryTransactionFailure, Outbox, StatusStream, classify_delivery_store_error,
    delivery_rollback,
};
use fleet_sync_protocol::*;
use fleet_sync_session::PeerAuthenticator;
use runtime_primitives::{Clock, WakeScheduler};

impl<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<Material = String, Route = <P::Delivery as DeliveryTypes>::StatusRoute>,
> ServiceState<P, C, A>
{
    pub(crate) async fn delivery_request(
        &self,
        peer: PeerId,
        route: <P::Delivery as DeliveryTypes>::StatusRoute,
        request: DeliveryRequest<P::Delivery>,
    ) -> Result<DeliveryAnswer<P::Delivery>, ServiceError> {
        let config = self
            .options
            .delivery
            .as_ref()
            .ok_or_else(|| ServiceError::rule("delivery is not installed"))?;
        if let DeliveryRequest::Settle { submitter, .. } = &request
            && *submitter != Submitter::Machine(peer)
        {
            return Ok(DeliveryAnswer::DeliveryUnavailable {
                reason: DeliveryRefusal::NotSubmitter,
            });
        }
        let submitting = matches!(request, DeliveryRequest::Submit { .. });
        let reporting = matches!(request, DeliveryRequest::Report { .. });
        let stream_request = matches!(
            request,
            DeliveryRequest::StatusFetch { .. } | DeliveryRequest::StatusAck { .. }
        );
        let now = self.registry.host().now();
        let limits = config.limits;
        let relay = config.relay;
        let binding = self.consumer.clone();
        let result = self
            .registry
            .transaction(move |scope| {
                Box::pin(async move {
                    let answer = async {
                        let owner = Submitter::Machine(peer);
                        match request {
                            DeliveryRequest::Submit { item, .. } => Ok(DeliveryAnswer::Submit(
                                Outbox::<_, P::Delivery, _>::new(scope, limits, binding.as_ref())
                                    .submit_relay::<RelayPush<P::Delivery, C>, RelayRemoval>(
                                        owner, item, relay, route, now,
                                    )
                                    .await?,
                            )),
                            DeliveryRequest::Report { report, .. } => Ok(DeliveryAnswer::Report(
                                Outbox::<_, P::Delivery, _>::new(scope, limits, binding.as_ref())
                                    .recipient_report::<RelayPublication<P::Delivery>>(
                                        peer, report, now,
                                    )
                                    .await?,
                            )),
                            DeliveryRequest::Query { items, .. } => {
                                let mut statuses = Vec::with_capacity(items.len());
                                for item in items {
                                    statuses.push(
                                        Outbox::<_, P::Delivery, _>::new(
                                            scope,
                                            limits,
                                            binding.as_ref(),
                                        )
                                        .relay_query(owner, item)
                                        .await?,
                                    );
                                }
                                Ok(DeliveryAnswer::Query(QueryResult { statuses }))
                            }
                            DeliveryRequest::Settle {
                                submitter,
                                item,
                                digest,
                                ..
                            } => Ok(DeliveryAnswer::Settle(
                                Outbox::<_, P::Delivery, _>::new(scope, limits, binding.as_ref())
                                    .relay_settle::<RelayRemoval>(submitter, item, digest, now)
                                    .await?,
                            )),
                            DeliveryRequest::StatusFetch { after, limit, .. } => {
                                Ok(DeliveryAnswer::StatusFetch(
                                    StatusStream::<_, P::Delivery>::new(scope, owner, route)
                                        .fetch(owner, after, limit)
                                        .await?,
                                ))
                            }
                            DeliveryRequest::StatusAck { through, .. } => {
                                Ok(DeliveryAnswer::StatusAck(
                                    StatusStream::<_, P::Delivery>::new(scope, owner, route)
                                        .acknowledge(through, now)
                                        .await?,
                                ))
                            }
                        }
                    }
                    .await;
                    answer.map_err(delivery_rollback)
                })
            })
            .await;
        match result {
            Ok(answer) => {
                self.registry
                    .host()
                    .wake_at(self.registry.host().now())
                    .await
                    .map_err(ServiceError::rule)?;
                Ok(answer)
            }
            Err(error) if stream_request => Err(ServiceError::rule(error)),
            Err(error) => match classify_delivery_store_error(error) {
                DeliveryTransactionFailure::Refused(reason) if submitting => {
                    Ok(DeliveryAnswer::Submit(SubmitResult::Refused { reason }))
                }
                DeliveryTransactionFailure::Refused(reason) if reporting => {
                    Ok(DeliveryAnswer::Report(ReportResult::Refused { reason }))
                }
                DeliveryTransactionFailure::Refused(reason) => {
                    Ok(DeliveryAnswer::DeliveryUnavailable { reason })
                }
                DeliveryTransactionFailure::Corrupt { .. } => {
                    Ok(DeliveryAnswer::DeliveryUnavailable {
                        reason: DeliveryRefusal::ProcessingFailed {
                            kind: DeliveryFailureKind::CustodyCorrupt,
                        },
                    })
                }
                failure => {
                    tracing::warn!(?peer,phase="request",%failure,"delivery relay operation retry");
                    Err(ServiceError::rule(failure))
                }
            },
        }
    }
}
