use crate::{
    ServiceConsumer, ServiceError, ServiceObject, delivery_owner::DeliveryObject,
    service_state::ServiceState,
};
use fleet_sync_delivery::{
    DeliveryError, Mailbox, PublishStatus, StatusStream, admit_publications,
};
use fleet_sync_protocol::*;
use fleet_sync_session::PeerAuthenticator;
use runtime_primitives::{Clock, WakeScheduler};
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
    pub(crate) async fn delivery_request(
        &self,
        peer: PeerId,
        route: <P::Delivery as DeliveryTypes>::StatusRoute,
        request: DeliveryRequest<P::Delivery>,
    ) -> Result<DeliveryAnswer<P::Delivery>, ServiceError> {
        let limits = self.delivery_limits()?;
        if let DeliveryRequest::StatusFetch { after, limit, .. } = request {
            let owner = self.delivery_owner(DeliveryObject::Status(peer)).await?;
            return owner
                .object
                .rule_transaction(move |scope| {
                    Box::pin(async move {
                        StatusStream::<_, P::Delivery>::new(scope, peer, route)
                            .fetch(peer, after, limit)
                            .await
                    })
                })
                .await
                .map(DeliveryAnswer::StatusFetch)
                .map_err(ServiceError::rule);
        }
        let mailbox = match &request {
            DeliveryRequest::Submit { item, .. } => item.recipient().peer,
            DeliveryRequest::Query { mailbox, .. }
            | DeliveryRequest::Retire { mailbox, .. }
            | DeliveryRequest::Settle { mailbox, .. } => *mailbox,
            _ => peer,
        };
        let owner = self
            .delivery_owner(DeliveryObject::Mailbox(mailbox))
            .await?;
        let now = owner.object.host().now();
        let submit = matches!(&request, DeliveryRequest::Submit { .. });
        let report = matches!(&request, DeliveryRequest::Report { .. });
        #[cfg(feature = "test-support")]
        let retirement = matches!(&request, DeliveryRequest::Retire { .. });
        if let Some(kind) = *owner.health.borrow() {
            return Ok(DeliveryAnswer::MailboxUnavailable {
                reason: DeliveryRefusal::ProcessingFailed { kind },
            });
        }
        owner.ensure_schema().await.map_err(|error| {
            tracing::warn!(?mailbox, phase="migration", %error, "delivery mailbox operation retry");
            ServiceError::from(error)
        })?;
        let result = owner
            .object
            .rule_transaction(move |scope| {
                Box::pin(async move {
                    let mut mailbox = Mailbox::<_, P::Delivery>::new(scope, mailbox, limits);
                    let answer = match request {
                        DeliveryRequest::Submit { item, .. } => DeliveryAnswer::Submit(
                            mailbox.submit(peer, item, now, route, None).await?,
                        ),
                        DeliveryRequest::Fetch {
                            stream,
                            after,
                            max_items,
                            max_bytes,
                            ..
                        } => DeliveryAnswer::Fetch(
                            mailbox
                                .fetch(peer, stream, after, max_items, max_bytes, now)
                                .await?,
                        ),
                        DeliveryRequest::Received {
                            stream, through, ..
                        } => DeliveryAnswer::Received(
                            mailbox.received(peer, stream, through, now).await?,
                        ),
                        DeliveryRequest::Report { report, .. } => {
                            DeliveryAnswer::Report(mailbox.report(peer, report).await?)
                        }
                        DeliveryRequest::Query { items, .. } => {
                            DeliveryAnswer::Query(mailbox.query(&items).await?)
                        }
                        DeliveryRequest::Retire { call, items, .. } => {
                            DeliveryAnswer::Retire(mailbox.retire(peer, &call, &items).await?)
                        }
                        DeliveryRequest::Settle { item, digest, .. } => {
                            DeliveryAnswer::Settle(mailbox.settle(item, digest).await?)
                        }
                        DeliveryRequest::StatusFetch { .. } => {
                            return Err(DeliveryError::InvalidBatch);
                        }
                    };
                    admit_publications::<
                        _,
                        P::Delivery,
                        PublishStatus<ServiceObject, Turso, P::Delivery>,
                    >(scope)
                    .await?;
                    Ok::<_, DeliveryError>(answer)
                })
            })
            .await;
        if result.is_ok() {
            owner.object.host().wake_at(owner.object.host().now()).await.map_err(|error| {
                tracing::warn!(?mailbox, phase="requestWake", %error, "delivery mailbox operation retry");
                ServiceError::rule(error)
            })?;
        }
        #[cfg(feature = "test-support")]
        if retirement && result.is_ok() {
            self.retirement_reply_probe.after_commit().await;
        }
        match result {
            Ok(answer) => Ok(answer),
            Err(DeliveryError::Refused { reason }) if submit => {
                Ok(DeliveryAnswer::Submit(SubmitResult::Refused { reason }))
            }
            Err(DeliveryError::Refused { reason }) if report => {
                Ok(DeliveryAnswer::Report(ReportResult::Refused { reason }))
            }
            Err(DeliveryError::Corrupt { field }) => {
                owner.mark_corrupt(mailbox, field);
                Ok(DeliveryAnswer::MailboxUnavailable {
                    reason: DeliveryRefusal::ProcessingFailed {
                        kind: DeliveryFailureKind::CustodyCorrupt,
                    },
                })
            }
            Err(error @ DeliveryError::Storage(_)) => {
                tracing::warn!(?mailbox, phase="request", %error, "delivery mailbox operation retry");
                Err(ServiceError::rule(error))
            }
            Err(error) => Err(ServiceError::rule(error)),
        }
    }
}
