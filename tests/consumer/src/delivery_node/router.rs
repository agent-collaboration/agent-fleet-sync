use super::*;
use durable_steps::{CallFailure, CallTarget, NotSentReason};
use fleet_sync_tokio_runner::{CallEnvelope, CallRouter};

pub(super) struct NodeRouter(pub OnceLock<Weak<NodeState>>);
fn unreachable() -> CallFailure {
    CallFailure::NotSent {
        reason: NotSentReason::Unreachable { last_seen: None },
    }
}
fn encoded(value: impl Serialize) -> Result<Value, CallFailure> {
    serde_json::to_value(value).map_err(|_| CallFailure::AnswerLost)
}
fn decoded<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, CallFailure> {
    serde_json::from_value(value).map_err(|_| unreachable())
}
impl CallRouter for NodeRouter {
    async fn dispatch(&self, envelope: CallEnvelope) -> Result<Value, CallFailure> {
        let state = self
            .0
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(unreachable)?;
        let config = state.config().map_err(|_| unreachable())?;
        let CallEnvelope {
            kind,
            target,
            request,
            call,
        } = envelope;
        match kind {
            SubmitCall::<SampleDelivery>::KIND => {
                let target: SubmitCall<SampleDelivery> = decoded(target)?;
                let reference: DeliveryItemRef = decoded(request)?;
                let lookup = reference.clone();
                let resolved = state
                    .object
                    .rule_transaction::<_, DeliveryError, _>(move |scope| {
                        Box::pin(async move {
                            Outbox::<_, SampleDelivery>::new(scope, config.limits)
                                .resolve_submit(&lookup)
                                .await
                        })
                    })
                    .await;
                match resolved {
                    Ok(resolved)
                        if resolved.relay == target.relay
                            && resolved.item.recipient().peer == target.mailbox =>
                    {
                        let answer = state
                            .link
                            .request(DeliveryRequest::Submit {
                                call,
                                item: resolved.item,
                            })
                            .await?;
                        let DeliveryAnswer::Submit(answer) = answer else {
                            return Err(CallFailure::AnswerLost);
                        };
                        encoded(DispatchOutcome::Answered(answer))
                    }
                    Err(DeliveryError::Storage(_)) => Err(unreachable()),
                    _ => {
                        let answer = state
                            .link
                            .request(DeliveryRequest::Settle {
                                call,
                                mailbox: target.mailbox,
                                item: reference.item,
                                digest: reference.digest,
                            })
                            .await?;
                        let DeliveryAnswer::Settle(answer) = answer else {
                            return Err(CallFailure::AnswerLost);
                        };
                        encoded(submit_settlement_outcome(answer))
                    }
                }
            }
            DeliverCall::<SampleDelivery>::KIND => {
                let reference: DeliveryItemRef = decoded(request)?;
                let resolved = state
                    .object
                    .rule_transaction::<_, DeliveryError, _>(move |scope| {
                        Box::pin(async move {
                            Inbox::<_, SampleDelivery>::new(scope, config.owner, config.limits)
                                .resolve_delivery(&reference)
                                .await
                        })
                    })
                    .await;
                match resolved {
                    Ok(item) => {
                        let consumer = consumer::NodeConsumer(Arc::downgrade(&state));
                        let outcome =
                            <consumer::NodeConsumer as DeliveryConsumer<
                                durable_steps::turso_store::TursoScope<'_, NodeHost>,
                            >>::deliver_locally(&consumer, &item, call)
                            .await;
                        let answer = DeliverCall::<SampleDelivery>::validated_answer(outcome);
                        if state.gate.after("local").await {
                            return Err(CallFailure::AnswerLost);
                        }
                        encoded(answer)
                    }
                    Err(DeliveryError::Storage(_)) => Err(unreachable()),
                    Err(error) => encoded(DispatchOutcome::<LocalOutcome<String>>::Unresolvable {
                        reason: error.failure_kind(),
                    }),
                }
            }
            FetchCall::<SampleDelivery>::KIND => {
                let target: FetchCall<SampleDelivery> = decoded(target)?;
                if target.mailbox != config.owner || target.relay != config.relay {
                    return Err(unreachable());
                }
                let request: FetchInput = decoded(request)?;
                let answer = state
                    .link
                    .request(DeliveryRequest::Fetch {
                        call,
                        stream: request.stream,
                        after: request.after,
                        max_items: request.max_items,
                        max_bytes: request.max_bytes,
                    })
                    .await?;
                match answer {
                    DeliveryAnswer::Fetch(answer) => encoded(answer),
                    _ => Err(CallFailure::AnswerLost),
                }
            }
            ReceivedCall::KIND => {
                let target: ReceivedCall = decoded(target)?;
                if target.mailbox != config.owner || target.relay != config.relay {
                    return Err(unreachable());
                }
                let request: ReceiptInput = decoded(request)?;
                let answer = state
                    .link
                    .request(DeliveryRequest::Received {
                        call,
                        stream: request.stream,
                        through: request.through,
                    })
                    .await?;
                match answer {
                    DeliveryAnswer::Received(answer) => encoded(answer),
                    _ => Err(CallFailure::AnswerLost),
                }
            }
            ReportCall::<SampleDelivery>::KIND => {
                let target: ReportCall<SampleDelivery> = decoded(target)?;
                if target.mailbox != config.owner || target.relay != config.relay {
                    return Err(unreachable());
                }
                let report = decoded(request)?;
                let answer = state
                    .link
                    .request(DeliveryRequest::Report { call, report })
                    .await?;
                match answer {
                    DeliveryAnswer::Report(answer) => encoded(answer),
                    _ => Err(CallFailure::AnswerLost),
                }
            }
            _ => Err(unreachable()),
        }
    }
}
