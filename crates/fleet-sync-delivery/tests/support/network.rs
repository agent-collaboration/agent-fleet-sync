use super::*;
use durable_steps::{Engine, StoreError};
use serde_json::Value;
use std::{
    collections::VecDeque,
    sync::{Mutex, Weak, atomic::AtomicBool},
};

pub type SubmitTask =
    SubmitItem<TestStore, sqlx_turso::Turso, TestTypes, TestConsumer, FixtureBindings>;
pub type DeliverTask =
    DeliverLocally<TestStore, sqlx_turso::Turso, TestTypes, TestConsumer, FixtureBindings>;
pub type ReportTask = ReportOutcome<TestStore, sqlx_turso::Turso, TestTypes, FixtureBindings>;
pub type PushTask = PushItem<TestStore, sqlx_turso::Turso, TestTypes, PublishTask, FixtureBindings>;
pub type PublishTask = PublishStatus<TestStore, sqlx_turso::Turso, TestTypes>;
pub struct PushBarrier {
    pub reached: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}
pub struct TestNetwork {
    pub sender: Mutex<Weak<TestStore>>,
    pub relay: Mutex<Weak<TestStore>>,
    pub recipient: Mutex<Weak<TestStore>>,
    pub publications: super::publication_network::PublicationRoutes,
    pub authenticated_submitter: Mutex<Submitter>,
    pub binding_refresh: Mutex<Option<Arc<tokio::sync::Notify>>>,
    pub push_barrier: Mutex<Option<PushBarrier>>,
    pub relay_up: AtomicBool,
    pub lose_submit: AtomicBool,
    pub lose_receipt: AtomicBool,
    pub lose_delivery: AtomicBool,
    pub lose_report: AtomicBool,
    pub lose_publication: AtomicBool,
    pub sent_bodies: AtomicU64,
    pub push_bodies: AtomicU64,
    pub settlements: AtomicU64,
    pub local_calls: AtomicU64,
    pub report_calls: AtomicU64,
    pub defer_submit: AtomicBool,
    pub deferred: Mutex<Option<ResolvedSubmit<TestTypes>>>,
    pub local_answers: Mutex<VecDeque<LocalOutcome<Acceptance>>>,
}
fn value<T: Serialize>(answer: T) -> Value {
    serde_json::to_value(answer).unwrap()
}
fn unreachable() -> CallFailure {
    CallFailure::NotSent {
        reason: NotSentReason::Unreachable { last_seen: None },
    }
}
impl TestNetwork {
    fn sender_store(&self) -> Arc<TestStore> {
        self.sender.lock().unwrap().upgrade().unwrap()
    }
    fn relay_store(&self) -> Arc<TestStore> {
        self.relay.lock().unwrap().upgrade().unwrap()
    }
    pub async fn dispatch(
        &self,
        kind: &str,
        target: Value,
        request: Value,
        call: CallId,
        now: SyncTime,
    ) -> Result<Value, CallFailure> {
        if kind != DeliverCall::<TestTypes>::KIND && !self.relay_up.load(Ordering::SeqCst) {
            return Err(unreachable());
        }
        let result: Result<Value, StoreError> = match kind {
            SubmitCall::<TestTypes>::KIND => {
                let target: SubmitCall<TestTypes> = serde_json::from_value(target).unwrap();
                let reference: DeliveryItemRef = serde_json::from_value(request).unwrap();
                let authenticated_submitter = *self.authenticated_submitter.lock().unwrap();
                let resolved = self
                    .sender_store()
                    .transaction({
                        let reference = reference.clone();
                        move |scope| {
                            Box::pin(async move {
                                Ok(Outbox::<_, TestTypes, _>::new(
                                    scope,
                                    limits(),
                                    &FixtureBindings,
                                )
                                .resolve_submit(&reference)
                                .await)
                            })
                        }
                    })
                    .await
                    .unwrap();
                let answer = match resolved {
                    Ok(resolved)
                        if resolved.relay == target.relay
                            && resolved.item.recipient().peer == target.mailbox =>
                    {
                        self.sent_bodies.fetch_add(1, Ordering::SeqCst);
                        if self.defer_submit.swap(false, Ordering::SeqCst) {
                            *self.deferred.lock().unwrap() = Some(resolved);
                            return Err(CallFailure::AnswerLost);
                        }
                        let answer = self
                            .relay_store()
                            .transaction(move |scope| {
                                Box::pin(async move {
                                    Outbox::<_, TestTypes, _>::new(
                                        scope,
                                        limits(),
                                        &FixtureBindings,
                                    )
                                    .submit_relay::<PushTask, RecordRemoval>(
                                        authenticated_submitter,
                                        resolved.item,
                                        peer(3),
                                        "fixture-route".into(),
                                        now,
                                    )
                                    .await
                                    .map_err(storage_error)
                                })
                            })
                            .await
                            .unwrap();
                        if self.lose_submit.swap(false, Ordering::SeqCst) {
                            return Err(CallFailure::AnswerLost);
                        }
                        DispatchOutcome::Answered(answer)
                    }
                    Err(DeliveryError::SenderBindingUnavailable { reason, .. }) => {
                        return Err(CallFailure::Retryable {
                            reason: durable_steps::CallRetryReason {
                                code: reason.code().into(),
                                detail: "consumer sender binding is not established".into(),
                            },
                        });
                    }
                    resolved => {
                        self.settlements.fetch_add(1, Ordering::SeqCst);
                        if let Err(DeliveryError::Storage(_)) = resolved {
                            return Err(unreachable());
                        }
                        let settled = self
                            .relay_store()
                            .transaction({
                                move |scope| {
                                    Box::pin(async move {
                                        Outbox::<_, TestTypes, _>::new(
                                            scope,
                                            limits(),
                                            &FixtureBindings,
                                        )
                                        .relay_settle::<RecordRemoval>(
                                            authenticated_submitter,
                                            reference.item,
                                            reference.digest,
                                            now,
                                        )
                                        .await
                                        .map_err(storage_error)
                                    })
                                }
                            })
                            .await
                            .unwrap();
                        submit_settlement_outcome(settled)
                    }
                };
                Ok(value(SubmitCall::<TestTypes>::validated_answer(answer)?))
            }
            PushCall::<TestTypes>::KIND => {
                let target: PushCall<TestTypes> = serde_json::from_value(target).unwrap();
                let reference: DeliveryItemRef = serde_json::from_value(request).unwrap();
                let envelope = self
                    .relay_store()
                    .transaction(move |scope| {
                        Box::pin(async move {
                            Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                                .resolve_push(&reference, now)
                                .await
                                .map_err(storage_error)
                        })
                    })
                    .await
                    .unwrap();
                let Some(envelope) = envelope else {
                    return Err(unreachable());
                };
                let recipient = self.recipient.lock().unwrap().upgrade().unwrap();
                self.push_bodies.fetch_add(1, Ordering::SeqCst);
                let answer = recipient
                    .transaction(move |scope| {
                        Box::pin(async move {
                            Inbox::<_, TestTypes, _>::new(
                                scope,
                                target.recipient,
                                limits(),
                                &FixtureBindings,
                            )
                            .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                                envelope.submitter,
                                envelope.item,
                                peer(3),
                                &TestConsumer,
                                now,
                            )
                            .await
                            .map_err(storage_error)
                        })
                    })
                    .await
                    .unwrap();
                // The host acts on Unknown after the custody transaction has
                // returned, while the injected clock is unchanged.
                if matches!(
                    answer,
                    PushResult::BindingUnavailable {
                        reason: SenderBindingFailure::Unknown
                    }
                ) && let Some(refresh) = self.binding_refresh.lock().unwrap().as_ref()
                {
                    refresh.notify_one();
                }
                let barrier = self.push_barrier.lock().unwrap().take();
                if let Some(barrier) = barrier {
                    barrier.reached.notify_one();
                    barrier.release.notified().await;
                }
                if self.lose_receipt.swap(false, Ordering::SeqCst) {
                    return Err(CallFailure::AnswerLost);
                }
                Ok(value(PushCall::<TestTypes>::validated_answer(answer)?))
            }

            DeliverCall::<TestTypes>::KIND => {
                let reference: DeliveryItemRef = serde_json::from_value(request).unwrap();
                let recipient = self.recipient.lock().unwrap().upgrade().unwrap();
                let resolved = recipient
                    .transaction(move |scope| {
                        Box::pin(async move {
                            Ok(Inbox::<_, TestTypes, _>::new(
                                scope,
                                peer(2),
                                limits(),
                                &FixtureBindings,
                            )
                            .resolve_local_delivery(&reference)
                            .await)
                        })
                    })
                    .await
                    .unwrap();
                let answer = match resolved {
                    Ok(resolved) => {
                        self.local_calls.fetch_add(1, Ordering::SeqCst);
                        let answer = self
                            .local_answers
                            .lock()
                            .unwrap()
                            .pop_front()
                            .unwrap_or_else(|| LocalOutcome::Delivered {
                                acceptance: Acceptance {
                                    receipt: resolved.item().body().clone(),
                                },
                            });
                        if self.lose_delivery.swap(false, Ordering::SeqCst) {
                            return Err(CallFailure::AnswerLost);
                        }
                        resolved.validated_answer(LocalDelivery {
                            outcome: answer,
                            observation: (),
                        })
                    }
                    Err(DeliveryError::SenderBindingUnavailable { reason, .. }) => {
                        return Err(CallFailure::Retryable {
                            reason: durable_steps::CallRetryReason {
                                code: reason.code().into(),
                                detail: "consumer sender binding is not established".into(),
                            },
                        });
                    }
                    Err(error) => DispatchOutcome::Unresolvable {
                        reason: error.failure_kind(),
                    },
                };
                Ok(value(answer))
            }
            ReportCall::<TestTypes>::KIND => {
                let report: RecipientReport<TestTypes> = serde_json::from_value(request).unwrap();
                self.report_calls.fetch_add(1, Ordering::SeqCst);
                let answer = self
                    .relay_store()
                    .transaction(move |scope| {
                        Box::pin(async move {
                            Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                                .recipient_report::<PublishTask>(peer(2), report, now)
                                .await
                                .map_err(|error| match error {
                                    DeliveryError::InvalidBatch => DeliveryError::Refused {
                                        reason: DeliveryRefusal::ProcessingFailed {
                                            kind: DeliveryFailureKind::ExecutionFailed,
                                        },
                                    },
                                    error => error,
                                })
                                .map_err(delivery_rollback)
                        })
                    })
                    .await;
                let answer = match answer {
                    Ok(answer) => Ok(answer),
                    Err(error) => match classify_delivery_store_error(error) {
                        DeliveryTransactionFailure::Refused(reason) => {
                            Ok(ReportResult::Refused { reason })
                        }
                        failure => Err(failure),
                    },
                }
                .unwrap();
                if self.lose_report.swap(false, Ordering::SeqCst) {
                    return Err(CallFailure::AnswerLost);
                }
                Ok(value(answer))
            }
            PublishCall::<TestTypes>::KIND => {
                let target: PublishCall<TestTypes> = serde_json::from_value(target).unwrap();
                let publication: StatusPublication<Acceptance> =
                    serde_json::from_value(request).unwrap();
                self.publications
                    .dispatch(target.target, publication, call, now)
                    .await?;
                if self.lose_publication.swap(false, Ordering::SeqCst) {
                    return Err(CallFailure::AnswerLost);
                }
                Ok(value(StatusPublicationResult::Applied))
            }
            _ => return Err(unreachable()),
        };
        Ok(result.unwrap())
    }
}
pub struct NetworkFixture {
    pub sender: Fixture,
    pub relay: Fixture,
    pub recipient: Fixture,
    pub status: Fixture,
    pub routes: Arc<TestNetwork>,
}
impl NetworkFixture {
    pub async fn new() -> Self {
        let sender = Fixture::new().await;
        let relay = Fixture::new().await;
        let recipient = Fixture::new().await;
        let status = Fixture::new().await;
        let routes = Arc::new(TestNetwork {
            sender: Mutex::new(Arc::downgrade(&sender.store)),
            relay: Mutex::new(Arc::downgrade(&relay.store)),
            recipient: Mutex::new(Arc::downgrade(&recipient.store)),
            publications: super::publication_network::PublicationRoutes::new(
                &sender.store,
                &status.store,
            ),
            authenticated_submitter: Mutex::new(Submitter::Machine(peer(1))),
            binding_refresh: Mutex::new(None),
            push_barrier: Mutex::new(None),
            relay_up: AtomicBool::new(true),
            lose_submit: AtomicBool::new(false),
            lose_receipt: AtomicBool::new(false),
            lose_delivery: AtomicBool::new(false),
            lose_report: AtomicBool::new(false),
            lose_publication: AtomicBool::new(false),
            sent_bodies: AtomicU64::new(0),
            push_bodies: AtomicU64::new(0),
            settlements: AtomicU64::new(0),
            local_calls: AtomicU64::new(0),
            report_calls: AtomicU64::new(0),
            defer_submit: AtomicBool::new(false),
            deferred: Mutex::new(None),
            local_answers: Mutex::new(VecDeque::new()),
        });
        for fixture in [&sender, &relay, &recipient, &status] {
            assert!(fixture.host.routes.set(routes.clone()).is_ok());
        }
        Self {
            sender,
            relay,
            recipient,
            status,
            routes,
        }
    }
    pub fn engine(&self, fixture: &Fixture) -> Engine<TestStore, TestHost> {
        self.engine_with_config(fixture, config())
    }
    pub fn engine_with_config(
        &self,
        fixture: &Fixture,
        config: DeliveryTaskConfig,
    ) -> Engine<TestStore, TestHost> {
        let mut engine = Engine::new(fixture.store.clone(), fixture.host.clone());
        let consumer = Arc::new(TestConsumer);
        engine
            .register(SubmitTask::new(
                limits(),
                consumer.clone(),
                fixture.host.clone(),
                std::sync::Arc::new(FixtureBindings),
            ))
            .unwrap();
        engine
            .register(DeliverTask::new(
                config.local,
                consumer,
                fixture.host.clone(),
                std::sync::Arc::new(FixtureBindings),
            ))
            .unwrap();
        engine
            .register(ReportTask::new(
                config,
                fixture.host.clone(),
                std::sync::Arc::new(FixtureBindings),
            ))
            .unwrap();
        engine.register(PublishTask::new()).unwrap();
        engine
            .register(PushTask::new(
                limits(),
                fixture.host.clone(),
                std::sync::Arc::new(FixtureBindings),
            ))
            .unwrap();
        engine.register(RecordRemoval::new()).unwrap();
        engine
    }
    pub fn advance(&self) {
        for fixture in [&self.sender, &self.relay, &self.recipient, &self.status] {
            fixture.host.clock.fetch_add(100_000, Ordering::SeqCst);
        }
    }
    pub async fn enqueue(&self, id: u64, body: &str) {
        let body = body.to_owned();
        let authenticated_submitter = *self.routes.authenticated_submitter.lock().unwrap();
        self.sender
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let DeliveryEnvelope::Item(item) = item(id, &body) else {
                        return Err(StoreError::backend("fixture item"));
                    };
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .enqueue_and_admit::<SubmitTask, RecordRemoval>(
                            item,
                            peer(3),
                            authenticated_submitter,
                            SyncTime::from_unix_millis(0),
                        )
                        .await
                        .map_err(storage_error)?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
}
pub fn config() -> DeliveryTaskConfig {
    DeliveryTaskConfig {
        local: LocalDeliveryConfig {
            owner: peer(2),
            limits: limits(),
        },
        relay: peer(3),
    }
}

pub async fn drain(engine: &Engine<TestStore, TestHost>) {
    let mut report = durable_steps::DriveReport::default();
    for _ in 0..20 {
        report = engine.drive_due().await.unwrap();
        assert!(report.failed.is_empty(), "{report:?}");
        assert!(report.retried.is_empty(), "{report:?}");
        if report.completed.is_empty() && report.yielded.is_empty() {
            return;
        }
    }
    assert!(
        report.completed.is_empty() && report.yielded.is_empty(),
        "fixture failed to reach an idle journal: {report:?}"
    );
}

pub async fn sender_status(network: &NetworkFixture, id: u64) -> OutboxStatusState<Acceptance> {
    network
        .sender
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                Ok(
                    Outbox::<_, TestTypes, _>::new(scope, limits(), &FixtureBindings)
                        .status(item_id(id))
                        .await
                        .map_err(storage_error)?
                        .state,
                )
            })
        })
        .await
        .unwrap()
}
