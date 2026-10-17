use super::*;
use durable_steps::{Engine, StoreError};
use serde_json::Value;
use std::{
    collections::VecDeque,
    sync::{Mutex, Weak, atomic::AtomicBool},
};

pub type SubmitTask = SubmitItem<TestStore, sqlx_turso::Turso, TestTypes, TestConsumer>;
pub type ReceiveTask = ReceiveItems<TestStore, sqlx_turso::Turso, TestTypes, TestConsumer>;
pub type DeliverTask = DeliverLocally<TestStore, sqlx_turso::Turso, TestTypes, TestConsumer>;
pub type ReportTask = ReportOutcome<TestStore, sqlx_turso::Turso, TestTypes>;
pub type PublishTask = PublishStatus<TestStore, sqlx_turso::Turso, TestTypes>;
pub struct FetchBarrier {
    pub reached: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}
pub struct TestNetwork {
    pub sender: Weak<TestStore>,
    pub mailbox: Weak<TestStore>,
    pub recipient: Mutex<Weak<TestStore>>,
    pub status: Weak<TestStore>,
    pub relay_up: AtomicBool,
    pub lose_submit: AtomicBool,
    pub lose_fetch: AtomicBool,
    pub fetch_barrier: Mutex<Option<FetchBarrier>>,
    pub invalid_fetch: AtomicBool,
    pub lose_receipt: AtomicBool,
    pub lose_delivery: AtomicBool,
    pub lose_report: AtomicBool,
    pub lose_publication: AtomicBool,
    pub sent_bodies: AtomicU64,
    pub settlements: AtomicU64,
    pub local_calls: AtomicU64,
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
    pub async fn dispatch(
        &self,
        kind: &str,
        target: Value,
        request: Value,
        _call: CallId,
        now: SyncTime,
    ) -> Result<Value, CallFailure> {
        if kind != DeliverCall::<TestTypes>::KIND && !self.relay_up.load(Ordering::SeqCst) {
            return Err(unreachable());
        }
        let result: Result<Value, StoreError> = match kind {
            SubmitCall::<TestTypes>::KIND => {
                let target: SubmitCall<TestTypes> = serde_json::from_value(target).unwrap();
                let reference: DeliveryItemRef = serde_json::from_value(request).unwrap();
                let resolved = self
                    .sender
                    .upgrade()
                    .unwrap()
                    .transaction({
                        let reference = reference.clone();
                        move |scope| {
                            Box::pin(async move {
                                Ok(Outbox::<_, TestTypes>::new(scope, limits())
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
                            .mailbox
                            .upgrade()
                            .unwrap()
                            .transaction(move |scope| {
                                Box::pin(async move {
                                    let answer =
                                        Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                                            .submit(peer(1), resolved.item, now)
                                            .await
                                            .map_err(storage_error)?;
                                    admit_publications::<_, TestTypes, PublishTask>(scope)
                                        .await
                                        .map_err(storage_error)?;
                                    Ok(answer)
                                })
                            })
                            .await
                            .unwrap();
                        if self.lose_submit.swap(false, Ordering::SeqCst) {
                            return Err(CallFailure::AnswerLost);
                        }
                        DispatchOutcome::Answered(answer)
                    }
                    resolved => {
                        self.settlements.fetch_add(1, Ordering::SeqCst);
                        if let Err(DeliveryError::Storage(_)) = resolved {
                            return Err(unreachable());
                        }
                        let settled = self
                            .mailbox
                            .upgrade()
                            .unwrap()
                            .transaction(move |scope| {
                                Box::pin(async move {
                                    Mailbox::<_, TestTypes>::new(scope, target.mailbox, limits())
                                        .settle(reference.item, reference.digest)
                                        .await
                                        .map_err(storage_error)
                                })
                            })
                            .await
                            .unwrap();
                        submit_settlement_outcome(settled)
                    }
                };
                Ok(value(answer))
            }
            FetchCall::<TestTypes>::KIND => {
                let request: FetchInput = serde_json::from_value(request).unwrap();
                let mut answer = self
                    .mailbox
                    .upgrade()
                    .unwrap()
                    .transaction(move |scope| {
                        Box::pin(async move {
                            let answer = Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                                .fetch(
                                    peer(2),
                                    request.stream,
                                    request.after,
                                    request.max_items,
                                    request.max_bytes,
                                    now,
                                )
                                .await
                                .map_err(storage_error)?;
                            admit_publications::<_, TestTypes, PublishTask>(scope)
                                .await
                                .map_err(storage_error)?;
                            Ok(answer)
                        })
                    })
                    .await
                    .unwrap();
                let barrier = self.fetch_barrier.lock().unwrap().take();
                if let Some(barrier) = barrier {
                    barrier.reached.notify_one();
                    barrier.release.notified().await;
                }
                if self.invalid_fetch.swap(false, Ordering::SeqCst) {
                    answer.stream = DeliveryStream::Controls;
                }
                if self.lose_fetch.swap(false, Ordering::SeqCst) {
                    return Err(CallFailure::AnswerLost);
                }
                Ok(value(answer))
            }
            ReceivedCall::KIND => {
                let request: ReceiptInput = serde_json::from_value(request).unwrap();
                let answer = self
                    .mailbox
                    .upgrade()
                    .unwrap()
                    .transaction(move |scope| {
                        Box::pin(async move {
                            let answer = Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                                .received(peer(2), request.stream, request.through, now)
                                .await
                                .map_err(storage_error)?;
                            admit_publications::<_, TestTypes, PublishTask>(scope)
                                .await
                                .map_err(storage_error)?;
                            Ok(answer)
                        })
                    })
                    .await
                    .unwrap();
                if self.lose_receipt.swap(false, Ordering::SeqCst) {
                    return Err(CallFailure::AnswerLost);
                }
                Ok(value(answer))
            }
            DeliverCall::<TestTypes>::KIND => {
                let reference: DeliveryItemRef = serde_json::from_value(request).unwrap();
                let recipient = self.recipient.lock().unwrap().upgrade().unwrap();
                let resolved = recipient
                    .transaction(move |scope| {
                        Box::pin(async move {
                            Ok(Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                                .resolve_delivery(&reference)
                                .await)
                        })
                    })
                    .await
                    .unwrap();
                let answer = match resolved {
                    Ok(item) => {
                        self.local_calls.fetch_add(1, Ordering::SeqCst);
                        let answer = self
                            .local_answers
                            .lock()
                            .unwrap()
                            .pop_front()
                            .unwrap_or_else(|| LocalOutcome::Delivered {
                                acceptance: Acceptance {
                                    receipt: item.body().clone(),
                                },
                            });
                        if self.lose_delivery.swap(false, Ordering::SeqCst) {
                            return Err(CallFailure::AnswerLost);
                        }
                        DeliverCall::<TestTypes>::validated_answer(answer)
                    }
                    Err(error) => DispatchOutcome::Unresolvable {
                        reason: error.failure_kind(),
                    },
                };
                Ok(value(answer))
            }
            ReportCall::<TestTypes>::KIND => {
                let report: RecipientReport<TestTypes> = serde_json::from_value(request).unwrap();
                let answer = self
                    .mailbox
                    .upgrade()
                    .unwrap()
                    .transaction(move |scope| {
                        Box::pin(async move {
                            let answer = Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                                .report(peer(2), report)
                                .await
                                .map_err(storage_error)?;
                            admit_publications::<_, TestTypes, PublishTask>(scope)
                                .await
                                .map_err(storage_error)?;
                            Ok(answer)
                        })
                    })
                    .await
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
                match target.target {
                    PublicationTarget::StatusStream { sender } => self
                        .status
                        .upgrade()
                        .unwrap()
                        .transaction(move |scope| {
                            Box::pin(async move {
                                StatusStream::<_, TestTypes>::new(scope, sender)
                                    .append_and_admit::<PublishTask>(&publication)
                                    .await
                                    .map_err(storage_error)?;
                                Ok(())
                            })
                        })
                        .await
                        .unwrap(),
                    PublicationTarget::Peer { .. } => self
                        .sender
                        .upgrade()
                        .unwrap()
                        .transaction(move |scope| {
                            Box::pin(async move {
                                Outbox::<_, TestTypes>::new(scope, limits())
                                    .publish_status(publication)
                                    .await
                                    .map_err(storage_error)
                            })
                        })
                        .await
                        .unwrap(),
                }
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
    pub mailbox: Fixture,
    pub recipient: Fixture,
    pub status: Fixture,
    pub routes: Arc<TestNetwork>,
}
impl NetworkFixture {
    pub async fn new() -> Self {
        let sender = Fixture::new().await;
        let mailbox = Fixture::new().await;
        let recipient = Fixture::new().await;
        let status = Fixture::new().await;
        let routes = Arc::new(TestNetwork {
            sender: Arc::downgrade(&sender.store),
            mailbox: Arc::downgrade(&mailbox.store),
            recipient: Mutex::new(Arc::downgrade(&recipient.store)),
            status: Arc::downgrade(&status.store),
            relay_up: AtomicBool::new(true),
            lose_submit: AtomicBool::new(false),
            lose_fetch: AtomicBool::new(false),
            fetch_barrier: Mutex::new(None),
            invalid_fetch: AtomicBool::new(false),
            lose_receipt: AtomicBool::new(false),
            lose_delivery: AtomicBool::new(false),
            lose_report: AtomicBool::new(false),
            lose_publication: AtomicBool::new(false),
            sent_bodies: AtomicU64::new(0),
            settlements: AtomicU64::new(0),
            local_calls: AtomicU64::new(0),
            defer_submit: AtomicBool::new(false),
            deferred: Mutex::new(None),
            local_answers: Mutex::new(VecDeque::new()),
        });
        for fixture in [&sender, &mailbox, &recipient, &status] {
            assert!(fixture.host.routes.set(routes.clone()).is_ok());
        }
        Self {
            sender,
            mailbox,
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
            .register(SubmitTask::new(limits(), consumer.clone()))
            .unwrap();
        engine
            .register(ReceiveTask::new(config, consumer.clone()))
            .unwrap();
        engine
            .register(DeliverTask::new(config, consumer, fixture.host.clone()))
            .unwrap();
        engine.register(ReportTask::new(config)).unwrap();
        engine.register(PublishTask::new()).unwrap();
        engine
    }
    pub fn advance(&self) {
        for fixture in [&self.sender, &self.mailbox, &self.recipient, &self.status] {
            fixture.host.clock.fetch_add(100_000, Ordering::SeqCst);
        }
    }
    pub async fn enqueue(&self, id: u64, body: &str) {
        let body = body.to_owned();
        self.sender
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    let DeliveryEnvelope::Item(item) = item(id, &body) else {
                        return Err(StoreError::backend("fixture item"));
                    };
                    Outbox::<_, TestTypes>::new(scope, limits())
                        .enqueue_and_admit::<SubmitTask>(item, peer(3))
                        .await
                        .map_err(storage_error)?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
    pub async fn receive(&self, stream: DeliveryStream) {
        self.recipient
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .begin_receive_and_admit::<ReceiveTask>(stream)
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
        owner: peer(2),
        relay: peer(3),
        limits: limits(),
        fetch_items: 10,
        fetch_bytes: 100_000,
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
                Ok(Outbox::<_, TestTypes>::new(scope, limits())
                    .status(item_id(id))
                    .await
                    .map_err(storage_error)?
                    .state)
            })
        })
        .await
        .unwrap()
}
