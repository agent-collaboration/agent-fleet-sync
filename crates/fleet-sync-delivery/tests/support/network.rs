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
pub type PushTask = PushItem<TestStore, sqlx_turso::Turso, TestTypes, PublishTask>;
pub type PublishTask = PublishStatus<TestStore, sqlx_turso::Turso, TestTypes>;
pub struct FetchBarrier {
    pub reached: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}
pub struct TestNetwork {
    pub sender: Mutex<Weak<TestStore>>,
    pub mailbox: Mutex<Weak<TestStore>>,
    pub recipient: Mutex<Weak<TestStore>>,
    pub publications: super::publication_network::PublicationRoutes,
    pub fleet_push: AtomicBool,
    pub push_barrier: Mutex<Option<FetchBarrier>>,
    pub relay_up: AtomicBool,
    pub lose_submit: AtomicBool,
    pub lose_fetch: AtomicBool,
    pub fetch_barrier: Mutex<Option<FetchBarrier>>,
    pub fetch_calls: Mutex<Vec<(CallId, FetchInput)>>,
    pub invalid_fetch: AtomicBool,
    pub unavailable_fetch: AtomicBool,
    pub unavailable_receipt: AtomicBool,
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
    fn sender_store(&self) -> Arc<TestStore> {
        self.sender.lock().unwrap().upgrade().unwrap()
    }
    fn mailbox_store(&self) -> Arc<TestStore> {
        self.mailbox.lock().unwrap().upgrade().unwrap()
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
                let resolved = self
                    .sender_store()
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
                        let fleet_push = self.fleet_push.load(Ordering::SeqCst);
                        let answer = self
                            .mailbox_store()
                            .transaction(move |scope| {
                                Box::pin(async move {
                                    if fleet_push {
                                        return Outbox::<_, TestTypes>::new(scope, limits())
                                            .submit_relay::<PushTask, RecordRemoval>(
                                                peer(1),
                                                resolved.item,
                                                peer(3),
                                                "fixture-route".into(),
                                                now,
                                            )
                                            .await
                                            .map_err(storage_error);
                                    }
                                    let answer =
                                        Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                                            .submit(
                                                peer(1),
                                                resolved.item,
                                                now,
                                                "fixture-route".to_owned(),
                                                None,
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
                            .mailbox_store()
                            .transaction({
                                let fleet_push = self.fleet_push.load(Ordering::SeqCst);
                                move |scope| {
                                    Box::pin(async move {
                                        if fleet_push {
                                            return Outbox::<_, TestTypes>::new(scope, limits())
                                                .relay_settle::<RecordRemoval>(
                                                    peer(1),
                                                    reference.item,
                                                    reference.digest,
                                                    now,
                                                )
                                                .await
                                                .map_err(storage_error);
                                        }
                                        Mailbox::<_, TestTypes>::new(
                                            scope,
                                            target.mailbox,
                                            limits(),
                                        )
                                        .settle(reference.item, reference.digest)
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
                Ok(value(answer))
            }
            PushCall::<TestTypes>::KIND => {
                let target: PushCall<TestTypes> = serde_json::from_value(target).unwrap();
                let reference: DeliveryItemRef = serde_json::from_value(request).unwrap();
                let envelope = self
                    .mailbox_store()
                    .transaction(move |scope| {
                        Box::pin(async move {
                            Outbox::<_, TestTypes>::new(scope, limits())
                                .resolve_push(&reference)
                                .await
                                .map_err(storage_error)
                        })
                    })
                    .await
                    .unwrap()
                    .unwrap();
                let recipient = self.recipient.lock().unwrap().upgrade().unwrap();
                let answer = recipient
                    .transaction(move |scope| {
                        Box::pin(async move {
                            Inbox::<_, TestTypes>::new(scope, target.recipient, limits())
                                .accept_push::<DeliverTask, ReportTask, RecordRemoval, _>(
                                    envelope,
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
            FetchCall::<TestTypes>::KIND => {
                if self.unavailable_fetch.load(Ordering::SeqCst) {
                    return Err(CallFailure::AnswerLost);
                }
                let request: FetchInput = serde_json::from_value(request).unwrap();
                self.fetch_calls
                    .lock()
                    .unwrap()
                    .push((call.clone(), request));
                let mut answer = self
                    .mailbox_store()
                    .transaction(move |scope| {
                        Box::pin(async move {
                            let answer = Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                                .fetch_with_budget(peer(2), request, now)
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
                if self.unavailable_receipt.load(Ordering::SeqCst) {
                    return Err(CallFailure::AnswerLost);
                }
                let request: ReceiptInput = serde_json::from_value(request).unwrap();
                let answer = self
                    .mailbox_store()
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
                    Err(error) => DispatchOutcome::Unresolvable {
                        reason: error.failure_kind(),
                    },
                };
                Ok(value(answer))
            }
            ReportCall::<TestTypes>::KIND => {
                let report: RecipientReport<TestTypes> = serde_json::from_value(request).unwrap();
                let fleet_push = self.fleet_push.load(Ordering::SeqCst);
                let answer = self
                    .mailbox_store()
                    .transaction(move |scope| {
                        Box::pin(async move {
                            if fleet_push {
                                return Outbox::<_, TestTypes>::new(scope, limits())
                                    .recipient_report::<PublishTask>(peer(2), report, now)
                                    .await
                                    .map_err(storage_error);
                            }
                            let answer = Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                                .report(peer(2), report, now)
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
                self.publications
                    .dispatch(target.target, publication, call)
                    .await;
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
            sender: Mutex::new(Arc::downgrade(&sender.store)),
            mailbox: Mutex::new(Arc::downgrade(&mailbox.store)),
            recipient: Mutex::new(Arc::downgrade(&recipient.store)),
            publications: super::publication_network::PublicationRoutes::new(
                &sender.store,
                &status.store,
            ),
            fleet_push: AtomicBool::new(false),
            push_barrier: Mutex::new(None),
            relay_up: AtomicBool::new(true),
            lose_submit: AtomicBool::new(false),
            lose_fetch: AtomicBool::new(false),
            fetch_barrier: Mutex::new(None),
            fetch_calls: Mutex::new(Vec::new()),
            invalid_fetch: AtomicBool::new(false),
            unavailable_fetch: AtomicBool::new(false),
            unavailable_receipt: AtomicBool::new(false),
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
            .register(SubmitTask::new(
                limits(),
                consumer.clone(),
                fixture.host.clone(),
            ))
            .unwrap();
        engine
            .register(ReceiveTask::new(
                config,
                consumer.clone(),
                fixture.host.clone(),
            ))
            .unwrap();
        engine
            .register(DeliverTask::new(
                config.local,
                consumer,
                fixture.host.clone(),
            ))
            .unwrap();
        engine
            .register(ReportTask::new(config, fixture.host.clone()))
            .unwrap();
        engine.register(PublishTask::new()).unwrap();
        engine
            .register(PushTask::new(limits(), fixture.host.clone()))
            .unwrap();
        engine.register(RecordRemoval::new()).unwrap();
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
                        .enqueue_and_admit::<SubmitTask, RecordRemoval>(
                            item,
                            peer(3),
                            peer(1),
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
    pub async fn receive(&self, stream: DeliveryStream) {
        self.recipient
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    Inbox::<_, TestTypes>::new(scope, peer(2), limits())
                        .begin_receive_and_admit::<ReceiveTask>(
                            stream,
                            fleet_sync_protocol::SyncTime::from_unix_millis(0),
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
