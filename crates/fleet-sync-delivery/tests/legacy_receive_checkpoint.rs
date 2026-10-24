#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{AtomicStore, Engine, OperationContext, OperationFailure, StepName, Task};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use std::{sync::Arc, time::Duration};
use support::{network::*, *};

struct OptedTypes;
impl DeliveryTypes for OptedTypes {
    const MAX_SENDER_BYTES: u32 = TestTypes::MAX_SENDER_BYTES;
    const MAX_ADDRESS_BYTES: u32 = TestTypes::MAX_ADDRESS_BYTES;
    const MAX_ACCEPTANCE_BYTES: u32 = TestTypes::MAX_ACCEPTANCE_BYTES;
    const MAX_LOCAL_OBSERVATION_BYTES: u32 = TestTypes::MAX_LOCAL_OBSERVATION_BYTES;
    const MAX_STATUS_ROUTE_BYTES: u32 = TestTypes::MAX_STATUS_ROUTE_BYTES;
    const PER_ITEM_SETTLEMENT: bool = true;
    type Body = String;
    type Sender = String;
    type Address = String;
    type Acceptance = Acceptance;
    type LocalObservation = ();
    type StatusRoute = String;
}
struct OptedProtocol;
impl ConsumerProtocol for OptedProtocol {
    type Delivery = OptedTypes;
    type Request = ();
    type Answer = ();
    type ServerRequest = ();
    type ClientAnswer = ();
    type Notice = ();
    type Annotation = ();
}
struct OptedConsumer;
impl<S: durable_steps::storage::TransactionScope> DeliveryConsumer<S> for OptedConsumer {
    type Protocol = OptedProtocol;
    async fn release_gate(
        &self,
        _: &mut S,
        _: &DeliveryItem<OptedTypes>,
    ) -> Result<GateDecision, DeliveryError> {
        Ok(GateDecision::Release)
    }
    async fn deliver_locally(
        &self,
        item: &DeliveryItem<OptedTypes>,
        _: durable_steps::CallId,
    ) -> LocalDelivery<OptedTypes> {
        LocalDelivery {
            outcome: LocalOutcome::Delivered {
                acceptance: Acceptance {
                    receipt: item.body().clone(),
                },
            },
            observation: (),
        }
    }
    fn may_cancel(&self, sender: &String, item: &DeliveryHeader<OptedTypes>) -> bool {
        sender == item.sender()
    }
}
fn resumed_engine(network: &NetworkFixture, opted_in: bool) -> Engine<TestStore, TestHost> {
    if !opted_in {
        return network.engine(&network.recipient);
    }
    let mut engine = Engine::new(
        network.recipient.store.clone(),
        network.recipient.host.clone(),
    );
    engine
        .register(ReceiveItems::<
            TestStore,
            sqlx_turso::Turso,
            OptedTypes,
            OptedConsumer,
        >::new(config(), Arc::new(OptedConsumer)))
        .unwrap();
    engine
        .register(DeliverLocally::<
            TestStore,
            sqlx_turso::Turso,
            OptedTypes,
            OptedConsumer,
        >::new(
            config().local,
            Arc::new(OptedConsumer),
            network.recipient.host.clone(),
        ))
        .unwrap();
    engine
        .register(ReportOutcome::<TestStore, sqlx_turso::Turso, OptedTypes>::new(config()))
        .unwrap();
    engine
}

// The v0.2.0 receive prefix: same kind, version, positions and local tuple bytes.
// Stop at the real fetch boundary, after the budget has committed.
struct LegacyReceivePrefix;
impl Task for LegacyReceivePrefix {
    const KIND: &'static str = ReceiveTask::KIND;
    const VERSION: u32 = 1;
    const KEEP_RESULT: bool = false;
    type Store = TestStore;
    type Input = ReceiveInput;
    type Output = ();
    async fn run<O: OperationContext<Store = TestStore>>(
        &self,
        ctx: &mut O,
        input: &ReceiveInput,
    ) -> Result<(), OperationFailure> {
        let input = *input;
        let config = config();
        let (max_items, max_bytes) = ctx
            .local(StepName::new("budget").unwrap(), move |scope| {
                Box::pin(async move {
                    Inbox::<_, TestTypes>::new(scope, config.local.owner, config.local.limits)
                        .receive_budget(input, config.fetch_items, config.fetch_bytes)
                        .await
                        .map_err(|e| {
                            durable_steps::StepError::Failed(OperationFailure::InvalidPayload {
                                message: e.to_string(),
                            })
                        })
                })
            })
            .await?;
        ctx.call(
            StepName::new("fetch").unwrap(),
            FetchCall::<TestTypes>::new(config.relay, config.local.owner),
            FetchInput {
                stream: input.stream,
                after: input.after,
                max_items,
                max_bytes,
                inbox_room_bytes: None,
            },
        )
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn v020_budget_checkpoint_reopens_and_completes_with_identical_fetch_identity() {
    for (opted_in, legacy_prefix) in [(false, true), (true, true), (false, false), (true, false)] {
        let mut network = NetworkFixture::new().await;
        network.enqueue(1, "legacy continuation").await;
        drain(&network.engine(&network.sender)).await;
        network.receive(DeliveryStream::Items).await;
        let reached = Arc::new(tokio::sync::Notify::new());
        *network.routes.fetch_barrier.lock().unwrap() = Some(FetchBarrier {
            reached: reached.clone(),
            release: Arc::new(tokio::sync::Notify::new()),
        });
        let mut old_engine = Engine::new(
            network.recipient.store.clone(),
            network.recipient.host.clone(),
        );
        if legacy_prefix {
            old_engine.register(LegacyReceivePrefix).unwrap();
        } else if opted_in {
            old_engine
                .register(ReceiveItems::<
                    TestStore,
                    sqlx_turso::Turso,
                    OptedTypes,
                    OptedConsumer,
                >::new(config(), Arc::new(OptedConsumer)))
                .unwrap();
        } else {
            old_engine
                .register(ReceiveTask::new(config(), Arc::new(TestConsumer)))
                .unwrap();
        }
        let worker = tokio::spawn(async move { old_engine.drive_due().await });
        tokio::time::timeout(Duration::from_secs(10), reached.notified())
            .await
            .unwrap();
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        let before = network
            .recipient
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    use durable_steps::storage::TransactionScope;
                    let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                        "SELECT output FROM _fleet_sync_steps WHERE step_name='budget'",
                    )
                    .fetch_one(scope.executor())
                    .await?;
                    let output: Vec<u8> = row.try_get("output")?;
                    assert_eq!(
                        serde_json::from_slice::<serde_json::Value>(&output).unwrap(),
                        if legacy_prefix || !opted_in {serde_json::json!([1, 93_513])} else {serde_json::json!({"stream":"items","after":0,"maxItems":10,"maxBytes":100_000,"inboxRoomBytes":100_000})}
                    );
                    Ok(output)
                })
            })
            .await
            .unwrap();
        let path = network.recipient.directory.path().join("delivery.db");
        let replacement = Fixture::new().await;
        let old = std::mem::replace(&mut network.recipient.store, replacement.store.clone());
        let old_store = Arc::downgrade(&old);
        drop(old);
        assert!(
            old_store.upgrade().is_none(),
            "old store must close before reopen"
        );
        network.recipient.store = Arc::new(
            TestStore::open_and_migrate(path, network.recipient.host.clone())
                .await
                .unwrap(),
        );
        *network.routes.recipient.lock().unwrap() = Arc::downgrade(&network.recipient.store);
        *network.routes.fetch_barrier.lock().unwrap() = None;
        network
            .recipient
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    use durable_steps::storage::TransactionScope;
                    let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                        "SELECT output FROM _fleet_sync_steps WHERE step_name='budget'",
                    )
                    .fetch_one(scope.executor())
                    .await?;
                    assert_eq!(
                        row.try_get::<Vec<u8>, _>("output")?,
                        before,
                        "reopen retains exact old checkpoint bytes"
                    );
                    Ok(())
                })
            })
            .await
            .unwrap();
        let first_fetch = network
            .routes
            .fetch_calls
            .lock()
            .unwrap()
            .first()
            .unwrap()
            .clone();
        network.advance();
        let report = resumed_engine(&network, opted_in)
            .drive_due()
            .await
            .unwrap();
        assert!(
            report.failed.is_empty(),
            "old budget must resume, not InvalidPayload: {report:?}"
        );
        assert!(report.retried.is_empty(), "{report:?}");
        drain(&resumed_engine(&network, opted_in)).await;
        assert_eq!(
            network
                .routes
                .local_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        {
            let fetches = network.routes.fetch_calls.lock().unwrap();
            let resumed: Vec<_> = fetches
                .iter()
                .filter(|(call, _)| *call == first_fetch.0)
                .collect();
            assert_eq!(
                resumed.len(),
                2,
                "the interrupted fetch resumes with its original CallId"
            );
            assert_eq!(
                serde_json::to_vec(&resumed[0].1).unwrap(),
                serde_json::to_vec(&resumed[1].1).unwrap()
            );
            assert_eq!(
                resumed[1].1.inbox_room_bytes,
                if legacy_prefix || !opted_in {
                    None
                } else {
                    Some(100_000)
                }
            );
        }
        network
            .recipient
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    use durable_steps::storage::TransactionScope;
                    let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                        "SELECT COUNT(*) AS count FROM _fleet_sync_inbox_items",
                    )
                    .fetch_one(scope.executor())
                    .await?;
                    assert_eq!(row.try_get::<i64, _>("count")?, 1, "no duplicate custody");
                    Ok(())
                })
            })
            .await
            .unwrap();
    }
}
