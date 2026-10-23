#![allow(clippy::unwrap_used)]
mod support;
use durable_steps::{
    AtomicStore, CallId, Engine, storage::TransactionScope, turso_store::TursoScope,
};
use fleet_sync_delivery::*;
use sqlx_core::row::Row;
use std::{sync::Arc, sync::atomic::Ordering};
use support::{network::*, *};

struct GatedConsumer;
impl DeliveryConsumer<TursoScope<'_, TestHost>> for GatedConsumer {
    type Protocol = TestProtocol;
    async fn release_gate(
        &self,
        scope: &mut TursoScope<'_, TestHost>,
        _item: &DeliveryItem<TestTypes>,
    ) -> Result<GateDecision, DeliveryError> {
        let row = sqlx_core::query::query::<sqlx_turso::Turso>(
            "SELECT released FROM consumer_delivery_gate",
        )
        .fetch_one(scope.executor())
        .await
        .map_err(|error| DeliveryError::Storage(error.to_string()))?;
        match row
            .try_get::<i64, _>("released")
            .map_err(|error| DeliveryError::Storage(error.to_string()))?
        {
            0 => Ok(GateDecision::Hold { until: None }),
            1 => Ok(GateDecision::Release),
            _ => Err(DeliveryError::Corrupt {
                field: "consumer release condition",
            }),
        }
    }
    async fn deliver_locally(
        &self,
        _item: &DeliveryItem<TestTypes>,
        _call: CallId,
    ) -> LocalDelivery<TestTypes> {
        // This sender-only fixture never dispatches local delivery.
        LocalDelivery {
            outcome: LocalOutcome::Uncertain,
            observation: (),
        }
    }
    fn may_cancel(&self, requester: &String, item: &DeliveryHeader<TestTypes>) -> bool {
        requester == item.sender()
    }
}
type GatedSubmit = SubmitItem<TestStore, sqlx_turso::Turso, TestTypes, GatedConsumer>;
fn sender_engine(network: &NetworkFixture) -> Engine<TestStore, TestHost> {
    let mut engine = Engine::new(network.sender.store.clone(), network.sender.host.clone());
    engine
        .register(GatedSubmit::new(limits(), Arc::new(GatedConsumer)))
        .unwrap();
    engine
}
async fn held_network() -> NetworkFixture {
    let network = NetworkFixture::new().await;
    network.sender.store.transaction(|scope|Box::pin(async move {
        sqlx_core::raw_sql::raw_sql("CREATE TABLE consumer_delivery_gate (released INTEGER NOT NULL CHECK (released IN (0,1))); INSERT INTO consumer_delivery_gate VALUES(0);")
            .execute(scope.executor()).await.map_err(durable_steps::StoreError::backend)?;
        Ok(())
    })).await.unwrap();
    network.enqueue(1, "consumer-held body").await;
    let held = sender_engine(&network).drive_due().await.unwrap();
    assert!(held.failed.is_empty());
    assert_eq!(held.retried.len(), 1);
    assert_eq!(network.routes.sent_bodies.load(Ordering::SeqCst), 0);
    assert!(matches!(
        sender_status(&network, 1).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Queued,
            ..
        }
    ));
    network
        .sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx_core::query::query::<sqlx_turso::Turso>(
                    "SELECT record,payload FROM _fleet_sync_outbox_items",
                )
                .fetch_one(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                let record: serde_json::Value =
                    serde_json::from_str(&row.try_get::<String, _>("record").unwrap()).unwrap();
                assert_eq!(
                    record
                        .pointer("/value/submitAttempted")
                        .and_then(serde_json::Value::as_bool),
                    Some(false)
                );
                assert!(
                    row.try_get::<Option<String>, _>("payload")
                        .unwrap()
                        .is_some()
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    network
}
#[tokio::test]
async fn a_persisted_consumer_condition_holds_the_original_until_release_after_engine_restart() {
    let network = held_network().await;
    // A fresh Engine sees the persisted held condition, not a saved Release answer.
    network.advance();
    assert_eq!(
        sender_engine(&network)
            .drive_due()
            .await
            .unwrap()
            .retried
            .len(),
        1
    );
    assert_eq!(network.routes.sent_bodies.load(Ordering::SeqCst), 0);
    network
        .sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                sqlx_core::query::query::<sqlx_turso::Turso>(
                    "UPDATE consumer_delivery_gate SET released=1",
                )
                .execute(scope.executor())
                .await
                .map_err(durable_steps::StoreError::backend)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    network.advance();
    drain(&sender_engine(&network)).await;
    assert!(matches!(
        sender_status(&network, 1).await,
        OutboxStatusState::Known {
            status: DeliveryStatus::Sent { .. },
            ..
        }
    ));
    assert_eq!(network.routes.sent_bodies.load(Ordering::SeqCst), 1);
    drain(&sender_engine(&network)).await;
    assert_eq!(network.routes.sent_bodies.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn cancelling_an_original_held_before_the_first_submit_withdraws_it_without_a_relay_call() {
    let network = held_network().await;
    network
        .sender
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let DeliveryEnvelope::Cancel(cancel) = cancel(2, 1) else {
                    return Err(durable_steps::StoreError::backend("fixture cancel"));
                };
                let mut outbox = Outbox::<_, TestTypes>::new(scope, limits());
                outbox
                    .enqueue_cancel_and_admit::<GatedSubmit, _>(cancel, peer(3), &GatedConsumer)
                    .await
                    .map_err(storage_error)?;
                for item in [item_id(1), item_id(2)] {
                    assert!(matches!(
                        outbox.status(item).await.map_err(storage_error)?.state,
                        OutboxStatusState::Known {
                            status: DeliveryStatus::Cancelled,
                            ..
                        }
                    ));
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    network.advance();
    drain(&sender_engine(&network)).await;
    assert_eq!(network.routes.sent_bodies.load(Ordering::SeqCst), 0);
    network
        .mailbox
        .store
        .transaction(|scope| {
            Box::pin(async move {
                let result = Mailbox::<_, TestTypes>::new(scope, peer(2), limits())
                    .query(&[item_id(1), item_id(2)])
                    .await
                    .map_err(storage_error)?;
                assert!(
                    result
                        .statuses
                        .iter()
                        .all(|item| matches!(item.state, ItemStatusState::Unknown))
                );
                Ok(())
            })
        })
        .await
        .unwrap();
}
