use super::*;
use durable_steps::{AtomicStore, Engine, StoreError, storage::TransactionScope};
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::Arc;
pub const CREATED: i64 = 1_717_986_918_400;
pub fn day(days: i64) -> SyncTime {
    SyncTime::from_unix_millis(CREATED + days * 24 * 60 * 60 * 1000)
}
pub fn envelope_at(milliseconds: i64, sequence: u64) -> DeliveryEnvelope<TestTypes> {
    let id = ItemId::new(durable_steps::Uuid::from_u128(
        ((milliseconds as u128) << 80) | 0x70008000000000000000 | u128::from(sequence),
    ))
    .unwrap();
    DeliveryEnvelope::Item(
        DeliveryItem::new(
            id,
            "sender".into(),
            Recipient {
                peer: peer(2),
                address: "inbox".into(),
            },
            None,
            "retention body".into(),
        )
        .unwrap(),
    )
}
pub async fn identities(store: &TestStore, side: &str) -> Vec<String> {
    let statement = match side {
        "outbox" => "SELECT item_id FROM _fleet_sync_outbox_items ORDER BY item_id",
        _ => "SELECT item_id FROM _fleet_sync_inbox_items ORDER BY item_id",
    };
    store
        .transaction(move |scope| {
            Box::pin(async move {
                query::<Turso>(statement)
                    .fetch_all(scope.executor())
                    .await?
                    .iter()
                    .map(|row| row.try_get("item_id"))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(StoreError::backend)
            })
        })
        .await
        .unwrap()
}
pub async fn reopen(fixture: Fixture) -> Fixture {
    let Fixture {
        store,
        host,
        directory,
    } = fixture;
    assert_eq!(Arc::strong_count(&store), 1);
    drop(store);
    let store = Arc::new(
        TestStore::open_and_migrate(directory.path().join("delivery.db"), host.clone())
            .await
            .unwrap(),
    );
    Fixture {
        store,
        host,
        directory,
    }
}
pub fn retention_engine(fixture: &Fixture) -> Engine<TestStore, TestHost> {
    let mut engine = Engine::new(fixture.store.clone(), fixture.host.clone());
    engine
        .register(RecordRemoval::new(
            fleet_sync_delivery::DeliveryHostRole::Machine,
        ))
        .unwrap();
    engine
}

// Hold unrelated work behind retention using the journal's normal guarded retry.
// This controls the causal order without admitting or running removal in the test.
pub async fn defer_delivery_work(fixture: &Fixture, until: SyncTime) {
    use durable_steps::{
        DriveLease, Task,
        storage::{JournalStorage, OperationExecution},
    };
    fixture
        .store
        .transaction(move |scope| {
            Box::pin(async move {
                for operation in scope.due(SyncTime::from_unix_millis(i64::MAX)).await? {
                    let record = scope.operation(operation).await?.unwrap();
                    if record.kind.as_str() == RecordRemoval::KIND {
                        continue;
                    }
                    if let OperationExecution::Running {
                        next_attempt_at,
                        attempt,
                        ..
                    } = record.execution
                    {
                        let token = durable_steps::Uuid::now_v7();
                        assert!(
                            scope
                                .claim(
                                    operation,
                                    next_attempt_at,
                                    DriveLease {
                                        token,
                                        until: next_attempt_at
                                            .after(std::time::Duration::from_secs(1))
                                    }
                                )
                                .await?
                                .is_some()
                        );
                        scope.retry(operation, token, until, attempt).await?;
                    }
                }
                Ok(())
            })
        })
        .await
        .unwrap();
}
