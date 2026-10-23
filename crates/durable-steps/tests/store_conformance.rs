#![allow(clippy::panic_in_result_fn, clippy::panic)]

mod support;

use durable_steps::{
    AtomicStore, CallReplay, DriveLease, JournalScope, OperationContext, OperationFailure,
    RequestKey, StartRefusal, StoreError, SyncTime, Task, storage::JournalStorage,
    volatile_store::VolatileStore,
};
use futures_executor::block_on;
use std::sync::Arc;
use support::TestHost;

struct EmptyTask;
impl Task for EmptyTask {
    const KIND: &'static str = "empty";
    const VERSION: u32 = 1;
    type Store = VolatileStore<TestHost>;
    type Input = u64;
    type Output = ();
    async fn run<C: OperationContext<Store = Self::Store>>(
        &self,
        _ctx: &mut C,
        _input: &u64,
    ) -> Result<(), OperationFailure> {
        Ok(())
    }
}

fn key(value: &str) -> RequestKey {
    RequestKey::new(value).unwrap_or_else(|error| panic!("{error}"))
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn engine_schema_records_its_explicit_integer_version()
-> Result<(), Box<dyn std::error::Error>> {
    use sqlx_turso::{
        TursoConnectOptions,
        sqlx::{ConnectOptions, Connection},
    };
    let directory = tempfile::tempdir()?;
    let mut connection = TursoConnectOptions::new()
        .filename(directory.path().join("journal.db"))
        .create_if_missing(true)
        .connect()
        .await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    durable_steps::turso_store::migrate_journal(&mut transaction).await?;
    transaction.commit().await?;
    use sqlx_core::row::Row;
    let rows = sqlx::query::<sqlx_turso::Turso>("SELECT version FROM _fleet_sync_schema")
        .fetch_all(&mut connection)
        .await?;
    let versions = rows
        .iter()
        .map(|row| row.try_get::<i64, _>("version"))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(versions, vec![1]);
    Ok(())
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn admission_refuses_a_non_v7_host_mint_without_writing_an_operation()
-> Result<(), Box<dyn std::error::Error>> {
    use durable_steps::{storage::TransactionScope, turso_store::TursoStore};
    use sqlx_core::row::Row;
    use std::sync::atomic::Ordering;
    let directory = tempfile::tempdir()?;
    let host = TestHost::new();
    host.invalid_ids.store(true, Ordering::SeqCst);
    let store =
        TursoStore::open_and_migrate(directory.path().join("journal.db"), host.clone()).await?;
    let refused = store
        .transaction(|scope| {
            Box::pin(async move {
                scope
                    .admit::<EmptyTask>(&key("bad-mint"), &7)
                    .await
                    .map_err(StoreError::backend)
            })
        })
        .await;
    assert!(matches!(refused, Err(StoreError::Backend { .. })));
    store
        .transaction(|scope| {
            Box::pin(async move {
                let row = sqlx::query::<sqlx_turso::Turso>(
                    "SELECT count(*) AS count FROM _fleet_sync_operations",
                )
                .fetch_one(scope.executor())
                .await?;
                assert_eq!(row.try_get::<i64, _>("count")?, 0);
                Ok(())
            })
        })
        .await?;
    host.invalid_ids.store(false, Ordering::SeqCst);
    let admitted = store
        .transaction(|scope| {
            Box::pin(async move {
                scope
                    .admit::<EmptyTask>(&key("good-mint"), &7)
                    .await
                    .map_err(StoreError::backend)
            })
        })
        .await?;
    assert!(runtime_primitives::uuid_v7(admitted.id().as_uuid()));
    Ok(())
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn reopened_operation_rows_refuse_noncanonical_keys_without_normalizing()
-> Result<(), Box<dyn std::error::Error>> {
    use durable_steps::{storage::TransactionScope, turso_store::TursoStore};
    let canonical = "0197abc0-def0-7123-8123-abcdef012345";
    for offered in [
        canonical.replace("7123", "4123"),
        canonical.to_uppercase(),
        canonical.replace('-', ""),
    ] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("journal.db");
        let host = TestHost::new();
        let store = TursoStore::open_and_migrate(&path, host.clone()).await?;
        store
            .transaction(|scope| {
                Box::pin(async move {
                    scope
                        .admit::<EmptyTask>(&key("stored-id"), &7)
                        .await
                        .map_err(StoreError::backend)?;
                    sqlx::query::<sqlx_turso::Turso>(
                        "UPDATE _fleet_sync_operations SET operation_id=?",
                    )
                    .bind(offered)
                    .execute(scope.executor())
                    .await?;
                    Ok(())
                })
            })
            .await?;
        drop(store);
        let store = TursoStore::open_and_migrate(&path, host).await?;
        let result = store
            .transaction(|scope| {
                Box::pin(async move { Ok(scope.admit::<EmptyTask>(&key("stored-id"), &7).await) })
            })
            .await?;
        assert!(
            matches!(result, Err(StartRefusal::Storage(StoreError::Corrupt { field, .. })) if field == "operation_id")
        );
        let due = store
            .transaction(|scope| {
                Box::pin(async move { scope.due(SyncTime::from_unix_millis(1_000)).await })
            })
            .await;
        assert!(matches!(due, Err(StoreError::Corrupt { field, .. }) if field == "operation_id"));
    }
    Ok(())
}

#[test]
fn admission_keeps_exact_integer_input_and_distinguishes_adjacent_large_values()
-> Result<(), Box<dyn std::error::Error>> {
    block_on(async {
        let host = TestHost::new();
        let store = VolatileStore::new(host);
        let id = store
            .transaction(|scope| {
                Box::pin(async move {
                    scope
                        .admit::<EmptyTask>(&key("exact-input"), &9_007_199_254_740_993)
                        .await
                        .map_err(StoreError::backend)
                })
            })
            .await?
            .id();
        store.transaction(move |scope| Box::pin(async move {
            let recorded = scope.operation(id).await?.ok_or(StoreError::MissingOperation { id })?;
            assert_eq!(serde_json::from_slice::<u64>(&match recorded.execution { durable_steps::storage::OperationExecution::Running { input, .. } => input, _ => return Err(StoreError::Aborted { reason: "unexpected terminal".into() }) }).map_err(StoreError::backend)?, 9_007_199_254_740_993);
            let conflict = scope.admit::<EmptyTask>(&key("exact-input"), &9_007_199_254_740_992).await;
            assert!(matches!(conflict, Err(StartRefusal::RequestConflict { existing }) if existing == id));
            Ok(())
        })).await?;
        Ok(())
    })
}

#[test]
fn admission_replays_and_rejects_changed_content() -> Result<(), Box<dyn std::error::Error>> {
    block_on(async {
        let host = TestHost::new();
        let store = VolatileStore::new(Arc::clone(&host));
        let first = store
            .transaction(|scope| {
                Box::pin(async move {
                    scope
                        .admit::<EmptyTask>(&key("same"), &7)
                        .await
                        .map_err(StoreError::backend)
                })
            })
            .await?
            .id();
        let repeat = store
            .transaction(|scope| {
                Box::pin(async move {
                    scope
                        .admit::<EmptyTask>(&key("same"), &7)
                        .await
                        .map_err(StoreError::backend)
                })
            })
            .await?
            .id();
        assert_eq!(repeat, first);
        let conflict = store
            .transaction(|scope| {
                Box::pin(async move { Ok(scope.admit::<EmptyTask>(&key("same"), &8).await) })
            })
            .await?;
        assert!(
            matches!(conflict, Err(StartRefusal::RequestConflict { existing }) if existing == first)
        );
        Ok(())
    })
}

#[test]
fn business_admission_and_wake_rollback_together() -> Result<(), Box<dyn std::error::Error>> {
    block_on(async {
        let host = TestHost::new();
        let store = VolatileStore::new(host);
        let aborted = store
            .transaction(|scope| {
                Box::pin(async move {
                    scope.write_business("counter", &1_u64)?;
                    scope
                        .admit::<EmptyTask>(&key("rollback"), &7)
                        .await
                        .map_err(StoreError::backend)?;
                    Err::<(), _>(StoreError::Aborted {
                        reason: "requested failure".into(),
                    })
                })
            })
            .await;
        assert!(matches!(aborted, Err(StoreError::Aborted { .. })));
        assert_eq!(store.committed_wake().await, None);
        store
            .transaction(|scope| {
                Box::pin(async move {
                    assert_eq!(scope.read_business::<u64>("counter")?, None);
                    assert!(
                        scope
                            .due(SyncTime::from_unix_millis(1_000))
                            .await?
                            .is_empty()
                    );
                    Ok(())
                })
            })
            .await?;
        Ok(())
    })
}

#[test]
fn committed_writes_are_detached_and_keep_the_earlier_wake()
-> Result<(), Box<dyn std::error::Error>> {
    block_on(async {
        let host = TestHost::new();
        let store = VolatileStore::new(host);
        store
            .transaction(|scope| {
                Box::pin(async move {
                    scope.write_business("rows", &vec![1_u64, 2])?;
                    scope.ensure_wake(SyncTime::from_unix_millis(50)).await?;
                    scope.ensure_wake(SyncTime::from_unix_millis(100)).await?;
                    Ok(())
                })
            })
            .await?;
        assert_eq!(
            store.committed_wake().await,
            Some(SyncTime::from_unix_millis(50))
        );
        let mut detached = store
            .transaction(|scope| Box::pin(async move { scope.read_business::<Vec<u64>>("rows") }))
            .await?
            .ok_or("missing committed value")?;
        detached.push(3);
        store
            .transaction(|scope| {
                Box::pin(async move {
                    assert_eq!(scope.read_business::<Vec<u64>>("rows")?, Some(vec![1, 2]));
                    Ok(())
                })
            })
            .await?;
        Ok(())
    })
}

#[test]
fn replaced_drive_token_cannot_change_the_operation() -> Result<(), Box<dyn std::error::Error>> {
    block_on(async {
        let host = TestHost::new();
        let store = VolatileStore::new(Arc::clone(&host));
        let id = store
            .transaction(|scope| {
                Box::pin(async move {
                    scope
                        .admit::<EmptyTask>(&key("claim"), &7)
                        .await
                        .map_err(StoreError::backend)
                })
            })
            .await?
            .id();
        let old = durable_steps::Uuid::from_u128(100);
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    assert!(
                        scope
                            .claim(
                                id,
                                SyncTime::from_unix_millis(1_000),
                                DriveLease {
                                    token: old,
                                    until: SyncTime::from_unix_millis(2_000)
                                }
                            )
                            .await?
                            .is_some()
                    );
                    Ok(())
                })
            })
            .await?;
        let replacement = durable_steps::Uuid::from_u128(101);
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    assert!(
                        scope
                            .claim(
                                id,
                                SyncTime::from_unix_millis(2_000),
                                DriveLease {
                                    token: replacement,
                                    until: SyncTime::from_unix_millis(3_000)
                                }
                            )
                            .await?
                            .is_some()
                    );
                    Ok(())
                })
            })
            .await?;
        let rejected = store
            .transaction(move |scope| {
                Box::pin(async move {
                    scope
                        .finish(
                            id,
                            old,
                            durable_steps::storage::OperationOutcome::Completed { result: vec![] },
                            SyncTime::from_unix_millis(2_000),
                            true,
                        )
                        .await
                })
            })
            .await;
        assert_eq!(rejected, Err(StoreError::StaleDriveToken { id }));
        let _ = CallReplay::Deduplicated;
        Ok(())
    })
}
