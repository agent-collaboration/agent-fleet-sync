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
