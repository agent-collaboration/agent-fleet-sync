#![allow(clippy::unwrap_used)]
mod support;

use durable_steps::{
    AtomicStore, Engine, JournalScope, OperationContext, OperationFailure, OperationState,
    RequestKey, StepName, SyncTime, Task, WakeFailure,
    storage::{JournalStorage, OperationExecution},
    volatile_store::VolatileStore,
};
use std::sync::{Arc, atomic::Ordering};
use support::TestHost;

type MemoryStore = VolatileStore<TestHost>;

struct Complete;
impl Task for Complete {
    const KIND: &'static str = "future-wake-complete";
    const VERSION: u32 = 1;
    type Store = MemoryStore;
    type Input = ();
    type Output = ();
    async fn run<C: OperationContext<Store = Self::Store>>(
        &self,
        _: &mut C,
        _: &(),
    ) -> Result<(), OperationFailure> {
        Ok(())
    }
}

struct AdmitDuringStep(Arc<TestHost>);
impl Task for AdmitDuringStep {
    const KIND: &'static str = "future-wake-local-admission";
    const VERSION: u32 = 1;
    type Store = MemoryStore;
    type Input = ();
    type Output = ();
    async fn run<C: OperationContext<Store = Self::Store>>(
        &self,
        context: &mut C,
        _: &(),
    ) -> Result<(), OperationFailure> {
        let host = self.0.clone();
        context
            .local(StepName::new("admit-due-peer").unwrap(), move |scope| {
                Box::pin(async move {
                    scope
                        .admit::<Complete>(&RequestKey::new("peer").unwrap(), &())
                        .await
                        .map_err(durable_steps::StoreError::backend)?;
                    // Observe requests from this committed local step onward,
                    // independently of the engine's initial re-arm requests.
                    host.wakes.lock().unwrap().clear();
                    Ok(())
                })
            })
            .await?;
        Ok(())
    }
}

fn assert_only_future_requests(host: &TestHost, now: i64) {
    let requests = host.wakes.lock().unwrap();
    assert!(
        !requests.is_empty(),
        "the drive must reach the real wake scheduler"
    );
    assert!(
        requests.iter().all(|at| at.unix_millis() > now),
        "no wake may be requested at or before now={now}: {requests:?}"
    );
}

#[test]
fn engine_rearm_clamps_due_and_firing_deadlines_but_preserves_future_deadlines() {
    futures_executor::block_on(async {
        for deadline in [1_000, 999, 1_250] {
            let host = TestHost::new();
            host.clock.store(deadline, Ordering::SeqCst);
            let store = Arc::new(MemoryStore::new(host.clone()));
            let operation = store
                .transaction(|scope| {
                    Box::pin(async move {
                        Ok(scope
                            .admit::<Complete>(&RequestKey::new("original").unwrap(), &())
                            .await
                            .map_err(durable_steps::StoreError::backend)?
                            .id())
                    })
                })
                .await
                .unwrap();
            host.clock.store(1_000, Ordering::SeqCst);
            let mut engine = Engine::new(store.clone(), host.clone());
            engine.register(Complete).unwrap();

            let report = engine.drive_due().await.unwrap();

            assert_only_future_requests(&host, 1_000);
            assert_eq!(
                host.wakes.lock().unwrap().first().copied(),
                Some(SyncTime::from_unix_millis(deadline.max(1_001)))
            );
            // The committed journal wake is the original deadline, even when
            // the external scheduler request must move to a future millisecond.
            assert_eq!(
                store.committed_wake().await,
                Some(SyncTime::from_unix_millis(deadline))
            );
            let status = engine.status(&operation).await.unwrap();
            if deadline > 1_000 {
                assert!(report.completed.is_empty());
                assert!(
                    matches!(status.state, OperationState::Running { next_attempt_at, .. }
                    if next_attempt_at == SyncTime::from_unix_millis(deadline))
                );
            } else {
                assert_eq!(report.completed, vec![operation]);
                assert!(matches!(status.state, OperationState::Completed { .. }));
            }
        }
    });
}

#[test]
fn context_rearm_keeps_newly_admitted_due_work_and_requests_a_future_wake() {
    futures_executor::block_on(async {
        let host = TestHost::new();
        let store = Arc::new(MemoryStore::new(host.clone()));
        let mut engine = Engine::new(store.clone(), host.clone());
        engine.register(Complete).unwrap();
        engine.register(AdmitDuringStep(host.clone())).unwrap();
        store
            .transaction(|scope| {
                Box::pin(async move {
                    scope
                        .admit::<AdmitDuringStep>(&RequestKey::new("admitter").unwrap(), &())
                        .await
                        .map_err(durable_steps::StoreError::backend)?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let report = engine.drive_due().await.unwrap();

        assert_only_future_requests(&host, 1_000);
        assert_eq!(report.completed.len(), 1);
        let pending = store
            .transaction(|scope| {
                Box::pin(async move {
                    let due = scope.due(SyncTime::from_unix_millis(1_000)).await?;
                    assert_eq!(
                        due.len(),
                        1,
                        "local admission remains due for the next pass"
                    );
                    scope.operation(*due.first().unwrap()).await
                })
            })
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(pending.execution, OperationExecution::Running { next_attempt_at, .. }
            if next_attempt_at == SyncTime::from_unix_millis(1_000))
        );
        assert_eq!(
            store.committed_wake().await,
            Some(SyncTime::from_unix_millis(1_000))
        );
    });
}

#[test]
fn unrepresentable_future_deadline_reports_failure_without_a_wake_request() {
    futures_executor::block_on(async {
        let host = TestHost::new();
        host.clock.store(i64::MAX, Ordering::SeqCst);
        let store = Arc::new(MemoryStore::new(host.clone()));
        let mut engine = Engine::new(store.clone(), host.clone());
        engine.register(Complete).unwrap();
        store
            .transaction(|scope| {
                Box::pin(async move {
                    scope
                        .admit::<Complete>(&RequestKey::new("last-millisecond").unwrap(), &())
                        .await
                        .map_err(durable_steps::StoreError::backend)?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let report = engine.drive_due().await.unwrap();

        assert_eq!(
            report.wake_failure,
            Some(WakeFailure::InvalidDeadline {
                at: SyncTime::from_unix_millis(i64::MAX),
            })
        );
        assert!(
            host.wakes.lock().unwrap().is_empty(),
            "an unrepresentable future cannot be scheduled"
        );
        assert_eq!(report.completed.len(), 1);
    });
}
