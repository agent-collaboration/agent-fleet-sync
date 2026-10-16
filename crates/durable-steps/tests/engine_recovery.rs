#![allow(clippy::panic_in_result_fn, clippy::expect_used)]

mod support;

use durable_steps::{
    AtomicStore, CallReplay, CallTarget, Engine, Host, JournalScope, OperationContext,
    OperationFailure, OperationState, RequestKey, StepError, StepName, StoreError, Task,
    volatile_store::{VolatileScope, VolatileStore},
};
use futures_executor::block_on;
use std::{
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use support::TestHost;

type TestStore = VolatileStore<TestHost>;
type TestResult = Result<(), Box<dyn std::error::Error>>;

fn step(value: &str) -> StepName {
    StepName::new(value).expect("nonempty fixture step")
}
fn key(value: &str) -> RequestKey {
    RequestKey::new(value).expect("nonempty fixture key")
}

trait BusinessScope: JournalScope {
    fn increment(&mut self, key: &str) -> Result<u64, StoreError>;
}
impl<H: Host> BusinessScope for VolatileScope<'_, H> {
    fn increment(&mut self, key: &str) -> Result<u64, StoreError> {
        let next = self.read_business::<u64>(key)?.unwrap_or(0) + 1;
        self.write_business(key, &next)?;
        Ok(next)
    }
}

struct ChildTask<S>(PhantomData<fn() -> S>);
impl<S: AtomicStore> Task for ChildTask<S> {
    const KIND: &'static str = "child";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        _ctx: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        Ok(*input)
    }
}

struct ParentTask<S> {
    fail_once: Arc<AtomicBool>,
    store: PhantomData<fn() -> S>,
}
impl<S: AtomicStore> Task for ParentTask<S>
where
    for<'t> S::Scope<'t>: BusinessScope,
{
    const KIND: &'static str = "parent";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        ctx: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        let value = *input;
        let fail_once = Arc::clone(&self.fail_once);
        Ok(ctx
            .local(step("business-and-child"), move |scope| {
                Box::pin(async move {
                    let count = scope.increment("parent_effect")?;
                    scope
                        .admit::<ChildTask<S>>(&key("child-key"), &value)
                        .await?;
                    if fail_once.swap(false, Ordering::SeqCst) {
                        return Err(StepError::Retry {
                            after: Duration::ZERO,
                        });
                    }
                    Ok(count)
                })
            })
            .await?)
    }
}

#[test]
fn generic_parent_rolls_back_business_child_and_step_then_commits_and_reopens() -> TestResult {
    block_on(async {
        let host = TestHost::new();
        let store = Arc::new(TestStore::new(Arc::clone(&host)));
        let mut engine = Engine::new(Arc::clone(&store), Arc::clone(&host));
        engine.register(ParentTask::<TestStore> {
            fail_once: Arc::new(AtomicBool::new(true)),
            store: PhantomData,
        })?;
        engine.register(ChildTask::<TestStore>(PhantomData))?;
        let id = engine
            .start::<ParentTask<TestStore>>(&key("parent-key"), 7)
            .await?;
        assert!(matches!(
            engine.status(&id).await?.state,
            OperationState::Running { attempt: 1, .. }
        ));
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    assert_eq!(scope.read_business::<u64>("parent_effect")?, None);
                    use durable_steps::storage::JournalStorage;
                    assert_eq!(scope.step_count(id).await?, 0);
                    Ok(())
                })
            })
            .await?;
        host.advance(10);
        engine.drive_due().await?;
        assert!(matches!(
            engine.status(&id).await?.state,
            OperationState::Completed { .. }
        ));
        drop(engine);
        let mut reopened = Engine::new(Arc::clone(&store), host);
        reopened.register(ChildTask::<TestStore>(PhantomData))?;
        let report = reopened.drive_due().await?;
        assert_eq!(report.completed.len(), 1);
        store
            .transaction(|scope| {
                Box::pin(async move {
                    assert_eq!(scope.read_business::<u64>("parent_effect")?, Some(1));
                    Ok(())
                })
            })
            .await?;
        Ok(())
    })
}

struct DoubleTarget;
impl CallTarget for DoubleTarget {
    const KIND: &'static str = "double";
    const REPLAY: CallReplay = CallReplay::Deduplicated;
    type Request = u64;
    type Answer = u64;
    fn address(&self) -> &str {
        "test-receiver"
    }
}

struct ThreeStepTask {
    first_name: &'static str,
}
impl Task for ThreeStepTask {
    const KIND: &'static str = "three-steps";
    const VERSION: u32 = 1;
    type Store = TestStore;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = Self::Store>>(
        &self,
        ctx: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        let value = *input;
        let prepared = ctx
            .local(step(self.first_name), move |scope| {
                Box::pin(async move {
                    scope.increment("prepare_effect")?;
                    Ok(value)
                })
            })
            .await?;
        let answer = ctx
            .call(step("double-call"), DoubleTarget, prepared)
            .await?;
        Ok(ctx
            .local(step("finish"), move |scope| {
                Box::pin(async move {
                    scope.increment("finish_effect")?;
                    Ok(answer)
                })
            })
            .await?)
    }
}

#[test]
fn lost_answer_replays_call_id_and_local_outputs_after_engine_reopen() -> TestResult {
    block_on(async {
        let host = TestHost::new();
        host.lose_answers.store(1, Ordering::SeqCst);
        let store = Arc::new(TestStore::new(Arc::clone(&host)));
        let mut engine = Engine::new(Arc::clone(&store), Arc::clone(&host));
        engine.register(ThreeStepTask {
            first_name: "prepare",
        })?;
        let id = engine
            .start::<ThreeStepTask>(&key("lost-answer"), 7)
            .await?;
        assert!(matches!(
            engine.status(&id).await?.state,
            OperationState::Running { .. }
        ));
        drop(engine);
        host.advance(10);
        let mut reopened = Engine::new(Arc::clone(&store), Arc::clone(&host));
        reopened.register(ThreeStepTask {
            first_name: "prepare",
        })?;
        reopened.drive_due().await?;
        let OperationState::Completed { result } = reopened.status(&id).await?.state else {
            return Err("operation did not complete".into());
        };
        assert_eq!(serde_json::from_slice::<u64>(&result)?, 14);
        assert_eq!(host.deliveries.load(Ordering::SeqCst), 2);
        assert_eq!(host.effects.load(Ordering::SeqCst), 1);
        store
            .transaction(|scope| {
                Box::pin(async move {
                    assert_eq!(scope.read_business::<u64>("prepare_effect")?, Some(1));
                    assert_eq!(scope.read_business::<u64>("finish_effect")?, Some(1));
                    Ok(())
                })
            })
            .await?;
        assert_eq!(
            reopened
                .start::<ThreeStepTask>(&key("lost-answer"), 7)
                .await?,
            id
        );
        assert_eq!(host.deliveries.load(Ordering::SeqCst), 2);
        Ok(())
    })
}

#[test]
fn retry_waits_for_its_deadline_and_changed_step_name_fails_without_an_effect() -> TestResult {
    block_on(async {
        let host = TestHost::new();
        host.retry_calls.store(true, Ordering::SeqCst);
        let store = Arc::new(TestStore::new(Arc::clone(&host)));
        let mut engine = Engine::new(Arc::clone(&store), Arc::clone(&host));
        engine.register(ThreeStepTask {
            first_name: "prepare",
        })?;
        let id = engine
            .start::<ThreeStepTask>(&key("changed-step"), 7)
            .await?;
        assert!(engine.drive_due().await?.retried.is_empty());
        assert_eq!(host.deliveries.load(Ordering::SeqCst), 1);
        drop(engine);
        host.advance(10);
        let mut reopened = Engine::new(Arc::clone(&store), Arc::clone(&host));
        reopened.register(ThreeStepTask {
            first_name: "renamed",
        })?;
        reopened.drive_due().await?;
        assert!(matches!(
            reopened.status(&id).await?.state,
            OperationState::Failed {
                reason: OperationFailure::Nondeterministic { position: 0, .. }
            }
        ));
        assert_eq!(host.deliveries.load(Ordering::SeqCst), 1);
        store
            .transaction(|scope| {
                Box::pin(async move {
                    assert_eq!(scope.read_business::<u64>("prepare_effect")?, Some(1));
                    assert_eq!(scope.read_business::<u64>("finish_effect")?, None);
                    Ok(())
                })
            })
            .await?;
        Ok(())
    })
}
