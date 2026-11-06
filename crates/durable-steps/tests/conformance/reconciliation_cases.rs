//! Replay and failure rules through the real engine, with a read-only receipt host.
use crate::support::TestHost;
use durable_steps::{
    AtomicStore, CallFailure, CallResult, CallTarget, Engine, JournalScope, NotSentReason,
    OperationContext, OperationFailure, OperationId, OperationState, RequestKey, StepError,
    StepName, StoreError, Task,
    storage::{CallReach, JournalStorage, StepState},
};
use std::{
    future::{Future, poll_fn},
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
    time::Duration,
};

#[derive(Default, serde::Serialize, serde::Deserialize)]
pub struct ReconcilingTarget<const DEDUP: bool>;
impl<const DEDUP: bool> CallTarget for ReconcilingTarget<DEDUP> {
    const KIND: &'static str = "reconciling-double";
    const DEDUPLICATES: bool = DEDUP;
    const RECONCILES: bool = true;
    type Request = u64;
    type Answer = u64;
}

#[derive(Default, serde::Serialize, serde::Deserialize)]
pub struct DefaultTarget;
impl CallTarget for DefaultTarget {
    const KIND: &'static str = "default-nonrepeating-double";
    const DEDUPLICATES: bool = false;
    type Request = u64;
    type Answer = u64;
}

struct ReconcileTask<S, C> {
    wait_after_call: Arc<AtomicBool>,
    types: PhantomData<fn() -> (S, C)>,
}
impl<S: AtomicStore, C: CallTarget<Request = u64, Answer = u64> + Default> Task
    for ReconcileTask<S, C>
{
    const KIND: &'static str = "suite-call-reconciliation";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = u64;
    type Output = u64;
    async fn run<O: OperationContext<Store = S>>(
        &self,
        ctx: &mut O,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        let answer = ctx
            .call(
                step("effect").map_err(StepError::from)?,
                C::default(),
                *input,
            )
            .await?;
        let wait = self.wait_after_call.clone();
        ctx.local(step("after-answer").map_err(StepError::from)?, move |_| {
            Box::pin(async move {
                if wait.swap(false, Ordering::SeqCst) {
                    Err(StepError::Retry {
                        after: Duration::ZERO,
                    })
                } else {
                    Ok(())
                }
            })
        })
        .await?;
        Ok(answer)
    }
}

fn key(value: &str) -> Result<RequestKey, StoreError> {
    RequestKey::new(value).map_err(StoreError::backend)
}
fn step(value: &str) -> Result<StepName, StoreError> {
    StepName::new(value).map_err(StoreError::backend)
}
fn check(holds: bool, message: &str) -> Result<(), StoreError> {
    if holds {
        Ok(())
    } else {
        Err(StoreError::backend(message))
    }
}
fn engine<S: AtomicStore, C: CallTarget<Request = u64, Answer = u64> + Default>(
    store: Arc<S>,
    host: Arc<TestHost>,
    wait: bool,
) -> Result<Engine<S, TestHost>, StoreError> {
    let mut engine = Engine::new(store, host);
    engine.register(ReconcileTask::<S, C> {
        wait_after_call: Arc::new(AtomicBool::new(wait)),
        types: PhantomData,
    })?;
    Ok(engine)
}

async fn interrupted<S: AtomicStore, C: CallTarget<Request = u64, Answer = u64> + Default>(
    store: &S,
    host: &TestHost,
    engine: &Engine<S, TestHost>,
    name: &'static str,
) -> Result<OperationId, StoreError> {
    let id = store
        .transaction(move |scope| {
            Box::pin(async move {
                scope
                    .admit::<ReconcileTask<S, C>>(&key(name)?, &19)
                    .await
                    .map(|admitted| admitted.id())
                    .map_err(StoreError::backend)
            })
        })
        .await?;
    host.hang_after_effect.store(true, Ordering::SeqCst);
    let before = host.deliveries.load(Ordering::SeqCst);
    let mut drive = Box::pin(engine.drive_due());
    poll_fn(|context| match drive.as_mut().poll(context) {
        Poll::Pending if host.deliveries.load(Ordering::SeqCst) == before + 1 => {
            Poll::Ready(Ok(()))
        }
        Poll::Pending => Poll::Pending,
        Poll::Ready(_) => Poll::Ready(Err(StoreError::backend("dispatch did not stay blocked"))),
    })
    .await?;
    drop(drive);
    host.hang_after_effect.store(false, Ordering::SeqCst);
    store.transaction(move |scope|Box::pin(async move {
        check(matches!(scope.step(id,0).await?.map(|record|record.state),Some(StepState::CallPending {attempt,..}) if attempt.reach==CallReach::Dispatching),"cut must preserve committed Dispatching")
    })).await?;
    host.advance(30_001);
    Ok(id)
}

pub async fn recovered_answer<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError> {
    let first = engine::<S, ReconcilingTarget<false>>(store.clone(), host.clone(), false)?;
    let id = interrupted::<S, ReconcilingTarget<false>>(&*store, &host, &first, "reconcile-some")
        .await?;
    drop(first);
    let dispatches = host.deliveries.load(Ordering::SeqCst);
    let effects = host.effects.load(Ordering::SeqCst);
    let reconciliations = host.reconciliations.load(Ordering::SeqCst);
    let restarted = engine::<S, ReconcilingTarget<false>>(store.clone(), host.clone(), true)?;
    restarted.drive_due().await?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                match scope.step(id, 0).await?.map(|record| record.state) {
                    Some(StepState::CallDone {
                        attempt, output, ..
                    }) => check(
                        attempt.reach == CallReach::MayHaveReached
                            && serde_json::from_slice::<CallResult<u64>>(&output)
                                .map_err(StoreError::backend)?
                                == CallResult::Answered(38),
                        "reconciled answer must be saved CallDone with original possible reach",
                    ),
                    _ => Err(StoreError::backend(
                        "read-only reconciliation must save CallDone",
                    )),
                }
            })
        })
        .await?;
    host.advance(100);
    restarted.drive_due().await?;
    check(
        matches!(restarted.status(&id).await?.state,OperationState::Completed {result} if serde_json::from_slice::<u64>(&result).map_err(StoreError::backend)?==38),
        "saved reconciliation answer must complete on replay",
    )?;
    check(
        host.deliveries.load(Ordering::SeqCst) == dispatches
            && host.effects.load(Ordering::SeqCst) == effects
            && host.reconciliations.load(Ordering::SeqCst) == reconciliations + 1,
        "reconciliation must read one receipt with no dispatch/effect and replay must not repeat lookup",
    )
}

pub async fn missing_answer<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError> {
    let engine = engine::<S, ReconcilingTarget<false>>(store.clone(), host.clone(), false)?;
    let id = interrupted::<S, ReconcilingTarget<false>>(&*store, &host, &engine, "reconcile-none")
        .await?;
    let dispatches = host.deliveries.load(Ordering::SeqCst);
    let reconciliations = host.reconciliations.load(Ordering::SeqCst);
    host.reconciliation_missing.store(true, Ordering::SeqCst);
    engine.drive_due().await?;
    host.reconciliation_missing.store(false, Ordering::SeqCst);
    check(
        matches!(
            engine.status(&id).await?.state,
            OperationState::Failed {
                reason: OperationFailure::Uncertain { .. }
            }
        ),
        "missing receipt must stay Uncertain",
    )?;
    check(
        host.reconciliations.load(Ordering::SeqCst) == reconciliations + 1
            && host.deliveries.load(Ordering::SeqCst) == dispatches,
        "None must come from reconciliation without another dispatch",
    )
}

pub async fn failures_preserve_possible_reach<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError> {
    for (name, failure) in [
        ("reconcile-answer-lost", CallFailure::AnswerLost),
        (
            "reconcile-not-sent",
            CallFailure::NotSent {
                reason: NotSentReason::Overloaded,
            },
        ),
        (
            "reconcile-retryable",
            CallFailure::Retryable {
                reason: durable_steps::CallRetryReason {
                    code: "lookupUnavailable".into(),
                    detail: "fixture".into(),
                },
            },
        ),
    ] {
        let engine = engine::<S, ReconcilingTarget<false>>(store.clone(), host.clone(), false)?;
        let id = interrupted::<S, ReconcilingTarget<false>>(&*store, &host, &engine, name).await?;
        let dispatches = host.deliveries.load(Ordering::SeqCst);
        let reconciliations = host.reconciliations.load(Ordering::SeqCst);
        host.reconciliation_failures
            .lock()
            .map_err(StoreError::backend)?
            .push_back(failure);
        engine.drive_due().await?;
        check(
            matches!(
                engine.status(&id).await?.state,
                OperationState::Failed {
                    reason: OperationFailure::Uncertain { .. }
                }
            ),
            "failed lookup cannot erase possible prior effect",
        )?;
        check(
            host.reconciliations.load(Ordering::SeqCst) == reconciliations + 1
                && host.deliveries.load(Ordering::SeqCst) == dispatches,
            "failed reconciliation must be consulted once without redispatch",
        )?;
    }
    Ok(())
}

pub async fn default_target<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError> {
    let engine = engine::<S, DefaultTarget>(store.clone(), host.clone(), false)?;
    let id = interrupted::<S, DefaultTarget>(&*store, &host, &engine, "reconcile-default-target")
        .await?;
    let dispatches = host.deliveries.load(Ordering::SeqCst);
    let reconciliations = host.reconciliations.load(Ordering::SeqCst);
    engine.drive_due().await?;
    check(
        matches!(
            engine.status(&id).await?.state,
            OperationState::Failed {
                reason: OperationFailure::Uncertain { .. }
            }
        ),
        "default target must stay Uncertain",
    )?;
    check(
        host.deliveries.load(Ordering::SeqCst) == dispatches
            && host.reconciliations.load(Ordering::SeqCst) == reconciliations,
        "default target must neither dispatch nor reconcile",
    )
}

pub async fn deduplicating_target<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError> {
    let engine = engine::<S, ReconcilingTarget<true>>(store.clone(), host.clone(), false)?;
    let id = interrupted::<S, ReconcilingTarget<true>>(
        &*store,
        &host,
        &engine,
        "reconcile-deduplicating-target",
    )
    .await?;
    let dispatches = host.deliveries.load(Ordering::SeqCst);
    let effects = host.effects.load(Ordering::SeqCst);
    let reconciliations = host.reconciliations.load(Ordering::SeqCst);
    engine.drive_due().await?;
    check(
        matches!(
            engine.status(&id).await?.state,
            OperationState::Completed { .. }
        ),
        "deduplicating call must replay normally",
    )?;
    check(
        host.deliveries.load(Ordering::SeqCst) == dispatches + 1
            && host.effects.load(Ordering::SeqCst) == effects
            && host.reconciliations.load(Ordering::SeqCst) == reconciliations,
        "deduplicating recovery dispatches, never reconciles",
    )
}

pub async fn fresh_and_safe_retries<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError> {
    for (name, failure) in [
        (
            "reconcile-never-sent",
            Some(CallFailure::NotSent {
                reason: NotSentReason::Overloaded,
            }),
        ),
        (
            "reconcile-no-effect",
            Some(CallFailure::Retryable {
                reason: durable_steps::CallRetryReason {
                    code: "busy".into(),
                    detail: "fixture".into(),
                },
            }),
        ),
        ("reconcile-fresh", None),
    ] {
        if let Some(failure) = failure {
            host.failures
                .lock()
                .map_err(StoreError::backend)?
                .push_back(failure);
        }
        let engine = engine::<S, ReconcilingTarget<false>>(store.clone(), host.clone(), false)?;
        let reconciliations = host.reconciliations.load(Ordering::SeqCst);
        let id = engine
            .start::<ReconcileTask<S, ReconcilingTarget<false>>>(&key(name)?, 13)
            .await
            .map_err(StoreError::backend)?;
        host.advance(100);
        engine.drive_due().await?;
        check(
            matches!(engine.status(&id).await?.state,OperationState::Completed {result} if serde_json::from_slice::<u64>(&result).map_err(StoreError::backend)?==26),
            "fresh/safe retry must dispatch and complete normally",
        )?;
        check(
            host.reconciliations.load(Ordering::SeqCst) == reconciliations,
            "fresh, NeverSent and NoEffect paths must never reconcile",
        )?;
    }
    Ok(())
}

pub async fn live_lost_answer<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError> {
    let engine = engine::<S, ReconcilingTarget<false>>(store.clone(), host.clone(), true)?;
    let reconciliations = host.reconciliations.load(Ordering::SeqCst);
    let dispatches = host.deliveries.load(Ordering::SeqCst);
    let effects = host.effects.load(Ordering::SeqCst);
    host.lose_answers.store(1, Ordering::SeqCst);
    let id = engine
        .start::<ReconcileTask<S, ReconcilingTarget<false>>>(
            &key("reconcile-fresh-answer-lost")?,
            19,
        )
        .await
        .map_err(StoreError::backend)?;
    store.transaction(move |scope|Box::pin(async move {
        match scope.step(id,0).await?.map(|record|record.state) {
            Some(StepState::CallDone {attempt,output,..})=>check(attempt.reach==CallReach::MayHaveReached && serde_json::from_slice::<CallResult<u64>>(&output).map_err(StoreError::backend)?==CallResult::Answered(38),"live AnswerLost reconciliation must save the validated answer before advancing"),
            _=>Err(StoreError::backend("live AnswerLost must reconcile to CallDone")),
        }
    })).await?;
    host.advance(100);
    engine.drive_due().await?;
    check(
        matches!(engine.status(&id).await?.state,OperationState::Completed {result} if serde_json::from_slice::<u64>(&result).map_err(StoreError::backend)?==38),
        "live reconciled answer must complete on replay",
    )?;
    check(
        host.reconciliations.load(Ordering::SeqCst) == reconciliations + 1
            && host.deliveries.load(Ordering::SeqCst) == dispatches + 1
            && host.effects.load(Ordering::SeqCst) == effects + 1,
        "live AnswerLost must use one read-only lookup and never dispatch twice",
    )?;
    Ok(())
}
