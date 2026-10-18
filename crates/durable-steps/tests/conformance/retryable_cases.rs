//! The host's scripted refusal happens before its receiver writes anything.
//! The subject is the real engine and its persisted call history.
use super::{BusinessScope, check, key, task_fixtures::CallTask};
use crate::support::TestHost;
use durable_steps::{
    AtomicStore, CallFailure, CallResult, CallRetryReason, Engine, OperationFailure, OperationId,
    OperationState, StoreError,
    storage::{CallAttempt, CallReach, JournalStorage, StepState},
};
use std::{
    future::{Future, poll_fn},
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
};

pub fn call_engine<S: AtomicStore, const BOUND: bool, const DEDUPLICATES: bool>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<Engine<S, TestHost>, StoreError> {
    let mut engine = Engine::new(store, host);
    engine.register(CallTask::<S, BOUND, DEDUPLICATES> {
        wait_once: Arc::new(AtomicBool::new(false)),
        store: PhantomData,
    })?;
    Ok(engine)
}
fn no_effect_reason() -> CallRetryReason {
    CallRetryReason {
        code: "fixture_no_effect".into(),
        detail: "receiver refused before writing".into(),
    }
}
fn refuse_next_call(host: &TestHost) -> Result<(), StoreError> {
    host.failures
        .lock()
        .map_err(StoreError::backend)?
        .push_back(CallFailure::Retryable {
            reason: no_effect_reason(),
        });
    Ok(())
}
pub async fn pending_attempt<S: AtomicStore>(
    store: &S,
    operation: OperationId,
) -> Result<CallAttempt, StoreError> {
    store
        .transaction(move |scope| {
            Box::pin(async move {
                match scope.step(operation, 0).await?.map(|step| step.state) {
                    Some(StepState::CallPending { attempt, .. }) => Ok(attempt),
                    _ => Err(StoreError::backend(
                        "expected a pending call, not a saved answer",
                    )),
                }
            })
        })
        .await
}
async fn answered<S: AtomicStore>(
    engine: &Engine<S, TestHost>,
    operation: OperationId,
    expected: u64,
) -> Result<(), StoreError> {
    match engine.status(&operation).await?.state {
        OperationState::Completed { result } => check(
            serde_json::from_slice::<CallResult<u64>>(&result).map_err(StoreError::backend)?
                == CallResult::Answered(expected),
            "repaired call must return the original answer",
        ),
        _ => Err(StoreError::backend(
            "retryable call did not complete after repair",
        )),
    }
}

/// Leave actual journal data for native file reopen and workerd runtime reopen.
pub async fn stage_no_effect<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<TestHost>,
    request: &'static str,
) -> Result<OperationId, StoreError> {
    let before = host.effects.load(Ordering::SeqCst);
    refuse_next_call(&host)?;
    let engine = call_engine::<S, true, false>(store.clone(), host.clone())?;
    let operation = engine
        .start::<CallTask<S, true, false>>(&key(request), 23)
        .await
        .map_err(StoreError::backend)?;
    let attempt = pending_attempt(store.as_ref(), operation).await?;
    check(
        attempt.reach == CallReach::NoEffect,
        "known refusal must record NoEffect",
    )?;
    check(
        attempt.retry_reason == Some(no_effect_reason()),
        "reason payload must survive the store codec",
    )?;
    check(
        host.effects.load(Ordering::SeqCst) == before,
        "retryable receiver must not write an effect",
    )?;
    Ok(operation)
}
pub async fn finish_no_effect<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<TestHost>,
    operation: OperationId,
) -> Result<(), StoreError> {
    let persisted = pending_attempt(store.as_ref(), operation).await?;
    check(
        persisted.reach == CallReach::NoEffect
            && persisted.retry_reason == Some(no_effect_reason()),
        "reopened journal must preserve NoEffect and its whole reason",
    )?;
    let before = host.effects.load(Ordering::SeqCst);
    let engine = call_engine::<S, true, false>(store, host.clone())?;
    host.advance(1_000);
    engine.drive_due().await?;
    answered(&engine, operation, 46).await?;
    check(
        host.effects.load(Ordering::SeqCst) == before + 1,
        "safe non-deduplicating retry must execute exactly once",
    )
}
pub async fn safe_retry_after_engine_restart<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError> {
    let operation = stage_no_effect(store.clone(), host.clone(), "retryable-nondup").await?;
    finish_no_effect(store, host, operation).await
}
pub async fn not_sent_after_no_effect_stays_pending<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError> {
    let operation = stage_no_effect(store.clone(), host.clone(), "retryable-unsent-bound").await?;
    let engine = call_engine::<S, true, false>(store.clone(), host.clone())?;
    host.retry_calls.store(true, Ordering::SeqCst);
    host.advance(1_000); // The configured unsent bound is 20 ms.
    engine.drive_due().await?;
    let attempt = pending_attempt(store.as_ref(), operation).await?;
    check(
        attempt.reach == CallReach::NoEffect,
        "NotSent must not turn proven receiver reach into NeverSent",
    )?;
    check(
        matches!(
            engine.status(&operation).await?.state,
            OperationState::Running { .. }
        ),
        "NoEffect must never qualify for the unsent timeout",
    )?;
    host.retry_calls.store(false, Ordering::SeqCst);
    host.advance(1_000);
    engine.drive_due().await?;
    answered(&engine, operation, 46).await
}
pub async fn crash_during_no_effect_retry_is_uncertain<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError> {
    let operation =
        stage_no_effect(store.clone(), host.clone(), "retryable-dispatch-crash").await?;
    let engine = call_engine::<S, true, false>(store.clone(), host.clone())?;
    let deliveries = host.deliveries.load(Ordering::SeqCst);
    let effects = host.effects.load(Ordering::SeqCst);
    host.hang_after_effect.store(true, Ordering::SeqCst);
    host.advance(1_000);
    let mut drive = Box::pin(engine.drive_due());
    poll_fn(|cx| match drive.as_mut().poll(cx) {
        Poll::Pending if host.deliveries.load(Ordering::SeqCst) == deliveries + 1 => {
            Poll::Ready(Ok(()))
        }
        Poll::Pending => Poll::Pending,
        Poll::Ready(_) => Poll::Ready(Err(StoreError::backend(
            "retry did not reach the held dispatch",
        ))),
    })
    .await?;
    drop(drive); // Outside any store callback, after Dispatching committed.
    check(
        pending_attempt(store.as_ref(), operation).await?.reach == CallReach::Dispatching,
        "retry from NoEffect must commit Dispatching before sending",
    )?;
    host.hang_after_effect.store(false, Ordering::SeqCst);
    host.advance(30_001);
    let restarted = call_engine::<S, true, false>(store, host.clone())?;
    restarted.drive_due().await?;
    check(
        matches!(
            restarted.status(&operation).await?.state,
            OperationState::Failed {
                reason: OperationFailure::Uncertain { .. }
            }
        ),
        "interrupted non-deduplicating retry must become Uncertain",
    )?;
    check(
        host.deliveries.load(Ordering::SeqCst) == deliveries + 1,
        "uncertain retry must not send again",
    )?;
    check(
        host.effects.load(Ordering::SeqCst) == effects + 1,
        "held dispatch must produce one effect in the scripted receiver",
    )
}
pub async fn retryable_preserves_earlier_possible_effect<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError>
where
    for<'t> S::Scope<'t>: BusinessScope,
{
    let receiving = store.clone();
    *host.receiver_dispatch.lock().map_err(StoreError::backend)? =
        Some(Arc::new(move |call, input| {
            let receiving = receiving.clone();
            Box::pin(async move {
                receiving
                    .transaction(move |scope| {
                        Box::pin(async move {
                            let receipt = format!(
                                "retry-real-receipt:{}",
                                serde_json::to_string(&call).map_err(StoreError::backend)?
                            );
                            if let Some(answer) = scope.read_counter(&receipt).await? {
                                return Ok((
                                    serde_json::to_vec(&answer).map_err(StoreError::backend)?,
                                    false,
                                ));
                            }
                            let offered: u64 =
                                serde_json::from_slice(&input).map_err(StoreError::backend)?;
                            let count =
                                scope.read_counter("retry-real-effects").await?.unwrap_or(0);
                            scope.write_counter("retry-real-effects", count + 1).await?;
                            scope.write_counter(&receipt, offered * 2).await?;
                            Ok((
                                serde_json::to_vec(&(offered * 2)).map_err(StoreError::backend)?,
                                true,
                            ))
                        })
                    })
                    .await
                    .map_err(|_| CallFailure::AnswerLost)
            })
        }));
    let result = lost_then_retryable(store.clone(), host.clone()).await;
    host.receiver_dispatch
        .lock()
        .map_err(StoreError::backend)?
        .take();
    result?;
    store
        .transaction(|scope| {
            Box::pin(async move {
                check(
                    scope.read_counter("retry-real-effects").await? == Some(1),
                    "the real business transaction and receipt must commit exactly one effect",
                )
            })
        })
        .await
}

async fn lost_then_retryable<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError> {
    let engine = call_engine::<S, true, true>(store.clone(), host.clone())?;
    let effects = host.effects.load(Ordering::SeqCst);
    host.lose_answers.store(1, Ordering::SeqCst);
    let operation = engine
        .start::<CallTask<S, true, true>>(&key("lost-then-retryable"), 31)
        .await
        .map_err(StoreError::backend)?;
    check(
        host.effects.load(Ordering::SeqCst) == effects + 1,
        "first answer loss follows the scripted receiver effect",
    )?;
    refuse_next_call(&host)?;
    host.advance(11);
    engine.drive_due().await?;
    let attempt = pending_attempt(store.as_ref(), operation).await?;
    check(
        attempt.reach == CallReach::MayHaveReached,
        "latest no-new-effect answer cannot erase an earlier possible effect",
    )?;
    check(
        attempt.retry_reason == Some(no_effect_reason()),
        "latest retry metadata is retained independently from reach history",
    )?;
    host.advance(21);
    engine.drive_due().await?;
    answered(&engine, operation, 62).await?;
    check(
        host.effects.load(Ordering::SeqCst) == effects + 1,
        "deduplicating repair must reconcile the original call identity",
    )
}
