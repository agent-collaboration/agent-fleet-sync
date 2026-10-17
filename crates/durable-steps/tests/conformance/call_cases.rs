use super::{BusinessScope, check, key, task_fixtures::CallTask};
use crate::support::TestHost;
use durable_steps::{
    AtomicStore, CallResult, Engine, JournalScope, OperationFailure, OperationId, OperationState,
    StoreError,
    storage::{JournalStorage, StepState},
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

fn engine<S: AtomicStore, const B: bool, const D: bool>(
    store: Arc<S>,
    host: Arc<TestHost>,
    wait: bool,
) -> Result<Engine<S, TestHost>, StoreError> {
    let mut engine = Engine::new(store, host);
    engine.register(CallTask::<S, B, D> {
        wait_once: Arc::new(AtomicBool::new(wait)),
        store: PhantomData,
    })?;
    Ok(engine)
}
async fn answer<S: AtomicStore>(
    engine: &Engine<S, TestHost>,
    id: OperationId,
) -> Result<CallResult<u64>, StoreError> {
    match engine.status(&id).await?.state {
        OperationState::Completed { result } => {
            serde_json::from_slice(&result).map_err(StoreError::backend)
        }
        _ => Err(StoreError::backend("expected completed call task")),
    }
}
pub async fn never_sent_replays<S>(store: Arc<S>, host: Arc<TestHost>) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let engine = engine::<S, true, true>(Arc::clone(&store), Arc::clone(&host), true)?;
    host.retry_calls.store(true, Ordering::SeqCst);
    let id = engine
        .start::<CallTask<S, true, true>>(&key("never-sent"), 9)
        .await
        .map_err(StoreError::backend)?;
    host.advance(21);
    engine.drive_due().await?;
    let before = host.deliveries.load(Ordering::SeqCst);
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let step = scope
                    .step(id, 0)
                    .await?
                    .ok_or_else(|| StoreError::backend("missing saved NeverSent"))?;
                match step.state {
                    StepState::CallDone { output, .. } => check(
                        serde_json::from_slice::<CallResult<u64>>(&output)
                            .map_err(StoreError::backend)?
                            == CallResult::NeverSent { last_seen: None },
                        "NeverSent is saved before next step",
                    ),
                    _ => check(false, "bounded call must save NeverSent"),
                }
            })
        })
        .await?;
    host.retry_calls.store(false, Ordering::SeqCst);
    host.advance(41);
    engine.drive_due().await?;
    check(
        answer(&engine, id).await? == CallResult::NeverSent { last_seen: None },
        "NeverSent replays as the same result",
    )?;
    check(
        host.deliveries.load(Ordering::SeqCst) == before,
        "NeverSent replay never dispatches",
    )
}
pub async fn lost_answer_prevents_give_up<S>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let engine = engine::<S, true, true>(store, Arc::clone(&host), false)?;
    host.lose_answers.store(1, Ordering::SeqCst);
    let effects = host.effects.load(Ordering::SeqCst);
    let id = engine
        .start::<CallTask<S, true, true>>(&key("lost-before-unsent"), 11)
        .await
        .map_err(StoreError::backend)?;
    host.retry_calls.store(true, Ordering::SeqCst);
    host.advance(21);
    engine.drive_due().await?;
    check(
        matches!(
            engine.status(&id).await?.state,
            OperationState::Running { .. }
        ),
        "AnswerLost then NotSent past bound keeps retrying",
    )?;
    host.retry_calls.store(false, Ordering::SeqCst);
    host.advance(41);
    engine.drive_due().await?;
    check(
        answer(&engine, id).await? == CallResult::Answered(22),
        "late answer is saved",
    )?;
    check(
        host.effects.load(Ordering::SeqCst) == effects + 1,
        "deduplicating receiver applied one effect",
    )
}
pub async fn answer_after_not_sent<S>(store: Arc<S>, host: Arc<TestHost>) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let engine = engine::<S, true, true>(store, Arc::clone(&host), false)?;
    host.retry_calls.store(true, Ordering::SeqCst);
    let id = engine
        .start::<CallTask<S, true, true>>(&key("answer-after-unsent"), 13)
        .await
        .map_err(StoreError::backend)?;
    host.retry_calls.store(false, Ordering::SeqCst);
    host.advance(11);
    engine.drive_due().await?;
    check(
        answer(&engine, id).await? == CallResult::Answered(26),
        "NotSent does not suppress a subsequent answer",
    )
}
pub async fn non_deduplicating_lost_answer<S>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let engine = engine::<S, false, false>(store, Arc::clone(&host), false)?;
    host.lose_answers.store(1, Ordering::SeqCst);
    let before = host.deliveries.load(Ordering::SeqCst);
    let id = engine
        .start::<CallTask<S, false, false>>(&key("nondup-lost"), 17)
        .await
        .map_err(StoreError::backend)?;
    check(
        matches!(engine.status(&id).await?.state, OperationState::Failed { reason: OperationFailure::Uncertain { step } } if step.as_str() == "double"),
        "non-deduplicating lost answer records named Uncertain",
    )?;
    host.advance(100);
    engine.drive_due().await?;
    check(
        host.deliveries.load(Ordering::SeqCst) == before + 1,
        "non-deduplicating lost answer is never resent",
    )
}
async fn abandon_in_flight<S: AtomicStore, const D: bool>(
    store: &S,
    host: &TestHost,
    engine: &Engine<S, TestHost>,
    request_key: &'static str,
) -> Result<OperationId, StoreError> {
    let id = store
        .transaction(move |scope| {
            Box::pin(async move {
                Ok(scope
                    .admit::<CallTask<S, true, D>>(&key(request_key), &19)
                    .await
                    .map_err(StoreError::backend)?
                    .id())
            })
        })
        .await?;
    host.hang_after_effect.store(true, Ordering::SeqCst);
    let before = host.deliveries.load(Ordering::SeqCst);
    let mut drive = Box::pin(engine.drive_due());
    // Drive the future until the deliberately blocked dispatch. No sleep or
    // assumed timing: dropping it leaves the committed in-flight marker.
    poll_fn(|cx| match drive.as_mut().poll(cx) {
        Poll::Pending if host.deliveries.load(Ordering::SeqCst) == before + 1 => {
            Poll::Ready(Ok(()))
        }
        Poll::Pending => Poll::Pending,
        Poll::Ready(result) => {
            Poll::Ready(result.and(Err(StoreError::backend("drive did not block at dispatch"))))
        }
    })
    .await?;
    check(
        host.deliveries.load(Ordering::SeqCst) == before + 1,
        "cancellation reached the dispatch boundary",
    )?;
    drop(drive);
    host.hang_after_effect.store(false, Ordering::SeqCst);
    host.advance(30_001);
    Ok(id)
}
pub async fn in_flight_recovery<S>(store: Arc<S>, host: Arc<TestHost>) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let engine = engine::<S, true, true>(Arc::clone(&store), Arc::clone(&host), false)?;
    let effects = host.effects.load(Ordering::SeqCst);
    let id = abandon_in_flight::<S, true>(&store, &host, &engine, "in-flight").await?;
    host.retry_calls.store(true, Ordering::SeqCst);
    engine.drive_due().await?;
    check(
        matches!(
            engine.status(&id).await?.state,
            OperationState::Running { .. }
        ),
        "in-flight recovery cannot falsely save NeverSent",
    )?;
    host.retry_calls.store(false, Ordering::SeqCst);
    host.advance(21);
    engine.drive_due().await?;
    check(
        answer(&engine, id).await? == CallResult::Answered(38),
        "in-flight retry recovers receiver answer",
    )?;
    check(
        host.effects.load(Ordering::SeqCst) == effects + 1,
        "in-flight retry preserves receiver call identity",
    )
}
pub async fn non_deduplicating_in_flight<S>(
    store: Arc<S>,
    host: Arc<TestHost>,
) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let engine = engine::<S, true, false>(Arc::clone(&store), Arc::clone(&host), false)?;
    let id = abandon_in_flight::<S, false>(&store, &host, &engine, "nondup-in-flight").await?;
    let before = host.deliveries.load(Ordering::SeqCst);
    engine.drive_due().await?;
    check(
        matches!(
            engine.status(&id).await?.state,
            OperationState::Failed {
                reason: OperationFailure::Uncertain { .. }
            }
        ),
        "non-deduplicating in-flight recovery is uncertain",
    )?;
    check(
        host.deliveries.load(Ordering::SeqCst) == before,
        "non-deduplicating in-flight dispatch is not repeated",
    )
}
