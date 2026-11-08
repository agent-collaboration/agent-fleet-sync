//! Event-driven resumption uses the real journal and an unchanged injected clock.
use super::{BusinessScope, check, key, step, task_fixtures::CallTask};
use crate::support::TestHost;
use durable_steps::{
    AtomicStore, CallFailure, CallId, CallRetryReason, Engine, JournalScope, OperationContext,
    OperationFailure, OperationId, OperationState, StoreError, Task,
    storage::{JournalStorage, OperationExecution},
};
use runtime_primitives::{Clock, IdSource};
use std::{
    future::{Future, poll_fn},
    marker::PhantomData,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
};

struct CounterTask<S>(PhantomData<fn() -> S>);
impl<S: AtomicStore> Task for CounterTask<S>
where
    for<'t> S::Scope<'t>: BusinessScope,
{
    const KIND: &'static str = "event-resume-counter";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        context: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        let value = *input;
        Ok(context
            .local(step("counter"), move |scope| {
                Box::pin(async move {
                    let name = format!("event-resume-{value}");
                    let previous = scope.read_counter(&name).await?.unwrap_or(0);
                    scope.write_counter(&name, previous + 1).await?;
                    Ok(value)
                })
            })
            .await?)
    }
}

pub async fn named_operation_only<S>(store: Arc<S>, host: Arc<TestHost>) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    // Arrange: neither due operation has been claimed.
    let (first, second, second_before) = store
        .transaction(|scope| {
            Box::pin(async move {
                let first = scope
                    .admit::<CounterTask<S>>(&key("event-first"), &701)
                    .await
                    .map_err(StoreError::backend)?
                    .id();
                let second = scope
                    .admit::<CounterTask<S>>(&key("event-second"), &702)
                    .await
                    .map_err(StoreError::backend)?
                    .id();
                Ok((first, second, scope.operation(second).await?))
            })
        })
        .await?;
    let mut engine = Engine::new(store.clone(), host.clone());
    engine.register(CounterTask::<S>(PhantomData))?;

    // Act and assert: the named operation commits its effect; the peer is byte-for-byte unchanged.
    let report = engine.drive_one(first).await?;
    check(
        report.completed == vec![first] && report.failed.is_empty(),
        "drive_one completes only its named operation",
    )?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                check(
                    scope.operation(second).await? == second_before
                        && scope.step_count(second).await? == 0,
                    "drive_one leaves the other due operation unclaimed without a step",
                )?;
                check(
                    scope.read_counter("event-resume-701").await? == Some(1)
                        && scope.read_counter("event-resume-702").await?.is_none(),
                    "only the named operation writes its business effect",
                )
            })
        })
        .await?;
    check(
        engine.drive_one(first).await?.yielded == vec![first],
        "finished operation yields",
    )?;
    let unknown = OperationId::new(host.new_id()).map_err(StoreError::backend)?;
    check(
        engine.drive_one(unknown).await?.yielded == vec![unknown],
        "unknown operation yields",
    )?;
    check(
        engine.drive_one(second).await?.completed == vec![second],
        "untouched peer can be driven separately",
    )
}

pub async fn resume_backoff<S>(store: Arc<S>, host: Arc<TestHost>) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    // Arrange: first dispatch returns Retryable; the next dispatch returns an answer.
    let calls = Arc::new(Mutex::new(Vec::<CallId>::new()));
    let seen = calls.clone();
    *host.receiver_dispatch.lock().map_err(StoreError::backend)? =
        Some(Arc::new(move |call, _| {
            let seen = seen.clone();
            Box::pin(async move {
                let mut calls = seen.lock().map_err(|_| CallFailure::AnswerLost)?;
                calls.push(call);
                if calls.len() == 1 {
                    Err(CallFailure::Retryable {
                        reason: CallRetryReason {
                            code: "event_pending".into(),
                            detail: "reply pending".into(),
                        },
                    })
                } else {
                    Ok((b"14".to_vec(), true))
                }
            })
        }));
    let now = host.now();
    let id = store
        .transaction(|scope| {
            Box::pin(async move {
                Ok(scope
                    .admit::<CallTask<S, false, true>>(&key("event-backoff"), &7)
                    .await
                    .map_err(StoreError::backend)?
                    .id())
            })
        })
        .await?;
    let mut engine = Engine::new(store.clone(), host.clone());
    engine.register(CallTask::<S, false, true> {
        wait_once: Arc::new(AtomicBool::new(false)),
        store: PhantomData,
    })?;
    check(
        engine.drive_one(id).await?.retried == vec![id],
        "Retryable call schedules backoff",
    )?;
    let before = engine.status(&id).await?;
    check(
        matches!(before.state, OperationState::Running { next_attempt_at, .. } if next_attempt_at > now),
        "backoff is in the future",
    )?;
    check(
        engine.drive_one(id).await?.yielded == vec![id],
        "drive_one cannot bypass backoff",
    )?;
    check(
        calls.lock().map_err(StoreError::backend)?.len() == 1,
        "yield does not redispatch",
    )?;

    // A failed event transaction must roll back both the event and the lowered deadline.
    let rollback = store
        .transaction(move |scope| {
            Box::pin(async move {
                scope.write_counter("event-reply", 1).await?;
                check(
                    scope.make_due(id, now).await?,
                    "rollback fixture lowers deadline",
                )?;
                Err::<(), _>(StoreError::Aborted {
                    reason: "event rollback".into(),
                })
            })
        })
        .await;
    match rollback {
        Err(StoreError::Aborted { reason }) if reason == "event rollback" => {}
        Err(error) => return Err(error),
        Ok(()) => return Err(StoreError::backend("event transaction failed to roll back")),
    }
    check(
        engine.status(&id).await? == before,
        "rolled-back make_due preserves the original schedule",
    )?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                check(
                    scope.read_counter("event-reply").await?.is_none(),
                    "event write rolls back with deadline",
                )?;
                scope.write_counter("event-reply", 1).await?;
                let mut expected = scope
                    .operation(id)
                    .await?
                    .ok_or(StoreError::MissingOperation { id })?;
                match &mut expected.execution {
                    OperationExecution::Running {
                        next_attempt_at, ..
                    } => *next_attempt_at = now,
                    _ => return Err(StoreError::backend("expected pending operation")),
                }
                check(
                    scope.make_due(id, now).await?,
                    "make_due lowers future retry deadline",
                )?;
                check(
                    scope.operation(id).await? == Some(expected),
                    "make_due changes only the retry deadline",
                )?;
                check(
                    !scope.make_due(id, now).await?,
                    "already-due operation is unchanged",
                )
            })
        })
        .await?;
    let report = engine.drive_one(id).await?;
    check(
        report.completed == vec![id] && report.failed.is_empty(),
        "event resumes backoff immediately",
    )?;
    check(
        host.now() == now,
        "event resumption does not advance the clock",
    )?;
    {
        let delivered = calls.lock().map_err(StoreError::backend)?;
        check(
            delivered.len() == 2 && delivered.first() == delivered.last(),
            "event retry preserves its original CallId",
        )?;
    }
    *host.receiver_dispatch.lock().map_err(StoreError::backend)? = None;
    let unknown = OperationId::new(host.new_id()).map_err(StoreError::backend)?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                check(
                    !scope.make_due(id, now).await? && !scope.make_due(unknown, now).await?,
                    "finished and unknown operations are unchanged",
                )
            })
        })
        .await
}

pub async fn preserve_live_lease<S>(store: Arc<S>, host: Arc<TestHost>) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let id = store
        .transaction(|scope| {
            Box::pin(async move {
                Ok(scope
                    .admit::<CallTask<S, false, true>>(&key("event-live-lease"), &9)
                    .await
                    .map_err(StoreError::backend)?
                    .id())
            })
        })
        .await?;
    let make_engine = || -> Result<Engine<S, TestHost>, StoreError> {
        let mut engine = Engine::new(store.clone(), host.clone());
        engine.register(CallTask::<S, false, true> {
            wait_once: Arc::new(AtomicBool::new(false)),
            store: PhantomData,
        })?;
        Ok(engine)
    };
    let first = make_engine()?;
    let competitor = make_engine()?;
    host.hang_after_effect.store(true, Ordering::SeqCst);
    let deliveries = host.deliveries.load(Ordering::SeqCst);
    let mut drive = Box::pin(first.drive_one(id));
    poll_fn(|context| match drive.as_mut().poll(context) {
        Poll::Pending if host.deliveries.load(Ordering::SeqCst) == deliveries + 1 => {
            Poll::Ready(Ok(()))
        }
        Poll::Pending => Poll::Pending,
        Poll::Ready(_) => Poll::Ready(Err(StoreError::backend("expected held dispatch"))),
    })
    .await?;
    check(
        host.deliveries.load(Ordering::SeqCst) == deliveries + 1,
        "first drive reached dispatch with its lease held",
    )?;
    let mut expected = first.status(&id).await?;
    let now = host.now();
    check(
        matches!(&expected.state, OperationState::Running { drive: Some(lease), .. } if lease.until > now),
        "first drive owns a live lease",
    )?;
    // An earlier event timestamp exercises an actual deadline update while
    // the first drive still owns the lease; the clock itself does not move.
    let earlier = durable_steps::SyncTime::from_unix_millis(now.unix_millis() - 1);
    store
        .transaction(move |scope| {
            Box::pin(async move {
                check(
                    !scope.make_due(id, now).await?,
                    "already-due leased work is unchanged",
                )?;
                check(
                    scope.make_due(id, earlier).await?,
                    "earlier event lowers deadline under the live lease",
                )
            })
        })
        .await?;
    if let OperationState::Running {
        next_attempt_at, ..
    } = &mut expected.state
    {
        *next_attempt_at = earlier;
    }
    let mut second_drive = Box::pin(competitor.drive_one(id));
    let competing_report = poll_fn(|context| match second_drive.as_mut().poll(context) {
        Poll::Ready(result) => Poll::Ready(result),
        Poll::Pending if host.deliveries.load(Ordering::SeqCst) > deliveries + 1 => {
            Poll::Ready(Err(StoreError::backend(
                "make_due bypassed the live lease and dispatched twice",
            )))
        }
        Poll::Pending => Poll::Pending,
    })
    .await?;
    drop(second_drive);
    check(
        competing_report.yielded == vec![id],
        "make_due cannot bypass a live lease in another engine",
    )?;
    check(
        first.status(&id).await? == expected
            && host.deliveries.load(Ordering::SeqCst) == deliveries + 1,
        "competing drive preserves the lease and does not dispatch",
    )?;
    drop(drive);
    host.hang_after_effect.store(false, Ordering::SeqCst);
    host.advance(30_001);
    check(
        competitor.drive_one(id).await?.completed == vec![id],
        "recovery can claim after lease expiry",
    )
}
