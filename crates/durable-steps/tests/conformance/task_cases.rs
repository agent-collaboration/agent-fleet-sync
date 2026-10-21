use super::{BusinessScope, check, key, task_fixtures::*};
use crate::support::TestHost;
use durable_steps::{
    AtomicStore, DriveLease, Engine, JournalScope, OperationFailure, OperationState, StartRefusal,
    StoreError, Task,
    storage::{JournalStorage, OperationExecution, OperationOutcome},
};

pub async fn running_keys<S>(store: Arc<S>, host: Arc<TestHost>) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let now = host.now();
    let live = store
        .transaction(move |scope| {
            Box::pin(async move {
                let live = scope
                    .admit::<Child<S>>(&key("live-key"), &1)
                    .await
                    .map_err(StoreError::backend)?
                    .id();
                let other = scope
                    .admit::<OldVersion<S>>(&key("other-kind"), &"other".into())
                    .await
                    .map_err(StoreError::backend)?
                    .id();
                for (name, outcome) in [
                    (
                        "completed-key",
                        OperationOutcome::Completed {
                            result: b"1".to_vec(),
                        },
                    ),
                    (
                        "failed-key",
                        OperationOutcome::Failed {
                            reason: OperationFailure::InvalidPayload {
                                message: "fixture failure".into(),
                            },
                        },
                    ),
                ] {
                    let id = scope
                        .admit::<Child<S>>(&key(name), &1)
                        .await
                        .map_err(StoreError::backend)?
                        .id();
                    let token = durable_steps::Uuid::from_u128(0x019eef000000700080000000000186a3);
                    check(
                        scope
                            .claim(id, now, DriveLease { token, until: now })
                            .await?
                            .is_some(),
                        "fixture claim",
                    )?;
                    scope.finish(id, token, outcome, now, true).await?;
                }
                check(
                    scope.running_request_keys(Child::<S>::KIND).await? == vec![key("live-key")],
                    "running keys exclude completed/failed and other kinds",
                )?;
                check(
                    scope.running_request_keys(OldVersion::<S>::KIND).await?
                        == vec![key("other-kind")],
                    "running keys preserve kind",
                )?;
                check(
                    scope.running_request_keys("absent-kind").await?.is_empty(),
                    "unknown kind has no work",
                )?;
                Ok((live, other))
            })
        })
        .await?;
    let aborted = store
        .transaction(|scope| {
            Box::pin(async move {
                scope
                    .admit::<Child<S>>(&key("aborted-live"), &1)
                    .await
                    .map_err(StoreError::backend)?;
                let mut keys = scope.running_request_keys(Child::<S>::KIND).await?;
                keys.sort();
                let mut expected = vec![key("live-key"), key("aborted-live")];
                expected.sort();
                check(
                    keys == expected,
                    "running read sees uncommitted admission in its transaction",
                )?;
                Err::<(), _>(StoreError::Aborted {
                    reason: "rollback read fixture".into(),
                })
            })
        })
        .await;
    check(
        matches!(aborted, Err(StoreError::Aborted { .. })),
        "rollback fixture aborts",
    )?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                check(
                    scope.running_request_keys(Child::<S>::KIND).await? == vec![key("live-key")],
                    "rolled back admission is excluded",
                )?;
                for id in [live.0, live.1] {
                    let token = durable_steps::Uuid::from_u128(0x019eef000000700080000000000186a4);
                    check(
                        scope
                            .claim(id, now, DriveLease { token, until: now })
                            .await?
                            .is_some(),
                        "live fixture claim",
                    )?;
                    scope
                        .finish(
                            id,
                            token,
                            OperationOutcome::Completed {
                                result: b"1".to_vec(),
                            },
                            now,
                            true,
                        )
                        .await?;
                }
                check(
                    scope
                        .running_request_keys(Child::<S>::KIND)
                        .await?
                        .is_empty(),
                    "no running keys after finish",
                )
            })
        })
        .await
}
use runtime_primitives::Clock;
use std::{
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

pub async fn admission_and_rollback<S>(store: Arc<S>, host: Arc<TestHost>) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let now = host.now();
    let aborted = store
        .transaction(|scope| {
            Box::pin(async move {
                scope.write_counter("rolled-back", 1).await?;
                scope
                    .admit::<Child<S>>(&key("rollback"), &7)
                    .await
                    .map_err(StoreError::backend)?;
                Err::<(), _>(StoreError::Aborted {
                    reason: "requested rollback".into(),
                })
            })
        })
        .await;
    check(
        matches!(aborted, Err(StoreError::Aborted { .. })),
        "callback error is returned",
    )?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                check(
                    scope.read_counter("rolled-back").await?.is_none(),
                    "business write rolled back",
                )?;
                check(
                    scope.due(now).await?.is_empty() && scope.earliest().await?.is_none(),
                    "journal and wake source rolled back",
                )?;
                scope.write_counter("committed", 23).await?;
                Ok(())
            })
        })
        .await?;
    let first = store.transaction(|scope| Box::pin(async move {
        check(scope.read_counter("committed").await? == Some(23), "read after commit")?;
        let first = scope.admit::<Child<S>>(&key("exact-key"), &9_007_199_254_740_993).await.map_err(StoreError::backend)?.id();
        let repeat = scope.admit::<Child<S>>(&key("exact-key"), &9_007_199_254_740_993).await.map_err(StoreError::backend)?.id();
        check(first == repeat, "same key and digest returns existing")?;
        check(matches!(scope.admit::<Child<S>>(&key("exact-key"), &9_007_199_254_740_992).await, Err(StartRefusal::RequestConflict { existing }) if existing == first), "adjacent exact integers conflict")?;
        let record = scope.operation(first).await?.ok_or(StoreError::MissingOperation { id: first })?;
        match record.execution {
            OperationExecution::Running { input, .. } => check(serde_json::from_slice::<u64>(&input).map_err(StoreError::backend)? == 9_007_199_254_740_993, "integer input remains exact")?,
            _ => check(false, "admitted operation is running")?,
        }
        Ok(first)
    })).await?;
    let mut engine = Engine::new(store, host);
    engine.register(Child::<S>(PhantomData))?;
    let report = engine.drive_due().await?;
    check(
        report.completed.contains(&first),
        &format!("admitted task completes: {report:?}"),
    )
}

pub async fn stale_token<S>(store: Arc<S>, host: Arc<TestHost>) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let now = host.now();
    let id = store
        .transaction(move |scope| {
            Box::pin(async move {
                let id = scope
                    .admit::<Child<S>>(&key("stale"), &1)
                    .await
                    .map_err(StoreError::backend)?
                    .id();
                let old = durable_steps::Uuid::from_u128(100_001);
                scope
                    .claim(
                        id,
                        now,
                        DriveLease {
                            token: old,
                            until: now,
                        },
                    )
                    .await?;
                let replacement = durable_steps::Uuid::from_u128(100_002);
                check(
                    scope
                        .claim(
                            id,
                            now,
                            DriveLease {
                                token: replacement,
                                until: now,
                            },
                        )
                        .await?
                        .is_some(),
                    "expired drive is replaced",
                )?;
                check(
                    matches!(
                        scope
                            .finish(
                                id,
                                old,
                                OperationOutcome::Completed { result: vec![] },
                                now,
                                true
                            )
                            .await,
                        Err(StoreError::StaleDriveToken { .. })
                    ),
                    "stale completion refused",
                )?;
                scope
                    .finish(
                        id,
                        replacement,
                        OperationOutcome::Completed {
                            result: b"1".to_vec(),
                        },
                        now,
                        true,
                    )
                    .await?;
                Ok(id)
            })
        })
        .await?;
    check(
        matches!(
            Engine::new(store, host).status(&id).await?.state,
            OperationState::Completed { .. }
        ),
        "replacement owns completion",
    )
}

pub async fn parent_and_retention<S>(store: Arc<S>, host: Arc<TestHost>) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let invocations = Arc::new(AtomicU64::new(0));
    let mut engine = Engine::new(Arc::clone(&store), Arc::clone(&host));
    engine.register(Child::<S>(PhantomData))?;
    engine.register(Parent::<S> {
        fail_once: Arc::new(AtomicBool::new(true)),
        invocations: Arc::clone(&invocations),
        store: PhantomData,
    })?;
    let id = engine
        .start::<Parent<S>>(&key("parent"), 7)
        .await
        .map_err(StoreError::backend)?;
    expect_probe_rollback(
        store
            .transaction(|scope| {
                Box::pin(async move {
                    check(
                        scope.read_counter("parent").await?.is_none(),
                        "parent business write rolled back",
                    )?;
                    check(
                        matches!(
                            scope.admit::<Child<S>>(&key("parent-child"), &99).await,
                            Ok(durable_steps::Admitted::New(_))
                        ),
                        "child admission rolled back",
                    )?;
                    Err::<(), _>(StoreError::Aborted {
                        reason: "probe rollback".into(),
                    })
                })
            })
            .await,
    )?;
    host.lose_answers.store(1, Ordering::SeqCst);
    host.advance(11);
    engine.drive_due().await?;
    let mut reopened = Engine::new(Arc::clone(&store), Arc::clone(&host));
    reopened.register(Child::<S>(PhantomData))?;
    reopened.register(Parent::<S> {
        fail_once: Arc::new(AtomicBool::new(false)),
        invocations: Arc::clone(&invocations),
        store: PhantomData,
    })?;
    host.advance(21);
    reopened.drive_due().await?;
    check(
        invocations.load(Ordering::SeqCst) == 2,
        "saved local callback is not even synchronously invoked on replay",
    )?;
    let repeated = reopened
        .start::<Parent<S>>(&key("parent"), 7)
        .await
        .map_err(StoreError::backend)?;
    check(
        repeated == id && invocations.load(Ordering::SeqCst) == 2,
        "compact finished row answers repeats",
    )?;
    check(
        matches!(reopened.start::<Parent<S>>(&key("parent"), 8).await, Err(StartRefusal::RequestConflict { existing }) if existing == id),
        "compact finished row rejects changed input by its retained digest",
    )?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                check(
                    scope.read_counter("parent").await? == Some(1),
                    "business effect committed once",
                )?;
                check(
                    scope.step_count(id).await? == 0,
                    "terminal transaction deletes every step",
                )?;
                let row = scope
                    .operation(id)
                    .await?
                    .ok_or(StoreError::MissingOperation { id })?;
                check(
                    matches!(row.execution, OperationExecution::Completed { .. }),
                    "compact completed row cannot carry input",
                )
            })
        })
        .await
}

pub async fn failure_hook<S>(store: Arc<S>, host: Arc<TestHost>) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let id = store
        .transaction(|scope| {
            Box::pin(async move {
                Ok(scope
                    .admit::<OldVersion<S>>(&key("old-version"), &"old input is not a u64".into())
                    .await
                    .map_err(StoreError::backend)?
                    .id())
            })
        })
        .await?;
    let mut engine = Engine::new(Arc::clone(&store), Arc::clone(&host));
    let resume_invocations = Arc::new(AtomicU64::new(0));
    engine.register(NewVersion::<S> {
        fail_hook: Arc::new(AtomicBool::new(true)),
        resume_invocations: Arc::clone(&resume_invocations),
        store: PhantomData,
    })?;
    engine.register(Child::<S>(PhantomData))?;
    check(
        matches!(engine.drive_due().await, Err(StoreError::Aborted { .. })),
        "failure hook rollback is returned",
    )?;
    expect_probe_rollback(
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    check(
                        scope.read_counter("domain-failed").await?.is_none(),
                        "hook business write rolled back",
                    )?;
                    check(
                        matches!(
                            scope.operation(id).await?.map(|r| r.execution),
                            Some(OperationExecution::Running { .. })
                        ),
                        "terminal journal rolled back with hook",
                    )?;
                    check(
                        matches!(
                            scope.admit::<Child<S>>(&key("owed-child"), &99).await,
                            Ok(durable_steps::Admitted::New(_))
                        ),
                        "owed child rolled back",
                    )?;
                    Err::<(), _>(StoreError::Aborted {
                        reason: "probe rollback".into(),
                    })
                })
            })
            .await,
    )?;
    host.advance(30_001);
    let report = engine.drive_due().await?;
    check(
        matches!(report.failed.as_slice(), [(failed, OperationFailure::VersionMismatch { recorded:0, registered:1 })] if *failed == id),
        "version mismatch is terminal typed failure",
    )?;
    check(
        resume_invocations.load(Ordering::SeqCst) == 0,
        "version mismatch must precede even synchronous resume-hook invocation",
    )?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                check(
                    scope.read_counter("domain-failed").await? == Some(1),
                    "domain failure committed",
                )?;
                check(
                    matches!(
                        scope.admit::<Child<S>>(&key("owed-child"), &17).await,
                        Ok(durable_steps::Admitted::Existing(_))
                    ),
                    "owed child committed with domain and failure",
                )?;
                check(
                    scope.step_count(id).await? == 0,
                    "failed operation has no steps",
                )
            })
        })
        .await?;
    engine.drive_due().await?;
    Ok(())
}

pub async fn delete_at_end<S>(store: Arc<S>, host: Arc<TestHost>) -> Result<(), StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let mut engine = Engine::new(Arc::clone(&store), host);
    engine.register(DeleteAtEnd::<S>(PhantomData))?;
    let id = engine
        .start::<DeleteAtEnd<S>>(&key("delete"), 7)
        .await
        .map_err(StoreError::backend)?;
    check(
        matches!(
            engine.status(&id).await,
            Err(StoreError::MissingOperation { .. })
        ),
        "delete-at-end has no operation",
    )?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                check(
                    scope.step_count(id).await? == 0,
                    "delete-at-end has no steps",
                )
            })
        })
        .await?;
    let next = engine
        .start::<DeleteAtEnd<S>>(&key("delete"), 7)
        .await
        .map_err(StoreError::backend)?;
    check(
        next != id,
        "owner opted out of journal request-key retention",
    )
}

fn expect_probe_rollback(result: Result<(), StoreError>) -> Result<(), StoreError> {
    match result {
        Err(StoreError::Aborted { reason }) if reason == "probe rollback" => Ok(()),
        Err(error) => Err(error),
        Ok(()) => Err(StoreError::backend("probe must roll back")),
    }
}
