#![allow(clippy::panic_in_result_fn)]
#[path = "support/pruned_settlement.rs"]
mod pruned_support;
mod support;

use durable_steps::{
    AtomicStore, BoxFuture, CallTarget, Engine, JournalScope, OperationContext, OperationFailure,
    OperationId, OperationState, RequestKey, Resume, SettleError, StartRefusal, StepName,
    StoreError, Task, storage::JournalStorage, volatile_store::VolatileStore,
};
use futures_util::lock::Mutex;
use pruned_support::{FaultStore, SettlementHost, WakeBarrier};
use serde::{Deserialize, Serialize};
use std::{
    future::{Future, poll_fn},
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::Poll,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type MemoryStore = VolatileStore<SettlementHost>;

#[derive(Serialize, Deserialize)]
struct ReadTarget;
impl CallTarget for ReadTarget {
    const KIND: &'static str = "pruned-settlement-read";
    type Request = u64;
    type Answer = u64;
}
struct PrunedDecision<S>(PhantomData<fn() -> S>);
impl<S: AtomicStore> Task for PrunedDecision<S> {
    const KIND: &'static str = "pruned-settlement-decision";
    const VERSION: u32 = 1;
    const KEEP_RESULT: bool = false;
    type Store = S;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        _: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        if *input == 0 {
            Err(declined())
        } else {
            Ok(*input)
        }
    }
}
struct PrunedReader<S>(PhantomData<fn() -> S>);
impl<S: AtomicStore> Task for PrunedReader<S> {
    const KIND: &'static str = "pruned-settlement-reader";
    const VERSION: u32 = 1;
    const KEEP_RESULT: bool = false;
    type Store = S;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        context: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        let name = StepName::new("read").map_err(|error| OperationFailure::InvalidPayload {
            message: error.to_string(),
        })?;
        Ok(context.call(name, ReadTarget, *input).await?)
    }
}
struct PrunedResume<S>(PhantomData<fn() -> S>);
impl<S: AtomicStore> Task for PrunedResume<S> {
    const KIND: &'static str = "pruned-settlement-resume";
    const VERSION: u32 = 1;
    const KEEP_RESULT: bool = false;
    type Store = S;
    type Input = ();
    type Output = u64;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        _: &mut C,
        _: &(),
    ) -> Result<u64, OperationFailure> {
        Err(declined())
    }
    fn before_resume<'scope, 'transaction>(
        &'scope self,
        _: &'scope mut S::Scope<'transaction>,
        _: &'scope RequestKey,
    ) -> BoxFuture<'scope, Result<Resume<u64>, StoreError>> {
        Box::pin(async { Ok(Resume::Finish(73)) })
    }
}
fn declined() -> OperationFailure {
    OperationFailure::Refused {
        code: "declined".into(),
        detail: "pruned fixture decision".into(),
    }
}
fn key(name: &str) -> TestResult<RequestKey> {
    Ok(RequestKey::new(name)?)
}
fn engine<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<SettlementHost>,
) -> Result<Engine<S, SettlementHost>, StoreError> {
    let mut engine = Engine::new(store, host);
    engine.register(PrunedDecision(PhantomData))?;
    engine.register(PrunedReader(PhantomData))?;
    engine.register(PrunedResume(PhantomData))?;
    Ok(engine)
}
fn memory() -> (Arc<MemoryStore>, Arc<SettlementHost>) {
    let host = SettlementHost::new();
    (Arc::new(MemoryStore::new(host.clone())), host)
}
async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
}
fn hold_calls(host: &SettlementHost) -> TestResult<Arc<Mutex<()>>> {
    let gate = Arc::new(Mutex::new(()));
    let calls = gate.clone();
    *host
        .base
        .receiver_dispatch
        .lock()
        .map_err(StoreError::backend)? = Some(Arc::new(move |_, input| {
        let calls = calls.clone();
        Box::pin(async move {
            let _released = calls.lock().await;
            Ok((input, true))
        })
    }));
    Ok(gate)
}
async fn admit_reader<S: AtomicStore>(store: &S, request: RequestKey) -> TestResult<OperationId> {
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move {
                Ok(scope
                    .admit::<PrunedReader<S>>(&request, &7)
                    .await
                    .map_err(StoreError::backend)?
                    .id())
            })
        })
        .await?)
}
async fn decision_case<S: AtomicStore>(
    store: Arc<S>,
    host: Arc<SettlementHost>,
    input: u64,
    expected: OperationState,
) -> TestResult {
    let engine = engine(store, host)?;
    let status = engine
        .start_and_settle::<PrunedDecision<S>>(&key("own")?, input)
        .await?;
    assert_eq!(status.state, expected);
    assert!(
        matches!(engine.status(&status.id).await, Err(StoreError::MissingOperation { id }) if id == status.id)
    );
    Ok(())
}
async fn resume_case<S: AtomicStore>(store: Arc<S>, host: Arc<SettlementHost>) -> TestResult {
    let engine = engine(store, host)?;
    let status = engine
        .start_and_settle::<PrunedResume<S>>(&key("resume")?, ())
        .await?;
    assert_eq!(
        status.state,
        OperationState::Completed {
            result: b"73".to_vec()
        }
    );
    assert!(matches!(
        engine.status(&status.id).await,
        Err(StoreError::MissingOperation { .. })
    ));
    Ok(())
}
#[test]
fn own_pruned_completion_is_confirmed_without_retaining_a_row() -> TestResult {
    futures_executor::block_on(async {
        let (store, host) = memory();
        decision_case(
            store,
            host,
            17,
            OperationState::Completed {
                result: b"17".to_vec(),
            },
        )
        .await
    })
}
#[test]
fn a_pruned_failure_is_failed_rather_than_completed() -> TestResult {
    futures_executor::block_on(async {
        let (store, host) = memory();
        decision_case(
            store,
            host,
            0,
            OperationState::Failed { reason: declined() },
        )
        .await
    })
}
#[test]
fn before_resume_finish_publishes_its_committed_pruned_result() -> TestResult {
    futures_executor::block_on(async {
        let (store, host) = memory();
        resume_case(store, host).await
    })
}
#[cfg(feature = "turso")]
async fn native_decision(input: u64, expected: OperationState) -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = SettlementHost::new();
    let store = Arc::new(
        durable_steps::turso_store::TursoStore::open_and_migrate(
            directory.path().join("pruned.db"),
            host.clone(),
        )
        .await?,
    );
    decision_case(store, host, input, expected).await
}
#[cfg(feature = "turso")]
#[tokio::test]
async fn native_pruned_completion_returns_its_confirmed_result() -> TestResult {
    native_decision(
        17,
        OperationState::Completed {
            result: b"17".to_vec(),
        },
    )
    .await
}
#[cfg(feature = "turso")]
#[tokio::test]
async fn native_pruned_failure_keeps_its_failure_reason() -> TestResult {
    native_decision(0, OperationState::Failed { reason: declined() }).await
}
#[cfg(feature = "turso")]
#[tokio::test]
async fn native_before_resume_finish_keeps_its_committed_receipt() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = SettlementHost::new();
    let store = Arc::new(
        durable_steps::turso_store::TursoStore::open_and_migrate(
            directory.path().join("resume.db"),
            host.clone(),
        )
        .await?,
    );
    resume_case(store, host).await
}
#[test]
fn admission_is_attached_before_its_wake_can_drive_and_prune() -> TestResult {
    futures_executor::block_on(async {
        let (store, host) = memory();
        let engine = engine(store, host.clone())?;
        let barrier = WakeBarrier::new();
        let held = barrier.hold.lock().await;
        *host.wake.lock().map_err(StoreError::backend)? = Some(barrier.clone());
        let request = key("wake-race")?;
        let mut settled =
            Box::pin(engine.start_and_settle::<PrunedDecision<MemoryStore>>(&request, 17));
        assert!(poll_once(settled.as_mut()).await.is_pending());
        assert!(barrier.entered.load(Ordering::SeqCst));
        let report = engine.drive_due().await?;
        assert_eq!(report.completed.len(), 1);
        let operation = report.completed[0];
        assert!(matches!(
            engine.status(&operation).await,
            Err(StoreError::MissingOperation { .. })
        ));
        drop(held);
        let status = settled.await?;
        assert_eq!(status.id, operation);
        assert_eq!(
            status.state,
            OperationState::Completed {
                result: b"17".to_vec()
            }
        );
        Ok(())
    })
}
#[test]
fn existing_admission_reads_a_joined_receipt_after_the_row_is_pruned() -> TestResult {
    futures_executor::block_on(async {
        let (store, host) = memory();
        let engine = engine(store.clone(), host.clone())?;
        let request = key("existing")?;
        let operation = admit_reader(store.as_ref(), request.clone()).await?;
        let calls = hold_calls(&host)?;
        let held_call = calls.lock().await;
        let mut drive = Box::pin(engine.drive_due());
        assert!(poll_once(drive.as_mut()).await.is_pending());
        let barrier = WakeBarrier::new();
        let held_wake = barrier.hold.lock().await;
        *host.wake.lock().map_err(StoreError::backend)? = Some(barrier.clone());
        let mut settled =
            Box::pin(engine.start_and_settle::<PrunedReader<MemoryStore>>(&request, 7));
        assert!(poll_once(settled.as_mut()).await.is_pending());
        assert!(barrier.entered.load(Ordering::SeqCst));
        drop(held_call);
        assert_eq!(drive.await?.completed, [operation]);
        assert!(matches!(
            engine.status(&operation).await,
            Err(StoreError::MissingOperation { .. })
        ));
        drop(held_wake);
        let status = settled.await?;
        assert_eq!(status.id, operation);
        assert_eq!(
            status.state,
            OperationState::Completed {
                result: b"7".to_vec()
            }
        );
        assert_eq!(host.base.effects.load(Ordering::SeqCst), 1);
        Ok(())
    })
}
#[test]
fn another_engine_pruning_without_a_receipt_stays_missing() -> TestResult {
    futures_executor::block_on(async {
        let (store, host) = memory();
        let first = engine(store.clone(), host.clone())?;
        let second = engine(store, host.clone())?;
        let barrier = WakeBarrier::new();
        let held = barrier.hold.lock().await;
        *host.wake.lock().map_err(StoreError::backend)? = Some(barrier.clone());
        let request = key("foreign")?;
        let mut settled =
            Box::pin(first.start_and_settle::<PrunedDecision<MemoryStore>>(&request, 17));
        assert!(poll_once(settled.as_mut()).await.is_pending());
        assert!(barrier.entered.load(Ordering::SeqCst));
        assert_eq!(second.drive_due().await?.completed.len(), 1);
        drop(held);
        assert!(matches!(
            settled.await,
            Err(SettleError::Execution(StoreError::MissingOperation { .. }))
        ));
        Ok(())
    })
}
#[test]
fn cancelling_a_settler_keeps_the_joined_drive_and_receipt_live() -> TestResult {
    futures_executor::block_on(async {
        let (store, host) = memory();
        let engine = engine(store.clone(), host.clone())?;
        let request = key("cancel-settler")?;
        let operation = admit_reader(store.as_ref(), request.clone()).await?;
        let calls = hold_calls(&host)?;
        let held = calls.lock().await;
        let mut drive = Box::pin(engine.drive_due());
        assert!(poll_once(drive.as_mut()).await.is_pending());
        let mut cancelled =
            Box::pin(engine.start_and_settle::<PrunedReader<MemoryStore>>(&request, 7));
        let mut settled =
            Box::pin(engine.start_and_settle::<PrunedReader<MemoryStore>>(&request, 7));
        assert!(poll_once(cancelled.as_mut()).await.is_pending());
        assert!(poll_once(settled.as_mut()).await.is_pending());
        drop(cancelled);
        drop(held);
        let (report, status) = futures_util::future::join(drive, settled).await;
        assert_eq!(report?.completed, [operation]);
        assert_eq!(
            status?.state,
            OperationState::Completed {
                result: b"7".to_vec()
            }
        );
        assert_eq!(host.base.effects.load(Ordering::SeqCst), 1);
        Ok(())
    })
}
#[test]
fn cancelling_a_drive_never_fabricates_a_terminal_receipt() -> TestResult {
    futures_executor::block_on(async {
        let (store, host) = memory();
        let engine = engine(store.clone(), host.clone())?;
        let request = key("cancel-drive")?;
        admit_reader(store.as_ref(), request.clone()).await?;
        let calls = hold_calls(&host)?;
        let held = calls.lock().await;
        let mut drive = Box::pin(engine.drive_due());
        assert!(poll_once(drive.as_mut()).await.is_pending());
        let mut settled =
            Box::pin(engine.start_and_settle::<PrunedReader<MemoryStore>>(&request, 7));
        assert!(poll_once(settled.as_mut()).await.is_pending());
        drop(drive);
        let Poll::Ready(status) = poll_once(settled.as_mut()).await else {
            return Err("cancelled drive left the settler blocked".into());
        };
        assert!(matches!(
            status?.state,
            OperationState::Running { drive: Some(_), .. }
        ));
        drop(settled);
        host.base.advance(31_000);
        drop(held);
        let status = engine
            .start_and_settle::<PrunedReader<MemoryStore>>(&request, 7)
            .await?;
        assert_eq!(
            status.state,
            OperationState::Completed {
                result: b"7".to_vec()
            }
        );
        assert_eq!(host.base.deliveries.load(Ordering::SeqCst), 2);
        assert_eq!(host.base.effects.load(Ordering::SeqCst), 1);
        Ok(())
    })
}
#[test]
fn rolled_back_admission_drops_its_attachment_without_a_completion() -> TestResult {
    futures_executor::block_on(async {
        let (inner, host) = memory();
        let store = FaultStore::new(inner);
        let engine = engine(store.clone(), host.clone())?;
        store.faults.rollback_next.store(true, Ordering::SeqCst);
        let request = key("rollback")?;
        assert!(matches!(
            engine
                .start_and_settle::<PrunedReader<FaultStore<MemoryStore>>>(&request, 7)
                .await,
            Err(SettleError::Admission(StartRefusal::Storage(
                StoreError::Aborted { .. }
            )))
        ));
        assert!(
            store
                .transaction(|scope| Box::pin(async move {
                    scope
                        .due(durable_steps::SyncTime::from_unix_millis(1_000))
                        .await
                }))
                .await?
                .is_empty()
        );
        let status = engine
            .start_and_settle::<PrunedReader<FaultStore<MemoryStore>>>(&request, 7)
            .await?;
        assert_eq!(
            status.state,
            OperationState::Completed {
                result: b"14".to_vec()
            }
        );
        assert_eq!(host.base.effects.load(Ordering::SeqCst), 1);
        Ok(())
    })
}
#[test]
fn post_commit_rearm_error_reaches_the_owner_while_a_joiner_reads_the_receipt() -> TestResult {
    futures_executor::block_on(async {
        let (inner, host) = memory();
        let store = FaultStore::new(inner);
        let engine = engine(store.clone(), host.clone())?;
        let request = key("rearm-failure")?;
        let operation = admit_reader(store.as_ref(), request.clone()).await?;
        *store.faults.tracked.lock().map_err(StoreError::backend)? = Some(operation);
        store.faults.fail_after_prune.store(true, Ordering::SeqCst);
        let calls = hold_calls(&host)?;
        let held = calls.lock().await;
        let mut own =
            Box::pin(engine.start_and_settle::<PrunedReader<FaultStore<MemoryStore>>>(&request, 7));
        assert!(poll_once(own.as_mut()).await.is_pending());
        let mut joined =
            Box::pin(engine.start_and_settle::<PrunedReader<FaultStore<MemoryStore>>>(&request, 7));
        assert!(poll_once(joined.as_mut()).await.is_pending());
        drop(held);
        let (own, joined) = futures_util::future::join(own, joined).await;
        assert!(
            matches!(own, Err(SettleError::Execution(StoreError::Backend { message })) if message == "post-commit rearm")
        );
        let status = joined?;
        assert_eq!(status.id, operation);
        assert_eq!(
            status.state,
            OperationState::Completed {
                result: b"7".to_vec()
            }
        );
        assert_eq!(host.base.effects.load(Ordering::SeqCst), 1);
        assert!(matches!(
            engine.status(&operation).await,
            Err(StoreError::MissingOperation { .. })
        ));
        Ok(())
    })
}
