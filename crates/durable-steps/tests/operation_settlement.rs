#![allow(clippy::panic_in_result_fn)]
mod support;

use durable_steps::{
    AtomicStore, CallTarget, Engine, JournalScope, OperationContext, OperationFailure, OperationId,
    OperationState, RequestKey, SettleError, StartRefusal, StepName, StoreError, Task,
    volatile_store::VolatileStore,
};
use futures_util::lock::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::Poll,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Store = VolatileStore<support::TestHost>;
type TestEngine = Engine<Store, support::TestHost>;
type Fixture = (
    TestEngine,
    Arc<Store>,
    Arc<support::TestHost>,
    Arc<Mutex<()>>,
);

#[derive(Serialize, Deserialize)]
struct ReadTarget;
impl CallTarget for ReadTarget {
    const KIND: &'static str = "settlement-read";
    type Request = u64;
    type Answer = u64;
}
struct Reader;
impl Task for Reader {
    const KIND: &'static str = "settlement-reader";
    const VERSION: u32 = 1;
    type Store = Store;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = Store>>(
        &self,
        context: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        Ok(context
            .call(
                StepName::new("read").map_err(|error| OperationFailure::Storage {
                    error: StoreError::backend(error),
                })?,
                ReadTarget,
                *input,
            )
            .await?)
    }
}
struct Decision;
impl Task for Decision {
    const KIND: &'static str = "settlement-decision";
    const VERSION: u32 = 1;
    type Store = Store;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = Store>>(
        &self,
        _: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        match *input {
            0 => Err(OperationFailure::Refused {
                code: "declined".into(),
                detail: "fixture decision".into(),
            }),
            1 => Err(OperationFailure::Retry { after_ms: 100 }),
            value => Ok(value),
        }
    }
}
fn key(name: &str) -> TestResult<RequestKey> {
    Ok(RequestKey::new(name)?)
}
fn fixture() -> TestResult<Fixture> {
    let host = support::TestHost::new();
    let gate = Arc::new(Mutex::new(()));
    let call_gate = gate.clone();
    *host.receiver_dispatch.lock().map_err(StoreError::backend)? =
        Some(Arc::new(move |_, input| {
            let gate = call_gate.clone();
            Box::pin(async move {
                let _released = gate.lock().await;
                Ok((input, true))
            })
        }));
    let store = Arc::new(Store::new(host.clone()));
    let mut engine = Engine::new(store.clone(), host.clone());
    engine.register(Reader)?;
    engine.register(Decision)?;
    Ok((engine, store, host, gate))
}
async fn admit_reader(store: &Store, name: &str, input: u64) -> TestResult<OperationId> {
    let key = key(name)?;
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move {
                scope
                    .admit::<Reader>(&key, &input)
                    .await
                    .map(|admitted| admitted.id())
                    .map_err(StoreError::backend)
            })
        })
        .await?)
}
async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
}

#[test]
fn a_conflicting_request_keeps_its_admission_refusal_and_original_identity() -> TestResult {
    futures_executor::block_on(async {
        let (engine, _, _, _) = fixture()?;
        let request = key("conflicting-input")?;
        let original = engine.start_and_settle::<Decision>(&request, 17).await?;
        assert!(
            matches!(engine.start_and_settle::<Decision>(&request, 19).await,
            Err(SettleError::Admission(StartRefusal::RequestConflict { existing })) if existing==original.id)
        );
        Ok(())
    })
}

#[test]
fn settlers_join_one_call_and_cancelling_a_waiter_keeps_the_drive_live() -> TestResult {
    futures_executor::block_on(async {
        let (engine, store, host, gate) = fixture()?;
        let held = gate.lock().await;
        let operation = admit_reader(&store, "joined", 17).await?;
        let mut drive = Box::pin(engine.drive_due());
        assert!(poll_once(drive.as_mut()).await.is_pending());
        assert_eq!(host.deliveries.load(Ordering::SeqCst), 1);

        let request = key("joined")?;
        let mut cancelled = Box::pin(engine.start_and_settle::<Reader>(&request, 17));
        let mut settled = Box::pin(engine.start_and_settle::<Reader>(&request, 17));
        assert!(poll_once(cancelled.as_mut()).await.is_pending());
        assert!(poll_once(settled.as_mut()).await.is_pending());
        drop(cancelled);
        drop(held);
        let (report, status) = futures_util::future::join(drive, settled).await;
        assert_eq!(report?.completed, [operation]);
        assert_eq!(
            status?.state,
            OperationState::Completed {
                result: b"17".to_vec()
            }
        );
        assert_eq!(host.deliveries.load(Ordering::SeqCst), 1);
        assert_eq!(host.effects.load(Ordering::SeqCst), 1);
        engine.start_and_settle::<Reader>(&request, 17).await?;
        assert_eq!(host.deliveries.load(Ordering::SeqCst), 1);
        Ok(())
    })
}

#[test]
fn settling_a_request_does_not_drive_an_unrelated_due_call() -> TestResult {
    futures_executor::block_on(async {
        let (engine, store, host, gate) = fixture()?;
        let _held = gate.lock().await;
        admit_reader(&store, "unrelated", 7).await?;
        let request = key("own-decision")?;
        let mut settled = Box::pin(engine.start_and_settle::<Decision>(&request, 23));
        let Poll::Ready(status) = poll_once(settled.as_mut()).await else {
            return Err("request drove an unrelated held call".into());
        };
        assert_eq!(
            status?.state,
            OperationState::Completed {
                result: b"23".to_vec()
            }
        );
        assert_eq!(host.deliveries.load(Ordering::SeqCst), 0);
        Ok(())
    })
}

#[test]
fn completed_results_do_not_wait_for_unrelated_execution_capacity() -> TestResult {
    futures_executor::block_on(async {
        let (engine, store, host, gate) = fixture()?;
        let request = key("already-finished")?;
        engine.start_and_settle::<Decision>(&request, 31).await?;
        let backoff = key("already-in-backoff")?;
        engine.start_and_settle::<Decision>(&backoff, 1).await?;
        let _held = gate.lock().await;
        for input in 0..8 {
            admit_reader(&store, &format!("held-{input}"), input).await?;
        }
        let mut drive = Box::pin(engine.drive_due());
        assert!(poll_once(drive.as_mut()).await.is_pending());
        assert_eq!(host.deliveries.load(Ordering::SeqCst), 8);
        let mut settled = Box::pin(engine.start_and_settle::<Decision>(&request, 31));
        let Poll::Ready(status) = poll_once(settled.as_mut()).await else {
            return Err("a completed request waited for unrelated engine capacity".into());
        };
        assert_eq!(
            status?.state,
            OperationState::Completed {
                result: b"31".to_vec()
            }
        );
        let mut retry = Box::pin(engine.start_and_settle::<Decision>(&backoff, 1));
        let Poll::Ready(status) = poll_once(retry.as_mut()).await else {
            return Err("scheduled backoff waited for unrelated engine capacity".into());
        };
        assert!(matches!(
            status?.state,
            OperationState::Running {
                attempt: 1,
                drive: None,
                ..
            }
        ));
        Ok(())
    })
}

#[test]
fn scheduled_backoff_and_terminal_failure_return_without_a_timer() -> TestResult {
    futures_executor::block_on(async {
        let (engine, _, host, _) = fixture()?;
        let retry = engine
            .start_and_settle::<Decision>(&key("backoff")?, 1)
            .await?;
        assert!(
            matches!(retry.state, OperationState::Running { attempt: 1, drive: None, next_attempt_at } if next_attempt_at.unix_millis() > 1_000)
        );
        assert!(matches!(
            engine
                .start_and_settle::<Decision>(&key("backoff")?, 1)
                .await?
                .state,
            OperationState::Running { attempt: 1, .. }
        ));
        assert_eq!(
            engine
                .start_and_settle::<Decision>(&key("failed")?, 0)
                .await?
                .state,
            OperationState::Failed {
                reason: OperationFailure::Refused {
                    code: "declined".into(),
                    detail: "fixture decision".into()
                }
            }
        );
        assert_eq!(host.deliveries.load(Ordering::SeqCst), 0);
        Ok(())
    })
}

#[test]
fn cancelled_drive_releases_waiters_but_its_unexpired_lease_is_not_executed() -> TestResult {
    futures_executor::block_on(async {
        let (engine, store, host, gate) = fixture()?;
        let held = gate.lock().await;
        admit_reader(&store, "cancelled-drive", 43).await?;
        let request = key("cancelled-drive")?;
        let mut drive = Box::pin(engine.drive_due());
        assert!(poll_once(drive.as_mut()).await.is_pending());
        let mut settled = Box::pin(engine.start_and_settle::<Reader>(&request, 43));
        assert!(poll_once(settled.as_mut()).await.is_pending());
        drop(drive);
        let Poll::Ready(status) = poll_once(settled.as_mut()).await else {
            return Err("cancelled drive left its completion waiter stuck".into());
        };
        assert!(matches!(
            status?.state,
            OperationState::Running { drive: Some(_), .. }
        ));
        assert_eq!(host.deliveries.load(Ordering::SeqCst), 1);
        drop(settled);
        host.advance(31_000);
        drop(held);
        assert!(matches!(
            engine.start_and_settle::<Reader>(&request, 43).await?.state,
            OperationState::Completed { .. }
        ));
        assert_eq!(host.deliveries.load(Ordering::SeqCst), 2);
        assert_eq!(host.effects.load(Ordering::SeqCst), 1);
        Ok(())
    })
}
