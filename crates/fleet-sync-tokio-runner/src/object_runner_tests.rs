#![allow(clippy::panic_in_result_fn)]
use crate::*;
use durable_steps::{
    AtomicStore, CallFailure, CallTarget, Engine, JournalScope, OperationContext, OperationFailure,
    OperationState, RequestKey, StepName, StoreError, Task,
};
use runtime_primitives::{Clock, SystemClock, WakeScheduler};
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, mpsc, watch};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct Calls {
    entered: mpsc::UnboundedSender<u64>,
    release_first: Notify,
    first_entered: watch::Sender<bool>,
    counts: [AtomicUsize; 2],
}
impl CallRouter for Calls {
    async fn dispatch(&self, call: CallEnvelope) -> Result<serde_json::Value, CallFailure> {
        let input =
            serde_json::from_value::<u64>(call.request).map_err(|_| CallFailure::AnswerLost)?;
        if input == 2 {
            // Establish the held-first-call order even if independent tasks
            // reach the carrier in a different executor scheduling order.
            let mut first = self.first_entered.subscribe();
            let _entered = first
                .wait_for(|entered| *entered)
                .await
                .map_err(|_| CallFailure::AnswerLost)?;
        }
        let index = usize::try_from(input - 1).map_err(|_| CallFailure::AnswerLost)?;
        self.counts
            .get(index)
            .ok_or(CallFailure::AnswerLost)?
            .fetch_add(1, Ordering::SeqCst);
        self.entered
            .send(input)
            .map_err(|_| CallFailure::AnswerLost)?;
        if input == 1 {
            self.first_entered.send_replace(true);
            self.release_first.notified().await;
        }
        Ok(serde_json::json!(input))
    }
}
#[derive(Serialize, Deserialize)]
struct Target;
impl CallTarget for Target {
    const KIND: &'static str = "runner-independent-call";
    type Request = u64;
    type Answer = u64;
}
type NativeHost = TokioHost<Calls>;
type NativeStore = TokioObject<NativeHost>;
struct Reader;
impl Task for Reader {
    const KIND: &'static str = "runner-independent-reader";
    const VERSION: u32 = 1;
    type Store = NativeStore;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = NativeStore>>(
        &self,
        context: &mut C,
        input: &u64,
    ) -> Result<u64, OperationFailure> {
        Ok(context
            .call(
                StepName::new("read").map_err(|error| OperationFailure::Storage {
                    error: StoreError::backend(error),
                })?,
                Target,
                *input,
            )
            .await?)
    }
}
async fn admit(store: &NativeStore, input: u64) -> TestResult<durable_steps::OperationId> {
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move {
                let key =
                    RequestKey::new(format!("reader-{input}")).map_err(StoreError::backend)?;
                Ok(scope
                    .admit::<Reader>(&key, &input)
                    .await
                    .map_err(StoreError::backend)?
                    .id())
            })
        })
        .await?)
}
async fn independent_readers(admit_second_later: bool, use_runner: bool) -> TestResult {
    let directory = tempfile::tempdir()?;
    let (entered, mut entries) = mpsc::unbounded_channel();
    let calls = Arc::new(Calls {
        entered,
        release_first: Notify::new(),
        first_entered: watch::channel(false).0,
        counts: [0.into(), 0.into()],
    });
    let host = Arc::new(TokioHost::new(calls.clone(), SystemClock));
    let objects = ObjectStore::open(directory.path()).await?;
    let store = objects
        .object(
            ObjectKey::new("independent-readers")?,
            ObjectOptions::default(),
            host.clone(),
        )
        .await?;
    let mut engine = Engine::new(store.clone(), host.clone());
    engine.register(Reader)?;
    let engine = Arc::new(engine);
    let first = admit(&store, 1).await?;
    let mut second = None;
    if !admit_second_later {
        second = Some(admit(&store, 2).await?);
    }
    let runner = TokioRunner::new(engine.clone(), host.clone());
    let (shutdown, receiver) = watch::channel(false);
    let running = if use_runner {
        tokio::spawn(async move { runner.run(receiver).await })
    } else {
        let engine = engine.clone();
        tokio::spawn(async move { engine.drive_due().await.map(|_| ()) })
    };
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), entries.recv()).await?,
        Some(1)
    );
    if admit_second_later {
        second = Some(admit(&store, 2).await?);
        host.wake_at(host.now()).await?;
    }
    // Only dispatch2 can satisfy this signal while dispatch1 is held. The
    // deadline bounds the regression, and does not establish readiness.
    let independent = tokio::time::timeout(Duration::from_secs(2), entries.recv()).await;
    calls.release_first.notify_one();
    shutdown.send_replace(true);
    tokio::time::timeout(Duration::from_secs(3), running).await???;
    // Settle any admitted work left by a shutdown after a completion.
    engine.drive_due().await?;
    for id in [first, second.ok_or("missing reader2")?] {
        assert!(matches!(
            engine.status(&id).await?.state,
            OperationState::Completed { .. }
        ));
    }
    objects.close().await?;
    assert_eq!(
        independent.ok().flatten(),
        Some(2),
        "independent reader stalled behind held external call"
    );
    assert_eq!(calls.counts[0].load(Ordering::SeqCst), 1);
    assert_eq!(calls.counts[1].load(Ordering::SeqCst), 1);
    Ok(())
}
#[tokio::test]
async fn due_readers_dispatch_while_another_external_call_is_held() -> TestResult {
    independent_readers(false, true).await
}
#[tokio::test]
async fn newly_woken_reader_dispatches_while_another_external_call_is_held() -> TestResult {
    independent_readers(true, true).await
}

#[tokio::test]
async fn overlapping_drive_does_not_execute_a_held_call_twice() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (entered, mut entries) = mpsc::unbounded_channel();
    let calls = Arc::new(Calls {
        entered,
        release_first: Notify::new(),
        first_entered: watch::channel(false).0,
        counts: [0.into(), 0.into()],
    });
    let host = Arc::new(TokioHost::new(calls.clone(), SystemClock));
    let objects = ObjectStore::open(directory.path()).await?;
    let store = objects
        .object(
            ObjectKey::new("duplicate-drive")?,
            ObjectOptions::default(),
            host.clone(),
        )
        .await?;
    let mut engine = Engine::new(store.clone(), host);
    engine.register(Reader)?;
    let engine = Arc::new(engine);
    let id = admit(&store, 1).await?;
    let driving = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.drive_due().await })
    };
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), entries.recv()).await?,
        Some(1)
    );
    let duplicate = tokio::time::timeout(Duration::from_secs(2), engine.drive_due()).await??;
    assert!(duplicate.completed.is_empty() && duplicate.failed.is_empty());
    assert_eq!(calls.counts[0].load(Ordering::SeqCst), 1);
    calls.release_first.notify_one();
    let report = tokio::time::timeout(Duration::from_secs(3), driving).await???;
    assert_eq!(report.completed, vec![id]);
    engine.drive_due().await?;
    assert_eq!(calls.counts[0].load(Ordering::SeqCst), 1);
    assert!(matches!(
        engine.status(&id).await?.state,
        OperationState::Completed { .. }
    ));
    objects.close().await?;
    Ok(())
}

#[tokio::test]
async fn one_engine_drive_dispatches_independent_due_calls_concurrently() -> TestResult {
    independent_readers(false, false).await
}

#[tokio::test]
async fn shutdown_drains_an_external_call_before_the_runner_returns() -> TestResult {
    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };
    let directory = tempfile::tempdir()?;
    let (entered, mut entries) = mpsc::unbounded_channel();
    let calls = Arc::new(Calls {
        entered,
        release_first: Notify::new(),
        first_entered: watch::channel(false).0,
        counts: [0.into(), 0.into()],
    });
    let host = Arc::new(TokioHost::new(calls.clone(), SystemClock));
    let objects = ObjectStore::open(directory.path()).await?;
    let store = objects
        .object(
            ObjectKey::new("shutdown-drain")?,
            ObjectOptions::default(),
            host.clone(),
        )
        .await?;
    let mut engine = Engine::new(store.clone(), host.clone());
    engine.register(Reader)?;
    let engine = Arc::new(engine);
    let id = admit(&store, 1).await?;
    let runner = TokioRunner::new(engine.clone(), host);
    let (shutdown, stopped) = watch::channel(false);
    let mut running = Box::pin(runner.run(stopped));
    tokio::time::timeout(Duration::from_secs(2),async {
        tokio::select! {
            ended = &mut running => Err(format!("runner stopped before dispatch: {ended:?}")),
            entered = entries.recv() => if entered == Some(1) {Ok(())} else {Err("missing held call".into())},
        }
    }).await??;
    shutdown.send_replace(true);
    // Poll the actual runner after its shutdown input is ready. The held call
    // must keep it pending; no sleep or scheduler turn establishes readiness.
    let polled = poll_fn(|context| Poll::Ready(running.as_mut().poll(context))).await;
    let draining = polled.is_pending();
    calls.release_first.notify_one();
    if let Poll::Ready(result) = polled {
        result?;
    } else {
        tokio::time::timeout(Duration::from_secs(2), running).await??;
    }
    assert!(draining, "runner abandoned its in-flight call on shutdown");
    assert!(matches!(
        engine.status(&id).await?.state,
        OperationState::Completed { .. }
    ));
    assert_eq!(calls.counts[0].load(Ordering::SeqCst), 1);
    objects.close().await?;
    Ok(())
}
