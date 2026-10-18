//! Child process for run-native.mjs. Cuts are after real commits; no fake storage.
#[path = "../../../tests/durable-steps/journey_task.rs"]
mod journey_task;

use durable_steps::{
    AtomicStore, BoxFuture, CallFailure, CallId, CallTarget, Engine, Host, OperationId, RequestKey,
    StoreError, Uuid,
    storage::{JournalStorage, OperationExecution, StepState, TransactionScope},
    turso_store::{TursoScope, TursoStore},
};
use journey_task::{JourneyInput, JourneyScope, JourneyTask};
use runtime_primitives::{
    Clock, IdSource, SyncTime, SystemClock, TokioWakeTimer, UuidV7Ids, WakeFailure, WakeScheduler,
};
use std::{io::Write, marker::PhantomData, path::Path, sync::Arc, time::Duration};

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum Checkpoint {
    Admission,
    Claim,
    First,
    Intent,
    Receiver,
    Answer,
    Last,
    Completed,
}

struct NoCalls;
impl Clock for NoCalls {
    fn now(&self) -> SyncTime {
        SystemClock.now()
    }
}
impl IdSource for NoCalls {
    fn new_id(&self) -> Uuid {
        UuidV7Ids.new_id()
    }
}
impl WakeScheduler for NoCalls {
    async fn wake_at(&self, _at: SyncTime) -> Result<(), WakeFailure> {
        Ok(())
    }
}
impl Host for NoCalls {
    async fn dispatch<C: CallTarget>(
        &self,
        _target: C,
        _request: C::Request,
        _call: CallId,
    ) -> Result<C::Answer, CallFailure> {
        Err(CallFailure::AnswerLost)
    }
}
struct JourneyHost {
    receiver: Arc<TursoStore<NoCalls>>,
    timer: TokioWakeTimer,
    cut: Option<Checkpoint>,
    receiver_ready: tokio::sync::Notify,
}
impl Clock for JourneyHost {
    fn now(&self) -> SyncTime {
        SystemClock.now()
    }
}
impl IdSource for JourneyHost {
    fn new_id(&self) -> Uuid {
        UuidV7Ids.new_id()
    }
}
impl WakeScheduler for JourneyHost {
    async fn wake_at(&self, at: SyncTime) -> Result<(), WakeFailure> {
        self.timer.wake_at(at).await
    }
}

struct CounterRow {
    value: i64,
}
struct ReceiptRow {
    request: i64,
    answer: i64,
}
struct IdRow {
    operation_id: String,
}
async fn read_counter<H: Host>(
    scope: &mut TursoScope<'_, H>,
    name: &str,
) -> Result<u64, StoreError> {
    let row = sqlx_turso::query_file_as!(
        CounterRow,
        "tests/conformance/queries/read_counter.sql",
        name
    )
    .fetch_optional(scope.executor())
    .await?;
    u64::try_from(row.map_or(0, |row| row.value)).map_err(StoreError::backend)
}
async fn increment<H: Host>(scope: &mut TursoScope<'_, H>, name: &str) -> Result<u64, StoreError> {
    let next = read_counter(scope, name)
        .await?
        .checked_add(1)
        .ok_or_else(|| StoreError::backend("fixture counter overflow"))?;
    let value = i64::try_from(next).map_err(StoreError::backend)?;
    sqlx_turso::query_file!("tests/conformance/queries/write_counter.sql", name, value)
        .execute(scope.executor())
        .await?;
    Ok(next)
}
impl<H: Host> JourneyScope for TursoScope<'_, H> {
    async fn increment_effect(&mut self, name: &str) -> Result<u64, StoreError> {
        increment(self, name).await
    }
}
impl Host for JourneyHost {
    async fn dispatch<C: CallTarget>(
        &self,
        _target: C,
        request: C::Request,
        call: CallId,
    ) -> Result<C::Answer, CallFailure> {
        let request: i64 =
            serde_json::from_slice(&serde_json::to_vec(&request).map_err(lost)?).map_err(lost)?;
        let call = serde_json::to_string(&call).map_err(lost)?;
        let answer = self
            .receiver
            .transaction(move |scope| {
                Box::pin(async move {
                    increment(scope, "deliveries").await?;
                    if let Some(row) = sqlx_turso::query_file_as!(
                        ReceiptRow,
                        "tests/conformance/queries/receiver_receipt.sql",
                        call
                    )
                    .fetch_optional(scope.executor())
                    .await?
                    {
                        if row.request != request {
                            return Err(StoreError::backend("receiver receipt content conflict"));
                        }
                        return Ok(row.answer);
                    }
                    increment(scope, "remote").await?;
                    let answer = request
                        .checked_mul(2)
                        .ok_or_else(|| StoreError::backend("fixture answer overflow"))?;
                    sqlx_turso::query_file!(
                        "tests/conformance/queries/insert_receipt.sql",
                        call,
                        request,
                        answer
                    )
                    .execute(scope.executor())
                    .await?;
                    Ok(answer)
                })
            })
            .await
            .map_err(lost)?;
        if self.cut == Some(Checkpoint::Receiver) {
            self.receiver_ready.notify_one();
            std::future::pending::<()>().await;
        }
        serde_json::from_slice(&serde_json::to_vec(&answer).map_err(lost)?).map_err(lost)
    }
}
fn lost(_error: impl std::fmt::Display) -> CallFailure {
    CallFailure::AnswerLost
}

struct JourneyStore {
    inner: Arc<TursoStore<JourneyHost>>,
    cut: Option<Checkpoint>,
}
impl AtomicStore for JourneyStore {
    type Scope<'t> = TursoScope<'t, JourneyHost>;
    async fn transaction<T, F>(&self, run: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: for<'s, 't> FnOnce(&'s mut Self::Scope<'t>) -> BoxFuture<'s, Result<T, StoreError>>
            + Send
            + 'static,
    {
        let result = self.inner.transaction(run).await?;
        if let Some(cut) = self.cut {
            let observed = self
                .inner
                .transaction(|scope| Box::pin(async move { checkpoint(scope).await }))
                .await?;
            if observed == Some(cut) {
                halt(cut).await?;
            }
        }
        Ok(result)
    }
}
async fn operation_id<H: Host>(
    scope: &mut TursoScope<'_, H>,
) -> Result<Option<OperationId>, StoreError> {
    sqlx_turso::query_file_as!(IdRow, "tests/conformance/queries/journey_operation.sql")
        .fetch_optional(scope.executor())
        .await?
        .map(|row| OperationId::try_from(row.operation_id).map_err(StoreError::backend))
        .transpose()
}
async fn checkpoint<H: Host>(
    scope: &mut TursoScope<'_, H>,
) -> Result<Option<Checkpoint>, StoreError> {
    let Some(id) = operation_id(scope).await? else {
        return Ok(None);
    };
    let row = scope
        .operation(id)
        .await?
        .ok_or(StoreError::MissingOperation { id })?;
    let drive = match row.execution {
        OperationExecution::Completed { .. } => return Ok(Some(Checkpoint::Completed)),
        OperationExecution::Failed { reason, .. } => return Err(StoreError::backend(reason)),
        OperationExecution::Running { drive, .. } => drive,
    };
    if scope.step(id, 2).await?.is_some() {
        return Ok(Some(Checkpoint::Last));
    }
    if let Some(step) = scope.step(id, 1).await? {
        return Ok(Some(match step.state {
            StepState::CallDone { .. } => Checkpoint::Answer,
            StepState::CallPending { .. } => Checkpoint::Intent,
            _ => return Err(StoreError::backend("unexpected journey step")),
        }));
    }
    if scope.step(id, 0).await?.is_some() {
        return Ok(Some(Checkpoint::First));
    }
    Ok(Some(if drive.is_some() {
        Checkpoint::Claim
    } else {
        Checkpoint::Admission
    }))
}
async fn halt(cut: Checkpoint) -> Result<(), StoreError> {
    println!("{}", serde_json::json!({ "checkpoint": cut }));
    std::io::stdout().flush().map_err(StoreError::backend)?;
    std::future::pending().await
}
async fn migrate_fixture<H: Host>(store: &TursoStore<H>) -> Result<(), StoreError> {
    store
        .transaction(|scope| {
            Box::pin(async move {
                let mut migrator = sqlx::migrate!("./tests/conformance/migrations");
                migrator.dangerous_set_table_name("journey_fixture_migrations");
                migrator
                    .run_direct(None, scope.executor(), false)
                    .await
                    .map_err(StoreError::backend)
            })
        })
        .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().ok_or("mode required")?;
    let root = args.next().ok_or("state directory required")?;
    let cut = args
        .next()
        .filter(|value| value != "none")
        .map(|value| serde_json::from_str::<Checkpoint>(&serde_json::to_string(&value)?))
        .transpose()?;
    let receiver = Arc::new(
        TursoStore::open_and_migrate(Path::new(&root).join("receiver.db"), Arc::new(NoCalls))
            .await?,
    );
    migrate_fixture(&receiver).await?;
    let host = Arc::new(JourneyHost {
        receiver,
        timer: TokioWakeTimer::new(SystemClock),
        cut,
        receiver_ready: tokio::sync::Notify::new(),
    });
    let inner = Arc::new(
        TursoStore::open_and_migrate(Path::new(&root).join("control.db"), Arc::clone(&host))
            .await?,
    );
    migrate_fixture(&inner).await?;
    let store = Arc::new(JourneyStore { inner, cut });
    let mut engine = Engine::new(Arc::clone(&store), Arc::clone(&host));
    engine.register(JourneyTask::<JourneyStore>(PhantomData))?;
    if mode == "start" {
        let request_key = RequestKey::new("journey")?;
        let mut start = Box::pin(engine.start::<JourneyTask<JourneyStore>>(
            &request_key,
            JourneyInput {
                value: 7,
                not_before: host.now(),
            },
        ));
        if cut == Some(Checkpoint::Receiver) {
            tokio::select! {
                result = &mut start => { result?; return Err("start returned before receiver cut".into()); }
                () = host.receiver_ready.notified() => {
                    let report = engine.drive_due().await?;
                    println!("{}", serde_json::json!({ "competing_drive": true, "completed": report.completed.len(), "failed": report.failed.len(), "retried": report.retried.len() }));
                    halt(Checkpoint::Receiver).await?;
                }
            }
        } else {
            start.await?;
        }
    } else {
        let report = engine.drive_due().await?;
        if mode == "compete" {
            println!(
                "{}",
                serde_json::json!({ "competing_drive": true, "completed": report.completed.len(), "failed": report.failed.len(), "retried": report.retried.len() })
            );
            return Ok(());
        }
    }
    loop {
        if store
            .inner
            .transaction(|scope| Box::pin(async move { checkpoint(scope).await }))
            .await?
            == Some(Checkpoint::Completed)
        {
            break;
        }
        tokio::time::timeout(Duration::from_secs(45), host.timer.wait()).await?;
        engine.drive_due().await?;
    }
    let (first, last, steps) = store
        .inner
        .transaction(|scope| {
            Box::pin(async move {
                let id = operation_id(scope)
                    .await?
                    .ok_or_else(|| StoreError::backend("missing journey"))?;
                Ok((
                    read_counter(scope, "first").await?,
                    read_counter(scope, "last").await?,
                    scope.step_count(id).await?,
                ))
            })
        })
        .await?;
    let (remote, deliveries) = host
        .receiver
        .transaction(|scope| {
            Box::pin(async move {
                Ok((
                    read_counter(scope, "remote").await?,
                    read_counter(scope, "deliveries").await?,
                ))
            })
        })
        .await?;
    println!(
        "{}",
        serde_json::json!({ "completed": true, "first": first, "last": last, "remote": remote, "deliveries": deliveries, "remaining_steps": steps })
    );
    Ok(())
}
