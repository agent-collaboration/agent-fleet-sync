//! Follow-up answers survive interruption without turning permanent failures into retries.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_follow_up_fixture::*, *};
use durable_steps::{
    Engine, OperationContext, OperationFailure, StepName, Task, turso_store::TursoStore,
};
use fleet_sync_protocol::FailureCode;
use std::sync::{Arc, atomic::Ordering};

type Store = TursoStore<TestHost>;

fn engine(store: Arc<Store>, host: Arc<TestHost>) -> TestResult<Engine<Store, TestHost>> {
    let mut engine = Engine::new(store, host);
    engine.register(ApplyFollowUp::<Store, String>::new())?;
    Ok(engine)
}

#[tokio::test]
async fn permanent_hook_failure_ends_with_its_code_and_is_never_retried() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = host(&directory.path().join("consumer.db")).await?;
    host.lose_answer.store(false, Ordering::SeqCst);
    *host.hook.next_failure.lock().await = Some(FollowUpFailure::Failed {
        reason: FailureCode::new("delivery_rejected")?,
    });
    let store = Arc::new(
        Store::open_and_migrate(directory.path().join("copy.db"), Arc::clone(&host)).await?,
    );
    setup(&store, Arc::clone(&host.hook)).await?;
    apply_and_admit(&store, Arc::clone(&host.hook), false).await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;

    let report = engine.drive_due().await?;
    assert_eq!(report.failed.len(), 1);
    assert!(report.retried.is_empty());
    let (_, failure) = report.failed.first().ok_or("failed operation missing")?;
    assert!(matches!(failure, OperationFailure::Refused { code, .. } if code=="delivery_rejected"));
    host.now.store(1_000_000, Ordering::SeqCst);
    assert!(engine.drive_due().await?.failed.is_empty());
    assert_eq!(host.hook.follow_up_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn retry_then_done_reuses_one_call_identity_and_completes_one_effect() -> TestResult {
    use sqlx_core::row::Row;
    let directory = tempfile::tempdir()?;
    let host = host(&directory.path().join("consumer.db")).await?;
    host.lose_answer.store(false, Ordering::SeqCst);
    *host.hook.next_failure.lock().await = Some(FollowUpFailure::Retry);
    let store = Arc::new(
        Store::open_and_migrate(directory.path().join("copy.db"), Arc::clone(&host)).await?,
    );
    setup(&store, Arc::clone(&host.hook)).await?;
    apply_and_admit(&store, Arc::clone(&host.hook), false).await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;

    let first = engine.drive_due().await?;
    assert_eq!(first.retried.len(), 1);
    assert!(first.failed.is_empty());
    host.now.store(1_000_000, Ordering::SeqCst);
    assert_eq!(engine.drive_due().await?.completed.len(), 1);
    let calls = host.calls.lock().await;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls.first(), calls.last());
    assert_eq!(host.hook.follow_up_calls.load(Ordering::SeqCst), 2);
    let mut delivered = host.hook.delivered.lock().await;
    let row = sqlx_core::query::query::<sqlx_turso::Turso>(
        "SELECT CAST(count(*) AS TEXT) AS effects FROM delivered",
    )
    .fetch_one(&mut *delivered)
    .await?;
    assert_eq!(row.try_get::<String, _>("effects")?, "1");
    Ok(())
}

// Stops after the real journal commits its call answer, before operation completion.
// On reopen the production task consumes that saved answer; this is not a process-kill probe.
struct InterruptAfterAnswer;
impl Task for InterruptAfterAnswer {
    const KIND: &'static str = "fleet-sync.apply-follow-up";
    const VERSION: u32 = 1;
    const KEEP_RESULT: bool = false;
    type Store = Store;
    type Input = FollowUpInput<String>;
    type Output = FollowUpDone;
    async fn run<C: OperationContext<Store = Store>>(
        &self,
        ctx: &mut C,
        input: &Self::Input,
    ) -> Result<Self::Output, OperationFailure> {
        let step =
            StepName::new("follow_up").map_err(|error| OperationFailure::InvalidPayload {
                message: error.to_string(),
            })?;
        let answer = ctx
            .call(
                step,
                FollowUpCall::<String>::new(input.database),
                input.item.clone(),
            )
            .await?;
        assert!(matches!(answer, FollowUpAnswer::Failed { .. }));
        Err(OperationFailure::Retry { after_ms: 0 })
    }
}

#[tokio::test]
async fn saved_failed_answer_survives_journal_reopen_without_calling_the_hook_again() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("copy.db");
    let host = host(&directory.path().join("consumer.db")).await?;
    host.lose_answer.store(false, Ordering::SeqCst);
    *host.hook.next_failure.lock().await = Some(FollowUpFailure::Failed {
        reason: FailureCode::new("delivery_rejected")?,
    });
    let store = Arc::new(Store::open_and_migrate(&path, Arc::clone(&host)).await?);
    setup(&store, Arc::clone(&host.hook)).await?;
    apply_and_admit(&store, Arc::clone(&host.hook), false).await?;
    let mut interrupted = Engine::new(Arc::clone(&store), Arc::clone(&host));
    interrupted.register(InterruptAfterAnswer)?;
    assert_eq!(interrupted.drive_due().await?.retried.len(), 1);
    assert_eq!(host.hook.follow_up_calls.load(Ordering::SeqCst), 1);
    drop(interrupted);
    drop(store);
    host.now.store(1_000_000, Ordering::SeqCst);
    let store = Arc::new(Store::open_and_migrate(&path, Arc::clone(&host)).await?);
    let resumed = engine(store, Arc::clone(&host))?;

    let report = resumed.drive_due().await?;
    assert_eq!(report.failed.len(), 1);
    assert!(report.retried.is_empty());
    let (_, failure) = report.failed.first().ok_or("failed operation missing")?;
    assert!(matches!(failure, OperationFailure::Refused { code, .. } if code=="delivery_rejected"));
    assert_eq!(host.hook.follow_up_calls.load(Ordering::SeqCst), 1);
    assert_eq!(host.calls.lock().await.len(), 1);
    Ok(())
}
