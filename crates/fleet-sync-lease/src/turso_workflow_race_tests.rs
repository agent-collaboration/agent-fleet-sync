//! Stop only at actual production local boundaries to serialize cancel and fencing commits.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_registry_fixture::*,
    turso_workflow_tests::{actor, attempt, cancel, move_request, view},
    *,
};
use durable_steps::{
    AtomicStore, BoxFuture, CallId, OperationContext, OperationFailure, OperationId, RequestKey,
    Resume, StoreError, Task, Uuid,
};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::sync::atomic::Ordering;

struct CheckpointTask {
    fencing: bool,
    inner: HolderChangeTask<TestStore, Turso>,
}
impl Task for CheckpointTask {
    const KIND: &'static str = "fleet-sync.holder-change";
    const VERSION: u32 = 1;
    type Store = TestStore;
    type Input = HolderChangeRequest;
    type Output = HolderChangeAnswer;
    async fn run<C: OperationContext<Store = TestStore>>(
        &self,
        ctx: &mut C,
        request: &Self::Input,
    ) -> Result<Self::Output, OperationFailure> {
        if let crate::change_checks::CheckedChange::Answer(answer) =
            crate::change_checks::check::<C, Turso>(
                ctx,
                request,
                &config().map_err(|error| OperationFailure::InvalidPayload {
                    message: error.to_string(),
                })?,
            )
            .await?
        {
            return Ok(answer);
        }
        if self.fencing
            && let Some(answer) = crate::change_completion::outcome::<C, Turso>(
                ctx,
                request,
                "before_fence",
                Some(HolderChangePhase::Fencing),
            )
            .await?
        {
            return Ok(answer);
        }
        // No extra saved step: a production task can resume this exact prefix.
        Err(OperationFailure::Retry { after_ms: 1 })
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut <TestStore as AtomicStore>::Scope<'t>,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<Self::Output>, StoreError>> {
        self.inner.before_resume(scope, key)
    }
}

async fn takeover(
    store: &TestStore,
    engine: &TestEngine,
    id: TransferId,
) -> TestResult<HolderChangeRequest> {
    let preview = task_answer::<PreviewTakeoverTask<TestStore, Turso>>(
        engine,
        &RequestKey::new(format!("preview-{}", id.as_uuid()))?,
        PreviewTakeover {
            database: database()?,
        },
    )
    .await?;
    let request = HolderChangeRequest::Takeover {
        attempt: id,
        database: database()?,
        preview,
        to: peer(3)?,
    };
    let offered = request.clone();
    store
        .transaction(move |scope| {
            Box::pin(async move {
                admit_holder_change::<TestStore, Turso>(
                    scope,
                    &offered,
                    &ActorRef::new("takeover-admin").map_err(storage)?,
                    SyncTime::from_unix_millis(1000),
                )
                .await
                .map_err(storage)
            })
        })
        .await?;
    Ok(request)
}

#[tokio::test]
async fn cancel_and_before_fence_commit_in_both_orders_without_reopening_cancellability()
-> TestResult {
    for cancel_first in [true, false] {
        let directory = tempfile::tempdir()?;
        let host = TestHost::open(directory.path(), false).await?;
        let store = open_store(&directory.path().join("registry.db"), host.clone(), true).await?;
        let engine = engine(store.clone(), host.clone())?;
        crate::turso_change_tests::register_first(&engine).await?;
        let id = attempt(2801)?;
        let request = takeover(&store, &engine, id).await?;
        let mut checkpoint = TestEngine::new(store.clone(), host.clone());
        checkpoint.register(CheckpointTask {
            fencing: false,
            inner: HolderChangeTask::new(config()?, host.clone()),
        })?;
        checkpoint.drive_due().await?;
        assert_eq!(view(&store, id).await?.phase, HolderChangePhase::Requested);
        if cancel_first {
            assert!(matches!(
                cancel(&store, id).await?,
                CancelAnswer::Accepted { .. }
            ));
        }
        let mut checkpoint = TestEngine::new(store.clone(), host.clone());
        checkpoint.register(CheckpointTask {
            fencing: true,
            inner: HolderChangeTask::new(config()?, host.clone()),
        })?;
        host.advance();
        checkpoint.drive_due().await?;
        if cancel_first {
            assert_eq!(
                view(&store, id).await?.outcome,
                Some(HolderChangeAnswer::Cancelled {
                    by: actor("admin-canceller")?
                })
            );
            assert!(
                host.calls
                    .lock()
                    .await
                    .iter()
                    .all(|(kind, _)| kind != "fleet-sync.copy-fence")
            );
        } else {
            assert_eq!(view(&store, id).await?.phase, HolderChangePhase::Fencing);
            assert_eq!(
                cancel(&store, id).await?,
                CancelAnswer::TooLate {
                    phase: HolderChangePhase::Fencing
                }
            );
            let engine = crate::turso_registry_fixture::engine(store.clone(), host.clone())?;
            host.advance();
            engine.drive_due().await?;
            let ended = view(&store, id).await?;
            assert!(
                matches!(ended.outcome,Some(HolderChangeAnswer::Granted{epoch,..}) if epoch==Epoch::new(2)?)
            );
            assert_eq!(
                ended
                    .history
                    .iter()
                    .filter(|entry| entry.phase == HolderChangePhase::Fencing)
                    .count(),
                1
            );
            assert!(matches!(
                cancel(&store, id).await?,
                CancelAnswer::Ended(HolderChangeAnswer::Granted { .. })
            ));
            let history = ended.history;
            engine
                .start::<HolderChangeTask<TestStore, Turso>>(
                    &holder_change_request_key(id)?,
                    request,
                )
                .await?;
            assert_eq!(view(&store, id).await?.history, history);
        }
    }
    Ok(())
}

#[tokio::test]
async fn takeover_supersedes_a_cancelling_move_and_its_withdrawal_task_stops() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&directory.path().join("registry.db"), host.clone(), true).await?;
    let engine = engine(store.clone(), host.clone())?;
    crate::turso_change_tests::register_first(&engine).await?;
    let old = attempt(2802)?;
    host.lose_release.store(true, Ordering::SeqCst);
    host.withdrawal_unreachable.store(true, Ordering::SeqCst);
    engine
        .start::<HolderChangeTask<TestStore, Turso>>(
            &holder_change_request_key(old)?,
            move_request(old)?,
        )
        .await?;
    cancel(&store, old).await?;
    host.advance();
    engine.drive_due().await?;
    engine.drive_due().await?;
    assert_eq!(
        view(&store, old).await?.phase,
        HolderChangePhase::Cancelling
    );
    let new = attempt(2803)?;
    takeover(&store, &engine, new).await?;
    engine.drive_due().await?;
    assert!(
        matches!(view(&store,new).await?.outcome,Some(HolderChangeAnswer::Granted{epoch,..}) if epoch==Epoch::new(3)?)
    );
    let calls = host.calls.lock().await.len();
    host.advance();
    engine.drive_due().await?;
    assert!(
        host.calls.lock().await[calls..]
            .iter()
            .all(|(kind, _)| kind != "fleet-sync.holder-withdrawal")
    );
    let ended = view(&store, old).await?;
    assert_eq!(
        ended.outcome,
        Some(HolderChangeAnswer::Refused(
            HolderChangeRefusal::Superseded { by: new }
        ))
    );
    assert_eq!(ended.phase, HolderChangePhase::Ended);
    assert!(ended.waiting.is_none());
    Ok(())
}

#[tokio::test]
async fn settling_an_attempt_before_its_delayed_reserve_prevents_the_reservation() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let id = attempt(2804)?;
    let change = HolderChangeRequest::Claim {
        attempt: id,
        database: database()?,
        claimant: peer(2)?,
    };
    let settle_call = CallId {
        operation: OperationId(Uuid::from_u128(2805)),
        step: durable_steps::StepName::new("settle")?,
    };
    let reserve_call = CallId {
        operation: OperationId(Uuid::from_u128(2806)),
        step: durable_steps::StepName::new("reserve")?,
    };
    host.gate
        .settle(&change, SettleOutcome::Cancelled, settle_call.clone())
        .await?;
    let answer = host.gate.reserve(&change, reserve_call.clone()).await?;
    assert!(matches!(answer,GateAnswer::Refused{ref reason} if reason.code.as_str()=="settled"));
    assert_eq!(host.gate.reserve(&change, reserve_call).await?, answer);
    host.gate
        .settle(&change, SettleOutcome::Cancelled, settle_call)
        .await?;
    assert_eq!(host.gate.settlement_count().await?, "1");
    use sqlx_core::row::Row;
    let mut gate = host.gate.connection.lock().await;
    assert_eq!(
        sqlx_core::query::query::<Turso>(
            "SELECT CAST(count(*) AS TEXT) AS total FROM reservations"
        )
        .fetch_one(&mut *gate)
        .await?
        .try_get::<String, _>("total")?,
        "0"
    );
    Ok(())
}

#[tokio::test]
async fn fencing_stays_closed_to_cancel_during_repair_second_dispatch_and_replay() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&directory.path().join("registry.db"), host.clone(), true).await?;
    let engine = engine(store.clone(), host.clone())?;
    crate::turso_change_tests::register_first(&engine).await?;
    let id = attempt(2807)?;
    let request = takeover(&store, &engine, id).await?;
    let first = crate::turso_registry_copy::FenceGate::new();
    let second = crate::turso_registry_copy::FenceGate::new();
    host.fence_gates.lock().await.insert(id, first.clone());
    let drive = engine.drive_due();
    let control = async {
        first.started.notified().await;
        assert_eq!(view(&store, id).await?.phase, HolderChangePhase::Fencing);
        assert_eq!(
            cancel(&store, id).await?,
            CancelAnswer::TooLate {
                phase: HolderChangePhase::Fencing
            }
        );
        crate::turso_floor_repair_tests::advance_copy(&host, 1, 5).await?;
        host.fence_gates.lock().await.insert(id, second.clone());
        first.proceed.notify_one();
        second.started.notified().await;
        assert_eq!(view(&store, id).await?.phase, HolderChangePhase::Fencing);
        assert_eq!(
            cancel(&store, id).await?,
            CancelAnswer::TooLate {
                phase: HolderChangePhase::Fencing
            }
        );
        assert_eq!(
            scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
            "6"
        );
        second.proceed.notify_one();
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let (report, control) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(drive, control)
    })
    .await?;
    report?;
    control?;
    let ended = view(&store, id).await?;
    assert!(
        matches!(ended.outcome,Some(HolderChangeAnswer::Granted{epoch,..}) if epoch==Epoch::new(6)?)
    );
    assert_eq!(
        ended
            .history
            .iter()
            .filter(|entry| entry.phase == HolderChangePhase::Fencing)
            .count(),
        1
    );
    engine
        .start::<HolderChangeTask<TestStore, Turso>>(&holder_change_request_key(id)?, request)
        .await?;
    assert_eq!(view(&store, id).await?.history, ended.history);
    Ok(())
}
