#![allow(clippy::panic_in_result_fn)]
use crate::{
    WriterJournalStore,
    sender_test_fixture::{SenderFixture, SenderSchema, TestResult},
};
use durable_steps::{
    AtomicStore, CallFailure, CallId, CallTarget, Engine, Host, JournalScope, NotSentReason,
    OperationContext, OperationFailure, RequestKey, StepName, StoreError, Task, Uuid,
    storage::TransactionScope,
};
use runtime_primitives::{
    Clock, IdSource, SyncTime, SystemClock, UuidV7Ids, WakeFailure, WakeScheduler,
};
use sqlx_core::{executor::Executor, query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::Arc;

pub(super) struct LocalHost;
impl Clock for LocalHost {
    fn now(&self) -> SyncTime {
        SystemClock.now()
    }
}
impl IdSource for LocalHost {
    fn new_id(&self) -> Uuid {
        UuidV7Ids.new_id()
    }
}
impl WakeScheduler for LocalHost {
    async fn wake_at(&self, _: SyncTime) -> Result<(), WakeFailure> {
        Ok(())
    }
}
impl Host for LocalHost {
    async fn dispatch<C: CallTarget>(
        &self,
        _: C,
        _: C::Request,
        _: CallId,
    ) -> Result<C::Answer, CallFailure> {
        Err(CallFailure::NotSent {
            reason: NotSentReason::Unreachable { last_seen: None },
        })
    }
}
type LocalStore = WriterJournalStore<SenderSchema, LocalHost>;
struct LocalMark;
impl Task for LocalMark {
    const KIND: &'static str = "test.writer-local-mark";
    const VERSION: u32 = 1;
    type Store = LocalStore;
    type Input = String;
    type Output = ();
    async fn run<C: OperationContext<Store = LocalStore>>(
        &self,
        ctx: &mut C,
        input: &String,
    ) -> Result<(), OperationFailure> {
        let input = input.clone();
        ctx.local(
            StepName::new("write-mark").map_err(|error| OperationFailure::InvalidPayload {
                message: error.to_string(),
            })?,
            move |scope| {
                Box::pin(async move {
                    query::<Turso>("INSERT INTO local_marks(id) VALUES(?)")
                        .bind(input)
                        .execute(scope.executor())
                        .await
                        .map_err(StoreError::backend)?;
                    Ok(())
                })
            },
        )
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn writer_journal_shares_capture_off_transaction_and_guards_consumer_executor() -> TestResult
{
    let fixture = SenderFixture::open().await?;
    let before = query::<Turso>("SELECT COUNT(*) AS count FROM turso_cdc")
        .fetch_one(fixture.database.reader())
        .await?
        .try_get::<i64, _>("count")?;
    let host = Arc::new(LocalHost);
    let store = Arc::new(fixture.database.journal_store(host.clone()).await?);
    let rollback = store
        .transaction(|scope| {
            Box::pin(async move {
                scope
                    .executor()
                    .execute("INSERT INTO local_marks VALUES('rolled-back')")
                    .await
                    .map_err(StoreError::backend)?;
                scope
                    .admit::<LocalMark>(
                        &RequestKey::new("rolled-back-child").map_err(StoreError::backend)?,
                        &"child".to_owned(),
                    )
                    .await
                    .map_err(StoreError::backend)?;
                Err::<(), _>(StoreError::Aborted {
                    reason: "rollback business write and admission".into(),
                })
            })
        })
        .await;
    assert!(matches!(rollback, Err(StoreError::Aborted { .. })));
    assert!(
        query::<Turso>("SELECT id FROM local_marks WHERE id='rolled-back'")
            .fetch_optional(fixture.database.reader())
            .await?
            .is_none()
    );
    assert_eq!(
        query::<Turso>("SELECT COUNT(*) AS count FROM _fleet_sync_operations")
            .fetch_one(fixture.database.reader())
            .await?
            .try_get::<i64, _>("count")?,
        0
    );
    let protected = store
        .transaction(|scope| {
            Box::pin(async move {
                scope
                    .executor()
                    .execute("UPDATE text_records SET amount=0")
                    .await
                    .map_err(StoreError::backend)?;
                Ok(())
            })
        })
        .await;
    assert!(
        protected.is_err(),
        "a local journal step must not bypass capture"
    );
    let mut engine = Engine::new(store, host);
    engine.register(LocalMark)?;
    let operation = engine
        .start::<LocalMark>(&RequestKey::new("committed")?, "durable-mark".into())
        .await?;
    engine.drive_due().await?;
    assert!(matches!(
        engine.status(&operation).await?.state,
        durable_steps::OperationState::Completed { .. }
    ));
    assert!(
        query::<Turso>("SELECT id FROM local_marks WHERE id='durable-mark'")
            .fetch_optional(fixture.database.reader())
            .await?
            .is_some()
    );
    let after = query::<Turso>("SELECT COUNT(*) AS count FROM turso_cdc")
        .fetch_one(fixture.database.reader())
        .await?
        .try_get::<i64, _>("count")?;
    assert_eq!(
        after, before,
        "journal setup, steps and local writes must leave capture unchanged"
    );
    fixture.close().await
}
