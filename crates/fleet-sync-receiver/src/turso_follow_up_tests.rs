//! Admission and recovered follow-up calls share the real Turso journal and apply commit.
// Result propagates fixture/storage failures; assertions identify contract violations.
#![allow(clippy::panic_in_result_fn)]
use crate::*;
use durable_steps::{
    AtomicStore, CallFailure, CallId, CallTarget, Engine, Host, Uuid, WakeFailure,
    storage::TransactionScope,
    turso_store::{TursoScope, TursoStore},
};
use fleet_sync_protocol::*;
use runtime_primitives::{Clock, IdSource, WakeScheduler};
use sqlx_core::{connection::ConnectOptions, row::Row, sql_str::SqlSafeStr};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
};
use tokio::sync::Mutex;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
struct TestHost {
    now: AtomicI64,
    ids: AtomicU64,
    lose_answer: AtomicBool,
    hook: Arc<Hook>,
    calls: Mutex<Vec<CallId>>,
}
struct Hook {
    delivered: Mutex<TursoConnection>,
}
impl ReceiverHook<TursoScope<'_, TestHost>> for Hook {
    type Protocol = NoConsumer;
    type FollowUp = String;
    fn local_migrations(&self) -> &'static [sqlx_core::migrate::Migration] {
        static MIGRATIONS: std::sync::LazyLock<Vec<sqlx_core::migrate::Migration>> =
            std::sync::LazyLock::new(|| {
                vec![sqlx_core::migrate::Migration::new(
                    1,
                    "consumer apply marks".into(),
                    sqlx_core::migrate::MigrationType::Simple,
                    "CREATE TABLE consumer_applied(value TEXT PRIMARY KEY)".into_sql_str(),
                    false,
                )]
            });
        &MIGRATIONS
    }
    async fn on_applied(
        &self,
        scope: &mut TursoScope<'_, TestHost>,
        _: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<String>, HookRefusal> {
        sqlx_core::query::query::<Turso>("INSERT INTO consumer_applied(value) VALUES('applied')")
            .execute(scope.executor())
            .await
            .map_err(|error| HookRefusal {
                code: fixture_refusal_code("storage"),
                detail: error.to_string(),
            })?;
        Ok(vec!["notice".into()])
    }
    async fn follow_up(
        &self,
        item: &String,
        call: CallId,
    ) -> Result<FollowUpDone, FollowUpFailure> {
        let mut connection = self.delivered.lock().await;
        let call = serde_json::to_string(&call).map_err(follow_up_failure)?;
        sqlx_core::query::query::<Turso>(
            "INSERT INTO delivered(call,item) VALUES(?1,?2) ON CONFLICT(call) DO NOTHING",
        )
        .bind(call)
        .bind(item.clone())
        .execute(&mut *connection)
        .await
        .map_err(follow_up_failure)?;
        Ok(FollowUpDone)
    }
}
fn follow_up_failure(_: impl std::fmt::Display) -> FollowUpFailure {
    FollowUpFailure::Retry {
        after: std::time::Duration::ZERO,
    }
}
impl Clock for TestHost {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(self.now.load(Ordering::SeqCst))
    }
}
impl IdSource for TestHost {
    fn new_id(&self) -> Uuid {
        Uuid::from_u128(u128::from(self.ids.fetch_add(1, Ordering::SeqCst)))
    }
}
impl WakeScheduler for TestHost {
    async fn wake_at(&self, _: SyncTime) -> Result<(), WakeFailure> {
        Ok(())
    }
}
impl Host for TestHost {
    async fn dispatch<C: CallTarget>(
        &self,
        target: C,
        request: C::Request,
        call: CallId,
    ) -> Result<C::Answer, CallFailure> {
        if C::KIND != "fleet-sync.apply-follow-up" {
            return Err(CallFailure::AnswerLost);
        }
        let _: FollowUpCall<String> =
            serde_json::from_value(serde_json::to_value(target).map_err(lost)?).map_err(lost)?;
        let item: String =
            serde_json::from_value(serde_json::to_value(request).map_err(lost)?).map_err(lost)?;
        self.calls.lock().await.push(call.clone());
        let answer =
            <Hook as ReceiverHook<TursoScope<'_, TestHost>>>::follow_up(&*self.hook, &item, call)
                .await
                .map_err(lost)?;
        if self.lose_answer.swap(false, Ordering::SeqCst) {
            return Err(CallFailure::AnswerLost);
        }
        serde_json::from_value(serde_json::to_value(answer).map_err(lost)?).map_err(lost)
    }
}
fn lost(_: impl std::fmt::Display) -> CallFailure {
    CallFailure::AnswerLost
}
fn database() -> TestResult<DatabaseId> {
    Ok(DatabaseId::new(
        "019f0000-0000-7000-8000-000000000031".parse()?,
    )?)
}
fn writer() -> TestResult<PeerId> {
    Ok(PeerId::new(
        "019f0000-0000-7000-8000-000000000032".parse()?,
    )?)
}
fn incoming() -> TestResult<Piece> {
    let epoch = Epoch::new(1)?;
    Ok(Piece::new(PieceContents {
        database: database()?,
        epoch,
        continues_from: ReplicationPosition::new(epoch, CommitSequence::new(1)),
        through: ReplicationPosition::new(epoch, CommitSequence::new(2)),
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: CommitSequence::new(2),
            rows: vec![RowChange::upsert(
                "records",
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(1), SqlValue::Text("body".into())],
            )?],
        }],
        part: None,
        annotation: None,
    })?)
}
async fn host(path: &std::path::Path) -> TestResult<Arc<TestHost>> {
    let mut delivered = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .connect()
        .await?;
    sqlx_core::query::query::<Turso>(
        "CREATE TABLE IF NOT EXISTS delivered(call TEXT PRIMARY KEY,item TEXT NOT NULL)",
    )
    .execute(&mut delivered)
    .await?;
    Ok(Arc::new(TestHost {
        now: AtomicI64::new(1000),
        ids: AtomicU64::new(1),
        lose_answer: AtomicBool::new(true),
        hook: Arc::new(Hook {
            delivered: Mutex::new(delivered),
        }),
        calls: Mutex::new(Vec::new()),
    }))
}
fn storage(error: impl std::fmt::Display) -> durable_steps::StoreError {
    durable_steps::StoreError::backend(error)
}
async fn setup(store: &TursoStore<TestHost>, hook: Arc<Hook>) -> TestResult {
    let request = CreateCopy {
        database: database()?,
        epoch: Epoch::new(1)?,
        holder: Some(writer()?),
        origin: CopyOrigin::Empty,
    };
    store
        .transaction(move |scope| {
            Box::pin(async move {
                sqlx_core::raw_sql::raw_sql(concat!(
                    include_str!("../migrations/0001_copy_rules.sql"),
                    "\n",
                    include_str!("../migrations/0002_replicated_schema.sql")
                ))
                .execute(scope.executor())
                .await?;
                initialize_hook(scope, &*hook).await.map_err(storage)?;
                create_copy(scope, &request).await.map_err(storage)?;
                let piece = Piece::<()>::new(PieceContents {
                    database: request.database,
                    epoch: request.epoch,
                    continues_from: ReplicationPosition::new(request.epoch, CommitSequence::new(0)),
                    through: ReplicationPosition::new(request.epoch, CommitSequence::new(1)),
                    transactions: vec![ReplicatedTransaction::SchemaStep {
                        commit_id: CommitSequence::new(1),
                        migration_version: 1,
                        statements: vec![
                            "CREATE TABLE records(id INTEGER PRIMARY KEY,body TEXT)".into(),
                        ],
                    }],
                    part: None,
                    annotation: None,
                })
                .map_err(storage)?;
                apply_piece(
                    scope,
                    &piece,
                    &writer().map_err(storage)?,
                    &SchemaHook,
                    SyncTime::from_unix_millis(0),
                )
                .await
                .map_err(storage)?;
                Ok(())
            })
        })
        .await?;
    Ok(())
}
async fn apply_and_admit(
    store: &TursoStore<TestHost>,
    hook: Arc<Hook>,
    rollback: bool,
) -> Result<(), durable_steps::StoreError> {
    let piece = incoming().map_err(storage)?;
    let writer = writer().map_err(storage)?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let decision = apply_piece(
                    scope,
                    &piece,
                    &writer,
                    &*hook,
                    SyncTime::from_unix_millis(1000),
                )
                .await
                .map_err(storage)?;
                if let ApplyDecision::Applied { follow_ups, .. } = decision {
                    admit_follow_ups::<TursoStore<TestHost>, String>(
                        scope,
                        piece.database,
                        piece.epoch,
                        piece.through,
                        follow_ups,
                    )
                    .await
                    .map_err(storage)?;
                }
                if rollback {
                    return Err(durable_steps::StoreError::Aborted {
                        reason: "injected after admission".into(),
                    });
                }
                Ok(())
            })
        })
        .await
}
async fn counts(store: &TursoStore<TestHost>) -> TestResult<(String, String, String, String)> {
    Ok(store.transaction(|scope|Box::pin(async move{
        let row=sqlx_core::query::query::<Turso>("SELECT CAST((SELECT count(*) FROM records) AS TEXT) AS records,CAST((SELECT count(*) FROM consumer_applied) AS TEXT) AS hooks,CAST((SELECT count(*) FROM _fleet_sync_operations) AS TEXT) AS operations,CAST((SELECT count(*) FROM _fleet_sync_receipts) AS TEXT) AS receipts").fetch_one(scope.executor()).await?;
        Ok((row.try_get("records")?,row.try_get("hooks")?,row.try_get("operations")?,row.try_get("receipts")?))
    })).await?)
}

#[tokio::test]
async fn follow_up_admission_rolls_back_with_apply_and_recovers_after_lost_answer() -> TestResult {
    let directory = tempfile::tempdir()?;
    let journal_path = directory.path().join("copy.db");
    let host = host(&directory.path().join("consumer.db")).await?;
    let store = Arc::new(TursoStore::open_and_migrate(&journal_path, Arc::clone(&host)).await?);
    setup(&store, Arc::clone(&host.hook)).await?;
    let mut engine = Engine::new(Arc::clone(&store), Arc::clone(&host));
    engine.register(ApplyFollowUp::<TursoStore<TestHost>, String>::new())?;
    assert!(matches!(
        apply_and_admit(&store, Arc::clone(&host.hook), true).await,
        Err(durable_steps::StoreError::Aborted { .. })
    ));
    assert_eq!(
        counts(&store).await?,
        ("0".into(), "0".into(), "0".into(), "1".into())
    );
    assert!(engine.drive_due().await?.retried.is_empty());
    assert!(host.calls.lock().await.is_empty());
    apply_and_admit(&store, Arc::clone(&host.hook), false).await?;
    assert_eq!(
        counts(&store).await?,
        ("1".into(), "1".into(), "1".into(), "2".into())
    );
    let report = engine.drive_due().await?;
    assert_eq!(report.retried.len(), 1);
    assert_eq!(host.calls.lock().await.len(), 1);
    drop(engine);
    drop(store);
    host.now.store(1_000_000, Ordering::SeqCst);
    let reopened = Arc::new(TursoStore::open_and_migrate(&journal_path, Arc::clone(&host)).await?);
    let mut engine = Engine::new(Arc::clone(&reopened), Arc::clone(&host));
    engine.register(ApplyFollowUp::<TursoStore<TestHost>, String>::new())?;
    assert_eq!(engine.drive_due().await?.completed.len(), 1);
    let calls = host.calls.lock().await.clone();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls.first(), calls.last());
    let mut delivered = host.hook.delivered.lock().await;
    let row = sqlx_core::query::query::<Turso>(
        "SELECT CAST(count(*) AS TEXT) AS effects,item FROM delivered",
    )
    .fetch_one(&mut *delivered)
    .await?;
    assert_eq!(row.try_get::<String, _>("effects")?, "1");
    assert_eq!(row.try_get::<String, _>("item")?, "notice");
    drop(delivered);
    // KEEP_RESULT=false removes the finished journal row; replay remains the piece owner's job.
    assert_eq!(
        counts(&reopened).await?,
        ("1".into(), "1".into(), "0".into(), "2".into())
    );
    apply_and_admit(&reopened, Arc::clone(&host.hook), false).await?;
    assert!(engine.drive_due().await?.completed.is_empty());
    assert_eq!(host.calls.lock().await.len(), 2);
    Ok(())
}

struct SchemaHook;
impl ReceiverHook<TursoScope<'_, TestHost>> for SchemaHook {
    type Protocol = NoConsumer;
    type FollowUp = ();
    async fn on_applied(
        &self,
        _: &mut TursoScope<'_, TestHost>,
        _: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<()>, HookRefusal> {
        Ok(Vec::new())
    }
    async fn follow_up(&self, _: &(), _: CallId) -> Result<FollowUpDone, FollowUpFailure> {
        Ok(FollowUpDone)
    }
}

// These are fixed valid fixture literals; an invalid code is a fixture authoring error.
#[allow(clippy::expect_used)]
fn fixture_refusal_code(code: &str) -> HookRefusalCode {
    HookRefusalCode::new(code).expect("valid fixture refusal code")
}
