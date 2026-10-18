//! Admission and recovered follow-up calls share the real Turso journal and apply commit.
// Result propagates fixture/storage failures; assertions identify contract violations.
#![allow(clippy::panic_in_result_fn)]
use crate::*;
use durable_steps::{
    AtomicStore, CallFailure, CallId, CallTarget, Host, Uuid, WakeFailure,
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

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
pub(super) struct TestHost {
    pub(super) now: AtomicI64,
    pub(super) ids: AtomicU64,
    pub(super) lose_answer: AtomicBool,
    pub(super) receiver_rules: Arc<ReceiverTestRules>,
    pub(super) follow_up_delivery: Arc<FollowUpDelivery>,
    pub(super) calls: Mutex<Vec<CallId>>,
}
pub(super) struct ReceiverTestRules {
    pub(super) snapshot_refuse: AtomicBool,
    pub(super) snapshot_calls: AtomicU64,
}
pub(super) struct FollowUpDelivery {
    pub(super) delivered: Mutex<TursoConnection>,
    pub(super) follow_up_calls: AtomicU64,
    pub(super) next_failure: Mutex<Option<FollowUpFailure>>,
}
impl ReceiverRules<TursoScope<'_, TestHost>> for ReceiverTestRules {
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

    async fn on_snapshot_installed(
        &self,
        scope: &mut TursoScope<'_, TestHost>,
        installed: &InstalledSnapshot,
    ) -> Result<Vec<String>, HookRefusal> {
        self.snapshot_calls.fetch_add(1, Ordering::SeqCst);
        let refusal = |error: sqlx_core::Error| HookRefusal {
            code: fixture_refusal_code("storage"),
            detail: error.to_string(),
        };
        let row =
            sqlx_core::query::query::<Turso>("SELECT CAST(count(*) AS TEXT) AS rows FROM records")
                .fetch_one(scope.executor())
                .await
                .map_err(refusal)?;
        let rows: String = row.try_get("rows").map_err(refusal)?;
        let item = format!(
            "snapshot:{}:{}:v{}:{rows}",
            installed.position.epoch.value(),
            installed.position.sequence.value(),
            installed.schema.version
        );
        sqlx_core::query::query::<Turso>("INSERT INTO consumer_applied(value) VALUES(?)")
            .bind(item.clone())
            .execute(scope.executor())
            .await
            .map_err(refusal)?;
        if self.snapshot_refuse.load(Ordering::SeqCst) {
            return Err(HookRefusal {
                code: fixture_refusal_code("snapshot_policy"),
                detail: "snapshot projection refused".into(),
            });
        }
        Ok(vec![item])
    }
}
impl FollowUpHandler for FollowUpDelivery {
    type Item = <ReceiverTestRules as ReceiverRules<TursoScope<'static, TestHost>>>::FollowUp;
    async fn follow_up(
        &self,
        item: &String,
        call: CallId,
    ) -> Result<FollowUpDone, FollowUpFailure> {
        self.follow_up_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(failure) = self.next_failure.lock().await.take() {
            return Err(failure);
        }
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
    FollowUpFailure::Retry
}
impl Clock for TestHost {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(self.now.load(Ordering::SeqCst))
    }
}
impl IdSource for TestHost {
    fn new_id(&self) -> Uuid {
        Uuid::from_u128(
            (u128::from(self.ids.fetch_add(1, Ordering::SeqCst)))
                | 0x019eef00000070008000000000000000,
        )
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
        let answer = match <FollowUpDelivery as FollowUpHandler>::follow_up(
            &*self.follow_up_delivery,
            &item,
            call,
        )
        .await
        {
            Ok(FollowUpDone) => FollowUpAnswer::Done,
            Err(FollowUpFailure::Failed { reason }) => FollowUpAnswer::Failed { reason },
            Err(FollowUpFailure::Retry) => return Err(CallFailure::AnswerLost),
        };
        if self.lose_answer.swap(false, Ordering::SeqCst) {
            return Err(CallFailure::AnswerLost);
        }
        serde_json::from_value(serde_json::to_value(answer).map_err(lost)?).map_err(lost)
    }
}
fn lost(_: impl std::fmt::Display) -> CallFailure {
    CallFailure::AnswerLost
}
pub(super) fn database() -> TestResult<DatabaseId> {
    Ok(DatabaseId::new(
        "019f0000-0000-7000-8000-000000000031".parse()?,
    )?)
}
pub(super) fn writer() -> TestResult<PeerId> {
    Ok(PeerId::new(
        "019f0000-0000-7000-8000-000000000032".parse()?,
    )?)
}
pub(super) fn incoming() -> TestResult<Piece> {
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
                vec![crate::turso_rule_fixture::uuid_key(1)],
                vec![
                    crate::turso_rule_fixture::uuid_key(1),
                    SqlValue::Text("body".into()),
                ],
            )?],
        }],
        part: None,
        annotation: None,
    })?)
}
pub(super) async fn host(path: &std::path::Path) -> TestResult<Arc<TestHost>> {
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
        receiver_rules: Arc::new(ReceiverTestRules {
            snapshot_refuse: AtomicBool::new(false),
            snapshot_calls: AtomicU64::new(0),
        }),
        follow_up_delivery: Arc::new(FollowUpDelivery {
            delivered: Mutex::new(delivered),
            follow_up_calls: AtomicU64::new(0),
            next_failure: Mutex::new(None),
        }),
        calls: Mutex::new(Vec::new()),
    }))
}
pub(super) fn storage(error: impl std::fmt::Display) -> durable_steps::StoreError {
    durable_steps::StoreError::backend(error)
}
pub(super) async fn setup(
    store: &TursoStore<TestHost>,
    hook: Arc<ReceiverTestRules>,
) -> TestResult {
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
                    include_str!("../migrations/0002_replicated_schema.sql"),
                    "\n",
                    include_str!("../migrations/0003_snapshot_install.sql")
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
                            "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,body TEXT)".into(),
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
pub(super) async fn apply_and_admit(
    store: &TursoStore<TestHost>,
    hook: Arc<ReceiverTestRules>,
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
pub(super) async fn counts(
    store: &TursoStore<TestHost>,
) -> TestResult<(String, String, String, String)> {
    Ok(store.transaction(|scope|Box::pin(async move{
        let row=sqlx_core::query::query::<Turso>("SELECT CAST((SELECT count(*) FROM records) AS TEXT) AS records,CAST((SELECT count(*) FROM consumer_applied) AS TEXT) AS hooks,CAST((SELECT count(*) FROM _fleet_sync_operations) AS TEXT) AS operations,CAST((SELECT count(*) FROM _fleet_sync_receipts) AS TEXT) AS receipts").fetch_one(scope.executor()).await?;
        Ok((row.try_get("records")?,row.try_get("hooks")?,row.try_get("operations")?,row.try_get("receipts")?))
    })).await?)
}

struct SchemaHook;
impl ReceiverRules<TursoScope<'_, TestHost>> for SchemaHook {
    type Protocol = NoConsumer;
    type FollowUp = ();
    async fn on_applied(
        &self,
        _: &mut TursoScope<'_, TestHost>,
        _: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<()>, HookRefusal> {
        Ok(Vec::new())
    }
}
impl FollowUpHandler for SchemaHook {
    type Item = ();
    async fn follow_up(&self, _: &(), _: CallId) -> Result<FollowUpDone, FollowUpFailure> {
        Ok(FollowUpDone)
    }
}

// These are fixed valid fixture literals; an invalid code is a fixture authoring error.
#[allow(clippy::expect_used)]
fn fixture_refusal_code(code: &str) -> HookRefusalCode {
    HookRefusalCode::new(code).expect("valid fixture refusal code")
}
