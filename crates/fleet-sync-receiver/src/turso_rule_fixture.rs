//! Rule outcomes and rollback over the real Turso transaction boundary.
// Result propagates fixture/storage failures; assertions identify contract violations.
#![allow(clippy::panic_in_result_fn)]
use crate::*;
use durable_steps::{CallId, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    row::Row,
    sql_str::SqlSafeStr,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
pub(super) fn ready_status(answer: CopyStatusAnswer) -> TestResult<CopyStatus> {
    match answer {
        CopyStatusAnswer::Status(status) => Ok(status),
        answer => Err(format!("copy is not following: {answer:?}").into()),
    }
}
pub(super) struct TursoScope<'a>(pub(super) &'a mut TursoConnection);
impl TransactionScope for TursoScope<'_> {
    type Database = Turso;
    type Executor<'q>
        = &'q mut TursoConnection
    where
        Self: 'q;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}

pub(super) struct Hook {
    pub(super) refuse: bool,
}
impl ReceiverRules<TursoScope<'_>> for Hook {
    type Protocol = NoConsumer;
    type FollowUp = String;
    fn local_migrations(&self) -> &'static [sqlx_core::migrate::Migration] {
        static MIGRATIONS: std::sync::LazyLock<Vec<sqlx_core::migrate::Migration>> =
            std::sync::LazyLock::new(|| {
                vec![sqlx_core::migrate::Migration::new(
                    1,
                    "local hook records".into(),
                    sqlx_core::migrate::MigrationType::Simple,
                    "CREATE TABLE hook_records(value TEXT PRIMARY KEY);".into_sql_str(),
                    false,
                )]
            });
        &MIGRATIONS
    }
    async fn on_applied(
        &self,
        scope: &mut TursoScope<'_>,
        applied: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<String>, HookRefusal> {
        sqlx_core::query::query::<Turso>("INSERT INTO hook_records(value) VALUES(?1)")
            .bind(applied.through.sequence.value().to_string())
            .execute(scope.executor())
            .await
            .map_err(|error| HookRefusal {
                code: fixture_refusal_code("storage"),
                detail: error.to_string(),
            })?;
        if self.refuse {
            return Err(HookRefusal {
                code: fixture_refusal_code("policy"),
                detail: "refused after writing".into(),
            });
        }
        Ok(vec!["deliver notice".into()])
    }
}
impl FollowUpHandler for Hook {
    type Item = String;
    async fn follow_up(&self, _: &String, _: CallId) -> Result<FollowUpDone, FollowUpFailure> {
        Ok(FollowUpDone)
    }
}

pub(super) fn uuid_key(index: i64) -> SqlValue {
    let payload = u128::from(index.unsigned_abs()) & ((1_u128 << 62) - 1);
    SqlValue::Text(uuid::Uuid::from_u128(0x019eef00000070008000000000000000 | payload).to_string())
}
pub(super) fn database() -> TestResult<DatabaseId> {
    Ok(DatabaseId::new(
        "019f0000-0000-7000-8000-000000000011".parse()?,
    )?)
}
pub(super) fn writer() -> TestResult<PeerId> {
    Ok(PeerId::new(
        "019f0000-0000-7000-8000-000000000012".parse()?,
    )?)
}
pub(super) fn position(commit: u64) -> TestResult<ReplicationPosition> {
    Ok(ReplicationPosition::new(
        Epoch::new(1)?,
        CommitSequence::new(commit + 1),
    ))
}
pub(super) fn piece(commit: u64, from: u64) -> TestResult<Piece> {
    Ok(Piece::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(from)?,
        through: position(commit)?,
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: position(commit)?.sequence,
            rows: vec![RowChange::upsert(
                "records",
                vec![SqlValue::Text(
                    "019eef00-0000-7000-8000-000000000001".into(),
                )],
                vec![
                    SqlValue::Text("019eef00-0000-7000-8000-000000000001".into()),
                    SqlValue::Text(format!("commit {commit}")),
                ],
            )?],
        }],
        part: None,
        annotation: None,
    })?)
}
pub(super) async fn connection() -> TestResult<TursoConnection> {
    connection_with_options(TursoConnectOptions::new()).await
}
pub(super) async fn connection_with_options(
    options: TursoConnectOptions,
) -> TestResult<TursoConnection> {
    let mut connection = options.connect().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    RECEIVER_MIGRATOR
        .run_direct(None, &mut *transaction, false)
        .await?;
    initialize_hook(&mut TursoScope(&mut transaction), &Hook { refuse: false }).await?;
    create_copy(
        &mut TursoScope(&mut transaction),
        &CreateCopy {
            database: database()?,
            epoch: Epoch::new(1)?,
            holder: Some(writer()?),
            origin: CopyOrigin::Empty,
        },
    )
    .await?;
    let schema = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0)),
        through: position(0)?,
        transactions: vec![ReplicatedTransaction::SchemaStep {
            commit_id: CommitSequence::new(1),
            migration_version: 1,
            statements: vec![
                "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, body TEXT)".into(),
            ],
        }],
        part: None,
        annotation: None,
    })?;
    apply_piece(
        &mut TursoScope(&mut transaction),
        &schema,
        &writer()?,
        &SchemaHook,
        SyncTime::from_unix_millis(0),
    )
    .await?;
    transaction.commit().await?;
    Ok(connection)
}

struct SchemaHook;
impl ReceiverRules<TursoScope<'_>> for SchemaHook {
    type Protocol = NoConsumer;
    type FollowUp = ();
    async fn on_applied(
        &self,
        _: &mut TursoScope<'_>,
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

pub(super) async fn counts(
    connection: &mut TursoConnection,
) -> TestResult<(String, String, String)> {
    let row = sqlx_core::query::query::<Turso>("SELECT CAST((SELECT count(*) FROM records) AS TEXT) AS records, CAST((SELECT count(*) FROM hook_records) AS TEXT) AS hooks, CAST((SELECT count(*) FROM _fleet_sync_receipts) AS TEXT) AS receipts").fetch_one(connection).await?;
    Ok((
        row.try_get("records")?,
        row.try_get("hooks")?,
        row.try_get("receipts")?,
    ))
}

// These are fixed valid fixture literals; an invalid code is a fixture authoring error.
#[allow(clippy::expect_used)]
pub(super) fn fixture_refusal_code(code: &str) -> HookRefusalCode {
    HookRefusalCode::new(code).expect("valid fixture refusal code")
}
