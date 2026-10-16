//! Rule outcomes and rollback over the real Turso transaction boundary.
// Result propagates fixture/storage failures; assertions identify contract violations.
#![allow(clippy::panic_in_result_fn)]
use crate::*;
use durable_steps::{CallId, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    row::Row,
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
impl ReceiverHook<TursoScope<'_>> for Hook {
    type Protocol = NoConsumer;
    type FollowUp = String;
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
                code: "storage".into(),
                detail: error.to_string(),
            })?;
        if self.refuse {
            return Err(HookRefusal {
                code: "policy".into(),
                detail: "refused after writing".into(),
            });
        }
        Ok(vec!["deliver notice".into()])
    }
    async fn follow_up(&self, _: &String, _: CallId) -> Result<FollowUpDone, FollowUpFailure> {
        Ok(FollowUpDone)
    }
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
        CommitSequence::new(commit),
    ))
}
pub(super) fn piece(commit: u64, from: u64) -> TestResult<Piece> {
    Ok(Piece::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(from)?,
        through: position(commit)?,
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: CommitSequence::new(commit),
            rows: vec![RowChange::upsert(
                "records",
                vec![SqlValue::Integer(9_007_199_254_740_993)],
                vec![
                    SqlValue::Integer(9_007_199_254_740_993),
                    SqlValue::Text(format!("commit {commit}")),
                ],
            )?],
        }],
        part: None,
        annotation: None,
    })?)
}
pub(super) async fn connection() -> TestResult<TursoConnection> {
    let mut connection = TursoConnectOptions::new().connect().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    sqlx_core::raw_sql::raw_sql(include_str!("../migrations/0001_copy_rules.sql"))
        .execute(&mut *transaction)
        .await?;
    sqlx_core::raw_sql::raw_sql("CREATE TABLE records(id INTEGER PRIMARY KEY, body TEXT); CREATE TABLE hook_records(value TEXT PRIMARY KEY);").execute(&mut *transaction).await?;
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
    transaction.commit().await?;
    Ok(connection)
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
