//! A controlled SQLITE_FULL at the SQLx seam, with real preceding writes and rollback.
//! This does not exhaust physical disk capacity.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_rule_fixture::*, *};
use durable_steps::storage::TransactionScope;
use either::Either;
use fleet_sync_protocol::*;
use futures_util::{
    future::BoxFuture,
    stream::{self, BoxStream},
};
use sqlx_core::{
    connection::Connection,
    database::Database,
    error::{DatabaseError, ErrorKind},
    executor::{Execute, Executor},
    row::Row,
    sql_str::SqlStr,
};
use sqlx_turso::{Turso, TursoConnection};
use std::{borrow::Cow, error::Error};

#[derive(Debug, thiserror::Error)]
#[error("controlled storage capacity failure")]
struct CapacityError;
impl DatabaseError for CapacityError {
    fn message(&self) -> &str {
        "controlled storage capacity failure"
    }
    fn code(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed("SQLITE_FULL"))
    }
    fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        self
    }
    fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
        self
    }
    fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
        self
    }
    fn kind(&self) -> ErrorKind {
        ErrorKind::Other
    }
}
struct CapturedQuery {
    sql: SqlStr,
    arguments: Option<<Turso as Database>::Arguments>,
    persistent: bool,
}
impl<'q> Execute<'q, Turso> for CapturedQuery {
    fn sql(self) -> SqlStr {
        self.sql
    }
    fn statement(&self) -> Option<&<Turso as Database>::Statement> {
        None
    }
    fn take_arguments(
        &mut self,
    ) -> Result<Option<<Turso as Database>::Arguments>, sqlx_core::error::BoxDynError> {
        Ok(self.arguments.take())
    }
    fn persistent(&self) -> bool {
        self.persistent
    }
}
#[derive(Debug)]
struct CapacityExecutor<'c> {
    connection: &'c mut TursoConnection,
    writes: &'c mut usize,
}
impl<'c> Executor<'c> for CapacityExecutor<'c> {
    type Database = Turso;
    fn fetch_many<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> BoxStream<
        'e,
        Result<
            Either<<Turso as Database>::QueryResult, <Turso as Database>::Row>,
            sqlx_core::Error,
        >,
    >
    where
        'c: 'e,
        E: 'q + Execute<'q, Turso>,
    {
        let arguments = query.take_arguments().map_err(sqlx_core::Error::Encode);
        let persistent = query.persistent();
        let sql = query.sql();
        let mut parser = turso_parser::parser::Parser::new(sql.as_str().as_bytes());
        let row_insert = matches!(parser.next_cmd(),Ok(Some(turso_parser::ast::Cmd::Stmt(turso_parser::ast::Stmt::Insert{tbl_name,..}))) if tbl_name.name.as_str()=="records");
        if row_insert {
            *self.writes += 1;
        }
        if row_insert && *self.writes == 2 {
            return Box::pin(stream::once(async {
                Err(sqlx_core::Error::Database(Box::new(CapacityError)))
            }));
        }
        match arguments {
            Ok(arguments) => self.connection.fetch_many(CapturedQuery {
                sql,
                arguments,
                persistent,
            }),
            Err(error) => Box::pin(stream::once(async move { Err(error) })),
        }
    }
    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<<Turso as Database>::Row>, sqlx_core::Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Turso>,
    {
        self.connection.fetch_optional(query)
    }
    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [<Turso as Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<Turso as Database>::Statement, sqlx_core::Error>>
    where
        'c: 'e,
    {
        self.connection.prepare_with(sql, parameters)
    }
    fn describe<'e>(
        self,
        sql: SqlStr,
    ) -> BoxFuture<'e, Result<sqlx_core::describe::Describe<Turso>, sqlx_core::Error>>
    where
        'c: 'e,
    {
        self.connection.describe(sql)
    }
}
struct CapacityScope<'c> {
    connection: &'c mut TursoConnection,
    writes: usize,
}
impl TransactionScope for CapacityScope<'_> {
    type Database = Turso;
    type Executor<'q>
        = CapacityExecutor<'q>
    where
        Self: 'q;
    fn executor(&mut self) -> Self::Executor<'_> {
        CapacityExecutor {
            connection: self.connection,
            writes: &mut self.writes,
        }
    }
}
struct NoHook;
impl ReceiverHook<CapacityScope<'_>> for NoHook {
    type Protocol = NoConsumer;
    type FollowUp = ();
    async fn on_applied(
        &self,
        _: &mut CapacityScope<'_>,
        _: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<()>, HookRefusal> {
        Ok(Vec::new())
    }
    async fn follow_up(
        &self,
        _: &(),
        _: durable_steps::CallId,
    ) -> Result<FollowUpDone, FollowUpFailure> {
        Ok(FollowUpDone)
    }
}

#[tokio::test]
async fn copy_full_after_a_real_row_write_returns_the_typed_refusal_and_rolls_back_everything()
-> TestResult {
    let mut connection = connection().await?;
    let incoming = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![ReplicatedTransaction::Rows {
            commit_id: position(1)?.sequence,
            rows: vec![
                RowChange::upsert(
                    "records",
                    vec![uuid_key(1)],
                    vec![uuid_key(1), SqlValue::Text("first real write".into())],
                )?,
                RowChange::upsert(
                    "records",
                    vec![uuid_key(2)],
                    vec![uuid_key(2), SqlValue::Text("capacity failure".into())],
                )?,
            ],
        }],
        part: None,
        annotation: None,
    })?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut scope = CapacityScope {
        connection: &mut transaction,
        writes: 0,
    };
    assert_eq!(
        apply_piece(
            &mut scope,
            &incoming,
            &writer()?,
            &NoHook,
            SyncTime::from_unix_millis(1)
        )
        .await
        .err(),
        Some(ApplyFailure::Refused(PieceRefusal::CopyFull))
    );
    assert_eq!(scope.writes, 2);
    let row =
        sqlx_core::query::query::<Turso>("SELECT CAST(count(*) AS TEXT) AS rows FROM records")
            .fetch_one(scope.executor())
            .await?;
    assert_eq!(row.try_get::<String, _>("rows")?, "1");
    transaction.rollback().await?;
    assert_eq!(
        counts(&mut connection).await?,
        ("0".into(), "0".into(), "1".into())
    );
    assert_eq!(
        ready_status(copy_status(&mut TursoScope(&mut connection)).await?)?.acked,
        position(0)?
    );
    Ok(())
}
