//! SQLx `Executor` for Turso connections: fetch, prepare and describe

mod row_stream;
mod sql_inspection;
#[cfg(test)]
mod statement_cache_tests;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use either::Either;
use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::{FutureExt, StreamExt, TryStreamExt, stream};
use sqlx_core::{
    error::Error,
    executor::{Execute, Executor},
    logger::QueryLogger,
    sql_str::SqlStr,
    statement::Statement,
};

use self::{
    row_stream::{RowStreamState, RunningQuery},
    sql_inspection::{inspect_sql, transaction_control},
};
use crate::{
    Turso, TursoAdapterError, TursoArguments, TursoColumn, TursoConnection, TursoQueryResult,
    TursoRow, TursoStatement, busy_wait::BusyWait, column::collect_column_names,
    error::map_turso_error,
};

type QueryOutputStream = BoxStream<'static, Result<Either<TursoQueryResult, TursoRow>, Error>>;

impl TursoConnection {
    async fn fetch_many_sql(
        &mut self,
        sql: SqlStr,
        persistent: bool,
        arguments: Option<TursoArguments>,
    ) -> Result<BoxStream<'_, Result<Either<TursoQueryResult, TursoRow>, Error>>, Error> {
        self.clear_pending_rollback().await?;
        let observation = self.before_statement(transaction_control(sql.as_str()))?;
        match self.fetch_many_unobserved(sql, persistent, arguments).await {
            Ok(stream) => Ok(stream
                .map(move |result| {
                    match &result {
                        Ok(Either::Left(_)) => self.after_statement_success(observation),
                        Err(_) => self.after_statement_error(observation),
                        Ok(Either::Right(_)) => {}
                    }
                    result
                })
                .boxed()),
            Err(error) => {
                self.after_statement_error(observation);
                Err(error)
            }
        }
    }

    async fn fetch_many_unobserved(
        &mut self,
        sql: SqlStr,
        persistent: bool,
        arguments: Option<TursoArguments>,
    ) -> Result<QueryOutputStream, Error> {
        // Logs the statement (and slow statements) when the query's results are done or dropped,
        // as SQLx's other drivers do, honouring the connection's log settings.
        let logger = QueryLogger::new(sql.clone(), self.options().log_settings().clone());

        let inspection = inspect_sql(sql.as_str());
        if inspection.may_contain_multiple_statements {
            if arguments.is_some() {
                return Err(TursoAdapterError::BatchArgumentsUnsupported.into());
            }

            // A batch can roll back and change the schema between statements that no cookie
            // comparison sees. Forgetting before it starts also covers a batch that fails or is
            // dropped part-way.
            self.forget_cached_statements();
            let mut parser = turso_parser::parser::Parser::new(sql.as_str().as_bytes());
            let mut start = 0;
            while parser
                .next_cmd()
                .map_err(|error| map_turso_error(turso::Error::Error(error.to_string())))?
                .is_some()
            {
                let end = parser.offset();
                let statement = sql.as_str().get(start..end).ok_or_else(|| {
                    Error::Protocol(
                        "Turso parser returned an invalid statement boundary".to_owned(),
                    )
                })?;
                self.execute_raw_statement(statement).await?;
                start = end;
            }
            drop(logger);
            let batch_result: Either<TursoQueryResult, TursoRow> =
                Either::Left(TursoQueryResult::default());
            return Ok(stream::iter([Ok(batch_result)]).boxed());
        }

        if arguments.is_some()
            && let Some(placeholder) = inspection.unsupported_named_placeholder
        {
            return Err(TursoAdapterError::NamedPlaceholderUnsupported { placeholder }.into());
        }

        let mut statement = self.prepare_query_statement(sql, persistent).await?;
        let columns: Arc<[TursoColumn]> = turso_columns(statement.columns()).into();
        let column_names = collect_column_names(&columns);
        let rows = match arguments {
            Some(arguments) => statement
                .query(arguments.into_turso_values())
                .await
                .map_err(map_turso_error)?,
            None => statement.query(()).await.map_err(map_turso_error)?,
        };

        Ok(stream::try_unfold(
            RowStreamState::Rows(Box::new(RunningQuery {
                statement,
                rows,
                columns,
                column_names,
                logger,
                busy_wait: BusyWait::new(self.options().get_busy_timeout()),
            })),
            RowStreamState::next,
        )
        .boxed())
    }

    async fn prepare_sql(&mut self, sql: SqlStr) -> Result<TursoStatement, Error> {
        self.require_live_transaction()?;
        self.discard_statements_prepared_for_another_schema()
            .await?;
        if let Some(statement) = self.cached_statement(sql.as_str()) {
            return Ok(statement);
        }

        let statement = self.prepare_raw_statement(sql.as_str()).await?;
        let statement = TursoStatement::with_raw(
            sql.as_str().to_owned(),
            turso_columns(statement.columns()),
            statement,
        );
        self.cache_statement(sql.as_str(), statement.clone());
        Ok(statement)
    }

    async fn prepare_query_statement(
        &mut self,
        sql: SqlStr,
        persistent: bool,
    ) -> Result<turso::Statement, Error> {
        if persistent {
            if let Some(statement) = self.prepare_sql(sql.clone()).await?.raw() {
                return Ok(statement);
            }
        } else {
            // An unprepared statement can follow a written rollback, so it compares the cookie
            // as a cached one does.
            self.discard_statements_prepared_for_another_schema()
                .await?;
        }

        self.prepare_raw_statement(sql.as_str()).await
    }
}

impl<'c> Executor<'c> for &'c mut TursoConnection {
    type Database = Turso;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<Either<TursoQueryResult, TursoRow>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        let arguments = query.take_arguments().map_err(Error::Encode);
        let persistent = query.persistent();
        let sql = query.sql();

        stream::once(async move { self.fetch_many_sql(sql, persistent, arguments?).await })
            .try_flatten()
            .boxed()
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<TursoRow>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        async move {
            let mut stream = self.fetch_many(query);

            while let Some(result) = stream.try_next().await? {
                if let Either::Right(row) = result {
                    return Ok(Some(row));
                }
            }

            Ok(None)
        }
        .boxed()
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        _parameters: &'e [<Self::Database as sqlx_core::database::Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<TursoStatement, Error>>
    where
        'c: 'e,
    {
        async move { self.prepare_sql(sql).await }.boxed()
    }

    fn describe<'e>(
        self,
        sql: SqlStr,
    ) -> BoxFuture<'e, Result<sqlx_core::describe::Describe<Self::Database>, Error>>
    where
        'c: 'e,
    {
        async move {
            let statement = self.prepare_sql(sql).await?;
            let columns = statement.columns().to_vec();
            let nullable = vec![None; columns.len()];

            // The high-level Turso statement exposes no parameter metadata, so arity stays
            // unchecked (`parameters: None`).
            Ok(sqlx_core::describe::Describe {
                columns,
                parameters: None,
                nullable,
            })
        }
        .boxed()
    }
}

fn turso_columns(columns: Vec<turso::Column>) -> Vec<TursoColumn> {
    columns
        .iter()
        .enumerate()
        .map(|(ordinal, column)| TursoColumn::from_turso(ordinal, column))
        .collect()
}
