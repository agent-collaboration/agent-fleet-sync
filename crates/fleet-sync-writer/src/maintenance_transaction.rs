use crate::StatementGuard;
use either::Either;
use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::{FutureExt, StreamExt, stream};
use sqlx_core::{
    describe::Describe,
    executor::{Execute, Executor},
    sql_str::SqlStr,
};
use sqlx_turso::{
    Turso, TursoConnection, TursoQueryResult, TursoRow, TursoStatement, TursoTypeInfo,
};
use std::fmt;

/// Borrowed capture-off bookkeeping scope. Its transaction and commit remain
/// with the sender; consumer SQL cannot mutate replicated or engine tables.
pub struct MaintenanceTransaction<'connection> {
    connection: &'connection mut TursoConnection,
    statements: StatementGuard,
}
impl fmt::Debug for MaintenanceTransaction<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MaintenanceTransaction")
            .finish_non_exhaustive()
    }
}
impl<'connection> MaintenanceTransaction<'connection> {
    pub(crate) fn new(
        connection: &'connection mut TursoConnection,
        tables: Vec<crate::TableLayout>,
    ) -> Self {
        Self {
            connection,
            statements: StatementGuard::new(tables),
        }
    }
    fn check(&mut self, sql: &str) -> Result<(), sqlx_core::Error> {
        self.statements.check_maintenance(sql).map_err(Into::into)
    }
}

impl<'transaction, 'connection> Executor<'transaction>
    for &'transaction mut MaintenanceTransaction<'connection>
{
    type Database = Turso;

    fn fetch_many<'executor, 'query: 'executor, E>(
        self,
        query: E,
    ) -> BoxStream<'executor, Result<Either<TursoQueryResult, TursoRow>, sqlx_core::Error>>
    where
        'transaction: 'executor,
        E: 'query + Execute<'query, Turso>,
    {
        let query = match crate::guarded_query::GuardedQuery::capture(query, |sql| self.check(sql))
        {
            Ok(query) => query,
            Err(error) => return stream::once(async move { Err(error) }).boxed(),
        };
        (&mut *self.connection).fetch_many(query)
    }

    fn fetch_optional<'executor, 'query: 'executor, E>(
        self,
        query: E,
    ) -> BoxFuture<'executor, Result<Option<TursoRow>, sqlx_core::Error>>
    where
        'transaction: 'executor,
        E: 'query + Execute<'query, Turso>,
    {
        async move {
            let query = crate::guarded_query::GuardedQuery::capture(query, |sql| self.check(sql))?;
            (&mut *self.connection).fetch_optional(query).await
        }
        .boxed()
    }

    fn prepare_with<'executor>(
        self,
        sql: SqlStr,
        parameters: &'executor [TursoTypeInfo],
    ) -> BoxFuture<'executor, Result<TursoStatement, sqlx_core::Error>>
    where
        'transaction: 'executor,
    {
        async move {
            self.check(sql.as_str())?;
            (&mut *self.connection).prepare_with(sql, parameters).await
        }
        .boxed()
    }

    fn describe<'executor>(
        self,
        sql: SqlStr,
    ) -> BoxFuture<'executor, Result<Describe<Turso>, sqlx_core::Error>>
    where
        'transaction: 'executor,
    {
        async move {
            self.check(sql.as_str())?;
            (&mut *self.connection).describe(sql).await
        }
        .boxed()
    }
}
