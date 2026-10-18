use crate::{ReadPool, StatementGuard};
use either::Either;
use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::{FutureExt, StreamExt, stream};
use sqlx_core::{
    describe::Describe,
    executor::{Execute, Executor},
    pool::Pool,
    sql_str::SqlStr,
    transaction::Transaction,
};
use sqlx_turso::{Turso, TursoQueryResult, TursoRow, TursoStatement, TursoTypeInfo};
use std::fmt;
use tokio::sync::OwnedRwLockReadGuard;

/// Related reads on one snapshot, without writable SQL or connection access.
/// Close rolls back; dropping it uses SQLx's existing pooled rollback cleanup.
pub struct ReadTransaction {
    transaction: Transaction<'static, Turso>,
    _pool: Pool<Turso>,
    _reader: OwnedRwLockReadGuard<()>,
    statements: StatementGuard,
}

impl fmt::Debug for ReadTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadTransaction")
            .finish_non_exhaustive()
    }
}

impl ReadTransaction {
    pub(crate) async fn begin(readers: &ReadPool) -> Result<Self, sqlx_core::Error> {
        let (pool, reader) = readers.pin().await?;
        let transaction = pool.begin().await?;
        Ok(Self {
            transaction,
            _pool: pool,
            _reader: reader,
            statements: StatementGuard::new(Vec::new()),
        })
    }

    fn check(&mut self, sql: &str) -> Result<(), sqlx_core::Error> {
        self.statements.check_read(sql).map_err(Into::into)
    }

    /// End the read snapshot by rolling back its transaction.
    pub async fn close(self) -> Result<(), sqlx_core::Error> {
        self.transaction.rollback().await
    }
}

impl<'transaction> Executor<'transaction> for &'transaction mut ReadTransaction {
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
        (&mut *self.transaction).fetch_many(query)
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
            (&mut *self.transaction).fetch_optional(query).await
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
            (&mut *self.transaction).prepare_with(sql, parameters).await
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
            (&mut *self.transaction).describe(sql).await
        }
        .boxed()
    }
}
