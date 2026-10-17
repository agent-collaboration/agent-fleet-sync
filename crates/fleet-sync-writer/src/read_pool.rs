use crate::{StatementGuard, WriteRefusal};
use either::Either;
use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::{FutureExt, StreamExt, stream};
use sqlx_core::{
    describe::Describe,
    executor::{Execute, Executor},
    pool::Pool,
    sql_str::SqlStr,
};
use sqlx_turso::{Turso, TursoQueryResult, TursoRow, TursoStatement, TursoTypeInfo};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

/// SQLx read access without an acquire method or a writable pool handle.
#[derive(Debug)]
pub struct ReadPool {
    pub(crate) pool: crate::native_pool::NativePool,
    pub(crate) readers: Arc<RwLock<()>>,
    statements: Mutex<StatementGuard>,
}

impl ReadPool {
    pub(crate) fn new(pool: Pool<Turso>) -> Self {
        Self {
            pool: crate::native_pool::NativePool::new(pool),
            readers: Arc::new(RwLock::new(())),
            statements: Mutex::new(StatementGuard::new(Vec::new())),
        }
    }
    pub(crate) fn pin(&self) -> Result<(Pool<Turso>, OwnedRwLockReadGuard<()>), sqlx_core::Error> {
        let guard = self
            .readers
            .clone()
            .try_read_owned()
            .map_err(|_| sqlx_core::Error::PoolClosed)?;
        Ok((self.pool.current()?, guard))
    }
    fn check(&self, sql: &str) -> Result<(), sqlx_core::Error> {
        self.statements
            .lock()
            .map_err(|_| sqlx_core::Error::Configuration(Box::new(WriteRefusal::Closed)))?
            .check_read(sql)
            .map_err(Into::into)
    }
}

impl<'pool> Executor<'pool> for &'pool ReadPool {
    type Database = Turso;

    fn fetch_many<'executor, 'query: 'executor, E>(
        self,
        query: E,
    ) -> BoxStream<'executor, Result<Either<TursoQueryResult, TursoRow>, sqlx_core::Error>>
    where
        'pool: 'executor,
        E: 'query + Execute<'query, Turso>,
    {
        let query = match crate::guarded_query::GuardedQuery::capture(query, |sql| self.check(sql))
        {
            Ok(query) => query,
            Err(error) => return stream::once(async move { Err(error) }).boxed(),
        };
        let (pool, guard) = match self.pin() {
            Ok(pin) => pin,
            Err(error) => return stream::once(async move { Err(error) }).boxed(),
        };
        Box::pin(GuardedReadStream {
            stream: (&pool).fetch_many(query),
            _guard: guard,
        })
    }

    fn fetch_optional<'executor, 'query: 'executor, E>(
        self,
        query: E,
    ) -> BoxFuture<'executor, Result<Option<TursoRow>, sqlx_core::Error>>
    where
        'pool: 'executor,
        E: 'query + Execute<'query, Turso>,
    {
        async move {
            let query = crate::guarded_query::GuardedQuery::capture(query, |sql| self.check(sql))?;
            let (pool, _guard) = self.pin()?;
            (&pool).fetch_optional(query).await
        }
        .boxed()
    }

    fn prepare_with<'executor>(
        self,
        sql: SqlStr,
        parameters: &'executor [TursoTypeInfo],
    ) -> BoxFuture<'executor, Result<TursoStatement, sqlx_core::Error>>
    where
        'pool: 'executor,
    {
        async move {
            self.check(sql.as_str())?;
            let (pool, _guard) = self.pin()?;
            (&pool).prepare_with(sql, parameters).await
        }
        .boxed()
    }

    fn describe<'executor>(
        self,
        sql: SqlStr,
    ) -> BoxFuture<'executor, Result<Describe<Turso>, sqlx_core::Error>>
    where
        'pool: 'executor,
    {
        async move {
            self.check(sql.as_str())?;
            let (pool, _guard) = self.pin()?;
            (&pool).describe(sql).await
        }
        .boxed()
    }
}

struct GuardedReadStream<'query> {
    stream: BoxStream<'query, Result<Either<TursoQueryResult, TursoRow>, sqlx_core::Error>>,
    _guard: OwnedRwLockReadGuard<()>,
}
impl futures_core::Stream for GuardedReadStream<'_> {
    type Item = Result<Either<TursoQueryResult, TursoRow>, sqlx_core::Error>;
    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(context)
    }
}
