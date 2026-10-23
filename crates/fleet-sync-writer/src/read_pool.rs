use crate::{StatementGuard, WriteRefusal};
use either::Either;
use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::{FutureExt, StreamExt, TryStreamExt, stream};
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
    pub(crate) file: Option<Arc<crate::read_copy_file::ReadCopyFile>>,
}

/// Pins both the reader admission and, for disposable copies, its physical file.
pub(crate) struct ReadPin {
    _reader: OwnedRwLockReadGuard<()>,
    _file: Option<Arc<crate::read_copy_file::ReadCopyFile>>,
}

impl ReadPool {
    /// Keep related reads on one transaction until it is closed or dropped.
    pub async fn begin_read(&self) -> Result<crate::ReadTransaction, sqlx_core::Error> {
        crate::ReadTransaction::begin(self).await
    }

    #[cfg(test)]
    pub(crate) fn new(pool: Pool<Turso>) -> Self {
        Self::from_native_pool(crate::native_pool::NativePool::passive(pool))
    }
    pub(crate) fn from_native_pool(pool: crate::native_pool::NativePool) -> Self {
        Self {
            pool,
            readers: Arc::new(RwLock::new(())),
            statements: Mutex::new(StatementGuard::new(Vec::new())),
            file: None,
        }
    }
    fn read_guard(&self) -> Result<ReadPin, sqlx_core::Error> {
        let reader = self
            .readers
            .clone()
            .try_read_owned()
            .map_err(|_| sqlx_core::Error::PoolClosed)?;
        Ok(ReadPin {
            _reader: reader,
            _file: self.file.clone(),
        })
    }
    pub(crate) async fn pin(&self) -> Result<(Pool<Turso>, ReadPin), sqlx_core::Error> {
        let guard = self.read_guard()?;
        self.pool.recover().await?;
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
        let guard = match self.read_guard() {
            Ok(guard) => guard,
            Err(error) => return stream::once(async move { Err(error) }).boxed(),
        };
        stream::once(async move {
            self.pool.recover().await?;
            let pool = self.pool.current()?;
            Ok::<_, sqlx_core::Error>(GuardedReadStream {
                stream: (&pool).fetch_many(query),
                _guard: guard,
            })
        })
        .try_flatten()
        .boxed()
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
            let (pool, _guard) = self.pin().await?;
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
            let (pool, _guard) = self.pin().await?;
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
            let (pool, _guard) = self.pin().await?;
            (&pool).describe(sql).await
        }
        .boxed()
    }
}

struct GuardedReadStream<'query> {
    stream: BoxStream<'query, Result<Either<TursoQueryResult, TursoRow>, sqlx_core::Error>>,
    _guard: ReadPin,
}
impl futures_core::Stream for GuardedReadStream<'_> {
    type Item = Result<Either<TursoQueryResult, TursoRow>, sqlx_core::Error>;
    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(context)
    }
}
