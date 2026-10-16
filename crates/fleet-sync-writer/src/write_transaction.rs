use crate::{StatementGuard, WriteRefusal};
use either::Either;
use fleet_sync_lease::{HolderError, LeaseHolder, WriterState};
use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::{FutureExt, StreamExt, stream};
use runtime_primitives::Clock;
use sqlx_core::{
    describe::Describe,
    executor::{Execute, Executor},
    pool::Pool,
    sql_str::SqlStr,
    transaction::Transaction,
};
use sqlx_turso::{Turso, TursoQueryResult, TursoRow, TursoStatement, TursoTypeInfo};
use std::{fmt, sync::Arc};
use tokio::sync::OwnedMutexGuard;

#[derive(Debug)]
pub(crate) struct WriterControl {
    pub holder: LeaseHolder,
    pub statements: StatementGuard,
    pub provisional_writes: bool,
}

impl WriterControl {
    pub(crate) fn admit(&self, now: runtime_primitives::SyncTime) -> Result<(), WriteRefusal> {
        let state = self.holder.writer_state(now).map_err(|error| match error {
            HolderError::RegistrationRejected { reason } => WriteRefusal::Rejected { reason },
            HolderError::Refused(_)
            | HolderError::LeaseMismatch
            | HolderError::InvalidTransition => WriteRefusal::NotWriter,
        })?;
        match state {
            WriterState::Writing { .. } => Ok(()),
            WriterState::Provisional if self.provisional_writes => Ok(()),
            WriterState::Provisional => Err(WriteRefusal::Provisional),
            WriterState::Seeding { .. } => Err(WriteRefusal::Seeding),
            WriterState::Paused { epoch, expired_at } => {
                Err(WriteRefusal::LeaseExpired { epoch, expired_at })
            }
            WriterState::Draining { transfer, .. } => Err(WriteRefusal::Draining { transfer }),
            WriterState::Released { .. } => Err(WriteRefusal::Released),
            WriterState::Superseded { current } => Err(WriteRefusal::Superseded { current }),
        }
    }
}

pub struct WriteTransaction {
    transaction: Transaction<'static, Turso>,
    control: OwnedMutexGuard<WriterControl>,
    clock: Arc<dyn Clock>,
}

impl fmt::Debug for WriteTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WriteTransaction")
            .finish_non_exhaustive()
    }
}

impl WriteTransaction {
    pub(crate) async fn begin(
        pool: &Pool<Turso>,
        control: OwnedMutexGuard<WriterControl>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, WriteRefusal> {
        control.admit(clock.now())?;
        let transaction = pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(WriteRefusal::from_sqlx)?;
        Ok(Self {
            transaction,
            control,
            clock,
        })
    }

    fn check(&mut self, sql: &str) -> Result<(), sqlx_core::Error> {
        self.control
            .admit(self.clock.now())
            .map_err(|error| sqlx_core::Error::Configuration(Box::new(error)))?;
        self.control.statements.check(sql).map_err(Into::into)
    }

    pub async fn commit(self) -> Result<(), WriteRefusal> {
        if let Err(refusal) = self.control.admit(self.clock.now()) {
            self.transaction
                .rollback()
                .await
                .map_err(WriteRefusal::from_sqlx)?;
            return Err(refusal);
        }
        self.transaction
            .commit()
            .await
            .map_err(WriteRefusal::from_sqlx)
    }

    pub async fn rollback(self) -> Result<(), WriteRefusal> {
        self.transaction
            .rollback()
            .await
            .map_err(WriteRefusal::from_sqlx)
    }
}

impl<'transaction> Executor<'transaction> for &'transaction mut WriteTransaction {
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
