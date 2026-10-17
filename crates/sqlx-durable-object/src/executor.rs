use std::future;

use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{FutureExt, StreamExt, TryFutureExt, stream};
use sqlx_core::Either;
use sqlx_core::describe::Describe;
use sqlx_core::error::Error;
use sqlx_core::executor::{Execute, Executor};
use sqlx_core::sql_str::SqlStr;

use crate::error::unsupported;
use crate::{
    Do, DoArguments, DoConnection, DoQueryResult, DoRow, DoStatement, DoTransaction, DoTypeInfo, js,
};

// `sql.exec` is synchronous, so each future here finishes on its first poll
// and holds nothing across an `await` -- which is why none of them needs
// D1's `SendFuture`: they are `Send` because `DoConnection` and
// `DoTransaction` are `Sync`. They still run the query when polled, not when
// built, as sqlx's futures do.
//
// One implementation for both executors, a connection and an open transaction:
// each only needs its `sql` handle, and a macro writes the impls for each.
macro_rules! impl_executor {
    ($ty:ty) => {
        impl<'c> Executor<'c> for &'c $ty {
            type Database = Do;

            /// Reports `rows_affected` and `last_insert_rowid` but no rows.
            fn execute<'e, 'q: 'e, E>(self, query: E) -> BoxFuture<'e, Result<DoQueryResult, Error>>
            where
                'c: 'e,
                E: 'q + Execute<'q, Do>,
            {
                async move {
                    let (sql, arguments) = take(query)?;
                    js::run(&self.sql, sql.as_str(), arguments_of(arguments.as_ref()))
                }
                .boxed()
            }

            fn execute_many<'e, 'q: 'e, E>(
                self,
                query: E,
            ) -> BoxStream<'e, Result<DoQueryResult, Error>>
            where
                'c: 'e,
                E: 'q + Execute<'q, Do>,
            {
                stream::once(self.execute(query)).boxed()
            }

            /// Yields the rows only, never a `DoQueryResult`. Use
            /// [`execute`](Executor::execute) for what a statement changed.
            fn fetch_many<'e, 'q: 'e, E>(
                self,
                query: E,
            ) -> BoxStream<'e, Result<Either<DoQueryResult, DoRow>, Error>>
            where
                'c: 'e,
                E: 'q + Execute<'q, Do>,
            {
                async move {
                    let (sql, arguments) = take(query)?;
                    js::fetch(
                        &self.sql,
                        sql.as_str(),
                        arguments_of(arguments.as_ref()),
                        None,
                    )
                    .map(|(rows, _)| rows)
                }
                .map_ok(|rows| stream::iter(rows.into_iter().map(|row| Ok(Either::Right(row)))))
                .try_flatten_stream()
                .boxed()
            }

            fn fetch_optional<'e, 'q: 'e, E>(
                self,
                query: E,
            ) -> BoxFuture<'e, Result<Option<DoRow>, Error>>
            where
                'c: 'e,
                E: 'q + Execute<'q, Do>,
            {
                async move {
                    let (sql, arguments) = take(query)?;
                    let (rows, _) = js::fetch(
                        &self.sql,
                        sql.as_str(),
                        arguments_of(arguments.as_ref()),
                        Some(1),
                    )?;
                    Ok(rows.into_iter().next())
                }
                .boxed()
            }

            fn prepare_with<'e>(
                self,
                sql: SqlStr,
                _parameters: &'e [DoTypeInfo],
            ) -> BoxFuture<'e, Result<DoStatement, Error>>
            where
                'c: 'e,
            {
                future::ready(Ok(DoStatement { sql })).boxed()
            }

            /// Always fails: `sql.exec` cannot describe a statement without running
            /// it. Only sqlx's `query!` macros need this, and they cannot target a
            /// third-party driver anyway.
            fn describe<'e>(self, _sql: SqlStr) -> BoxFuture<'e, Result<Describe<Do>, Error>>
            where
                'c: 'e,
            {
                future::ready(Err(unsupported("statements cannot be described"))).boxed()
            }
        }

        // What sqlx code actually passes -- `&mut conn` -- running on the shared
        // implementation above. `&'c mut` gives up its uniqueness to become `&'c`,
        // which is all a query needs.
        impl<'c> Executor<'c> for &'c mut $ty {
            type Database = Do;

            fn execute<'e, 'q: 'e, E>(self, query: E) -> BoxFuture<'e, Result<DoQueryResult, Error>>
            where
                'c: 'e,
                E: 'q + Execute<'q, Do>,
            {
                (&*self).execute(query)
            }

            fn execute_many<'e, 'q: 'e, E>(
                self,
                query: E,
            ) -> BoxStream<'e, Result<DoQueryResult, Error>>
            where
                'c: 'e,
                E: 'q + Execute<'q, Do>,
            {
                (&*self).execute_many(query)
            }

            fn fetch_many<'e, 'q: 'e, E>(
                self,
                query: E,
            ) -> BoxStream<'e, Result<Either<DoQueryResult, DoRow>, Error>>
            where
                'c: 'e,
                E: 'q + Execute<'q, Do>,
            {
                (&*self).fetch_many(query)
            }

            fn fetch_optional<'e, 'q: 'e, E>(
                self,
                query: E,
            ) -> BoxFuture<'e, Result<Option<DoRow>, Error>>
            where
                'c: 'e,
                E: 'q + Execute<'q, Do>,
            {
                (&*self).fetch_optional(query)
            }

            fn prepare_with<'e>(
                self,
                sql: SqlStr,
                parameters: &'e [DoTypeInfo],
            ) -> BoxFuture<'e, Result<DoStatement, Error>>
            where
                'c: 'e,
            {
                (&*self).prepare_with(sql, parameters)
            }

            fn describe<'e>(self, sql: SqlStr) -> BoxFuture<'e, Result<Describe<Do>, Error>>
            where
                'c: 'e,
            {
                (&*self).describe(sql)
            }
        }
    };
}

impl_executor!(DoConnection);
impl_executor!(DoTransaction);

/// `query`'s SQL and its arguments, bound.
fn take<'q, E>(mut query: E) -> Result<(SqlStr, Option<DoArguments>), Error>
where
    E: Execute<'q, Do>,
{
    let arguments = query.take_arguments().map_err(Error::Encode)?;
    Ok((query.sql(), arguments))
}

fn arguments_of(arguments: Option<&DoArguments>) -> &[crate::DoArgumentValue] {
    arguments.map_or(&[][..], DoArguments::values)
}
