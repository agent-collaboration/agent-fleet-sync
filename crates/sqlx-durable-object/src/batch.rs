use std::future::Future;

use futures_core::Stream;
use futures_util::{TryFutureExt, stream};
use sqlx_core::error::Error;
use sqlx_core::executor::Execute;
use sqlx_core::query::Query;
use sqlx_durable_object_core::BatchResult;

use crate::{Do, DoArguments, DoBatchResult, DoConnection, DoQueryResult, js};

impl DoConnection {
    /// Runs `queries` in order and atomically: if any statement fails, none
    /// of them take effect.
    ///
    /// This is the Durable Object answer to a transaction, which `sql.exec`
    /// does not allow (see [`DoTransactionManager`](crate::DoTransactionManager)).
    /// The statements run inside [`transaction`](Self::transaction), and a
    /// failing one rolls the others back. Returns what each statement did, in the order
    /// given; for the rows each statement returns as well, see
    /// [`fetch_batch`](Self::fetch_batch).
    ///
    /// ```no_run
    /// # async fn transfer(
    /// #     conn: &sqlx_durable_object::DoConnection,
    /// #     amount: i64,
    /// #     from: i64,
    /// #     to: i64,
    /// # ) -> Result<(), sqlx::Error> {
    /// let results = conn
    ///     .execute_batch([
    ///         sqlx::query("UPDATE accounts SET balance = balance - ?1 WHERE id = ?2")
    ///             .bind(amount)
    ///             .bind(from),
    ///         sqlx::query("UPDATE accounts SET balance = balance + ?1 WHERE id = ?2")
    ///             .bind(amount)
    ///             .bind(to),
    ///     ])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// When binding an argument fails, in which case nothing ran, or when any
    /// statement fails -- in which case the whole batch was rolled back.
    pub fn execute_batch<'q, I>(
        &self,
        queries: I,
    ) -> impl Future<Output = Result<Vec<DoQueryResult>, Error>> + Send + '_
    where
        I: IntoIterator<Item = Query<'q, Do, DoArguments>>,
    {
        self.run_batch(queries).map_ok(|results| {
            results
                .into_iter()
                .map(|result| result.into_parts().1)
                .collect()
        })
    }

    /// Runs `queries` in order and atomically, as
    /// [`execute_batch`](Self::execute_batch) does, and streams each
    /// statement's rows along with what it did, in the order given.
    ///
    /// The shape sqlc's `:batchexec`, `:batchone` and `:batchmany` queries
    /// take, each a stream with one item per statement -- but atomic. So
    /// unlike a loop of queries it is not lazy: the whole batch runs when the
    /// stream is first polled, and a failure is the one item it yields, with
    /// nothing applied.
    ///
    /// ```no_run
    /// # async fn users(conn: &sqlx_durable_object::DoConnection) -> Result<(), sqlx::Error> {
    /// use futures_util::TryStreamExt;
    /// use sqlx::Row;
    ///
    /// let names: Vec<String> = conn
    ///     .fetch_batch([1_i64, 2, 3].map(|id| {
    ///         sqlx::query("SELECT name FROM users WHERE id = ?").bind(id)
    ///     }))
    ///     .map_ok(|result| result.rows()[0].get("name"))
    ///     .try_collect()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// The stream's one item is an error when binding an argument fails, in
    /// which case nothing ran, or when any statement fails -- in which case
    /// the whole batch was rolled back.
    pub fn fetch_batch<'q, I>(
        &self,
        queries: I,
    ) -> impl Stream<Item = Result<DoBatchResult, Error>> + Send + '_
    where
        I: IntoIterator<Item = Query<'q, Do, DoArguments>>,
    {
        self.run_batch(queries)
            .map_ok(|results| stream::iter(results.into_iter().map(Ok)))
            .try_flatten_stream()
    }

    fn run_batch<'q, I>(
        &self,
        queries: I,
    ) -> impl Future<Output = Result<Vec<DoBatchResult>, Error>> + Send + '_
    where
        I: IntoIterator<Item = Query<'q, Do, DoArguments>>,
    {
        // Taken apart here, before the future, so it holds only owned SQL and
        // arguments: the queries themselves need not outlive this call, and
        // the transaction's callback has to own what it runs.
        let queries = queries
            .into_iter()
            .map(|mut query| {
                let arguments = query.take_arguments().map_err(Error::Encode)?;
                Ok((query.sql(), arguments.unwrap_or_default()))
            })
            .collect::<Result<Vec<_>, Error>>();

        async move {
            let queries = queries?;

            if queries.is_empty() {
                return Ok(Vec::new());
            }

            // Inside a transaction, statements run as they would outside
            // one; the transaction is what makes a failure undo the rest.
            self.transaction(move |tx| async move {
                queries
                    .iter()
                    .map(|(query, arguments)| {
                        js::execute(&tx.sql, query.as_str(), arguments.values(), None)
                            .map(|(rows, result)| BatchResult::new(rows, result))
                    })
                    .collect::<Result<Vec<_>, Error>>()
            })
            .await
        }
    }
}
