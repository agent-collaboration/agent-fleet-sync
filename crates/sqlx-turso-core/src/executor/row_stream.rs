//! The row stream over a running Turso statement
//!
//! Each step yields the row the engine produced before the next row is stepped, so a valid row
//! is never lost to an error in a later one. When the engine reports completion, the stream
//! yields a query result carrying the statement's change count, as SQLx's `fetch_many`
//! contract expects. The query logger travels with the stream and logs once, when the stream
//! ends or is dropped.
//!
//! The stream resets its statement when it ends or is dropped, as the engine does when it drops
//! a statement. A persistent statement outlives the stream in the connection's cache, so without
//! the reset a stream abandoned after its first row (`fetch_one`, `fetch_optional`) would keep
//! the statement's transaction open until the same SQL runs again: a read would pin the
//! connection to an old snapshot and schema, and a write with `RETURNING` would stay uncommitted.

use std::sync::Arc;

use either::Either;
use sqlx_core::{HashMap, column::Column, error::Error, ext::ustr::UStr, logger::QueryLogger};

use crate::busy_wait::BusyWait;
use crate::{TursoColumn, TursoQueryResult, TursoRow, TursoValue, error::map_turso_error};

pub(super) enum RowStreamState {
    Rows(Box<RunningQuery>),
    Done,
}

/// A statement whose rows are being streamed, reset when the stream ends or is dropped
pub(super) struct RunningQuery {
    pub(super) statement: turso::Statement,
    pub(super) rows: turso::Rows,
    pub(super) columns: Arc<[TursoColumn]>,
    pub(super) column_names: Arc<HashMap<UStr, usize>>,
    pub(super) logger: QueryLogger,
    pub(super) busy_wait: BusyWait,
}

impl Drop for RunningQuery {
    fn drop(&mut self) {
        // Runs before the fields drop, so `rows` still holds the SDK's shared operation guard.
        // A panic while stepping can poison the SDK's statement lock; resetting during that
        // unwind would panic again and abort the process.
        if std::thread::panicking() {
            return;
        }
        if let Err(error) = self.statement.reset() {
            log::warn!("resetting a finished or abandoned Turso statement failed: {error}");
        }
    }
}

impl RowStreamState {
    pub(super) async fn next(
        self,
    ) -> Result<Option<(Either<TursoQueryResult, TursoRow>, Self)>, Error> {
        let Self::Rows(mut query) = self else {
            return Ok(None);
        };

        let Some(row) = query.busy_wait.next_row(&mut query.rows).await? else {
            // Dropping the query as the stream finishes resets the statement and logs it.
            let rows_affected = query.statement.n_change();
            query.logger.increase_rows_affected(rows_affected);
            return Ok(Some((
                Either::Left(TursoQueryResult::new(rows_affected)),
                Self::Done,
            )));
        };

        let mut values = Vec::with_capacity(query.columns.len());
        for (index, column) in query.columns.iter().enumerate() {
            values.push(
                TursoValue::from_turso(row.get_value(index).map_err(map_turso_error)?)
                    .with_type_info(column.type_info().clone()),
            );
        }
        query.logger.increment_rows_returned();

        let row = TursoRow::with_shared_columns(
            Arc::clone(&query.columns),
            Arc::clone(&query.column_names),
            values,
        );
        Ok(Some((Either::Right(row), Self::Rows(query))))
    }
}

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;
    use sqlx_core::{connection::ConnectOptions, executor::Executor, query::query, row::Row};

    use crate::{Turso, TursoConnectOptions};

    const OVERFLOWING_QUERY: &str = "SELECT abs(value) AS value FROM items ORDER BY id";

    #[tokio::test]
    async fn a_row_is_delivered_before_a_later_row_fails() -> sqlx_core::Result<()> {
        // Arrange: abs() of the second row overflows, so the engine fails on that row
        let mut connection = TursoConnectOptions::new().connect().await?;
        (&mut connection)
            .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
            .await?;
        for (id, value) in [(1_i64, 1_i64), (2, i64::MIN)] {
            query::<Turso>("INSERT INTO items (id, value) VALUES (?, ?)")
                .bind(id)
                .bind(value)
                .execute(&mut connection)
                .await?;
        }

        // Act
        let (first, second) = {
            let mut rows = (&mut connection).fetch(OVERFLOWING_QUERY);
            let first = rows.try_next().await;
            let second = rows.try_next().await;
            (first, second)
        };
        let first_only = (&mut connection).fetch_optional(OVERFLOWING_QUERY).await;
        let reused = (&mut connection).fetch_one("SELECT 7 AS reused").await?;

        // Assert: the valid row comes first, then the error; the connection stays usable
        let first = first?.expect("the first row precedes the failing one");
        assert_eq!(first.try_get::<i64, _>("value")?, 1);
        assert!(second.is_err(), "the second row's overflow is reported");
        let first_only = first_only?.expect("fetch_optional returns the first row");
        assert_eq!(first_only.try_get::<i64, _>("value")?, 1);
        assert_eq!(reused.try_get::<i64, _>("reused")?, 7);
        Ok(())
    }

    #[tokio::test]
    async fn a_returning_write_read_with_fetch_one_is_committed() -> sqlx_core::Result<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let options = TursoConnectOptions::new()
            .filename(directory.path().join("returning.db"))
            .create_if_missing(true);
        let mut writer = options.connect().await?;
        let mut observer = options.connect().await?;
        (&mut writer)
            .execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
            .await?;

        // Act: the persistent statement stays cached after fetch_one takes its first row
        let inserted = query::<Turso>("INSERT INTO items (id) VALUES (1), (2) RETURNING id")
            .fetch_one(&mut writer)
            .await?;
        let count = (&mut observer)
            .fetch_one("SELECT count(*) AS count FROM items")
            .await?;

        // Assert: the write committed, so another connection sees both rows
        assert_eq!(inserted.try_get::<i64, _>("id")?, 1);
        assert_eq!(count.try_get::<i64, _>("count")?, 2);
        Ok(())
    }
}
