use std::{fmt, sync::Arc};

use either::Either;
use sqlx_core::{
    HashMap,
    column::ColumnIndex,
    ext::ustr::UStr,
    impl_statement_query,
    sql_str::{AssertSqlSafe, SqlSafeStr, SqlStr},
    statement::Statement,
};

use crate::{Turso, TursoArguments, TursoColumn, TursoTypeInfo, column::collect_column_names};

/// Owned prepared metadata; native handles stay in the connection's private cache.
#[derive(Clone)]
pub struct TursoStatement {
    sql: SqlStr,
    parameters: Option<usize>,
    columns: Arc<[TursoColumn]>,
    column_names: Arc<HashMap<UStr, usize>>,
    raw: Option<turso::Statement>,
}

impl TursoStatement {
    /// Creates statement metadata for a SQL string
    pub fn new(sql: impl Into<String>) -> Self {
        let columns: Arc<[TursoColumn]> = Vec::new().into();
        let column_names = collect_column_names(&columns);

        Self {
            sql: AssertSqlSafe(sql.into()).into_sql_str(),
            parameters: None,
            columns,
            column_names,
            raw: None,
        }
    }

    pub(crate) fn with_raw(
        sql: impl Into<String>,
        columns: Vec<TursoColumn>,
        raw: turso::Statement,
    ) -> Self {
        let columns: Arc<[TursoColumn]> = columns.into();
        let column_names = collect_column_names(&columns);

        Self {
            sql: AssertSqlSafe(sql.into()).into_sql_str(),
            parameters: None,
            columns,
            column_names,
            raw: Some(raw),
        }
    }

    pub(crate) fn raw(&self) -> Option<turso::Statement> {
        self.raw.clone()
    }

    pub(crate) fn into_metadata(mut self) -> Self {
        self.raw = None;
        self
    }
}

impl fmt::Debug for TursoStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TursoStatement")
            .field("sql", &self.sql)
            .field("parameters", &self.parameters)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

impl Statement for TursoStatement {
    type Database = Turso;

    fn into_sql(self) -> SqlStr {
        self.sql
    }

    fn sql(&self) -> &SqlStr {
        &self.sql
    }

    fn parameters(&self) -> Option<Either<&[TursoTypeInfo], usize>> {
        self.parameters.map(Either::Right)
    }

    fn columns(&self) -> &[TursoColumn] {
        &self.columns
    }

    impl_statement_query!(TursoArguments);
}

impl ColumnIndex<TursoStatement> for str {
    fn index(&self, statement: &TursoStatement) -> Result<usize, sqlx_core::error::Error> {
        statement
            .column_names
            .get(self)
            .copied()
            .ok_or_else(|| sqlx_core::error::Error::ColumnNotFound(self.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx_core::{
        column::Column,
        connection::{ConnectOptions, Connection},
        executor::Executor,
        row::Row,
    };

    #[tokio::test]
    async fn prepared_metadata_survives_close_and_uses_the_pinned_target() -> sqlx_core::Result<()>
    {
        let mut original = crate::TursoConnectOptions::new().connect().await?;
        original.execute("CREATE TABLE items(id INTEGER, label TEXT); INSERT INTO items VALUES(1, 'original')").await?;
        let sql = SqlStr::from_static("SELECT label FROM items WHERE id=?");
        let prepared = original.prepare(sql.clone()).await?;
        let cloned = prepared.clone();
        let description = original.describe(sql.clone()).await?;
        assert_eq!(original.cached_statements_size(), 1);
        original.close().await?;

        let mut target = crate::TursoConnectOptions::new().connect().await?;
        target
            .execute(
                "CREATE TABLE items(id INTEGER, label TEXT); INSERT INTO items VALUES(1, 'target')",
            )
            .await?;
        let row = cloned.query().bind(1_i64).fetch_one(&mut target).await?;
        assert_eq!(row.try_get::<String, _>("label")?, "target");
        assert_eq!(prepared.sql().as_str(), sql.as_str());
        assert_eq!(prepared.columns().len(), 1);
        assert_eq!(prepared.columns()[0].name(), "label");
        assert!(prepared.parameters().is_none());
        assert_eq!(description.columns()[0].name(), "label");
        target.close().await?;
        Ok(())
    }
}
