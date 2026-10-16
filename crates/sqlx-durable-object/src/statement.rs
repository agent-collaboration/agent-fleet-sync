use sqlx_core::Either;
use sqlx_core::sql_str::SqlStr;
use sqlx_core::statement::Statement;

use crate::{Do, DoArguments, DoColumn, DoTypeInfo};

/// A "prepared" Durable Object storage statement: the SQL and nothing else.
///
/// `sql.exec` has no separate prepare that reports parameters or columns, so there
/// is nothing to learn by preparing ahead of time. This exists because sqlx's
/// `Executor::prepare` has to return something.
#[derive(Debug, Clone)]
pub struct DoStatement {
    pub(crate) sql: SqlStr,
}

impl Statement for DoStatement {
    type Database = Do;

    fn into_sql(self) -> SqlStr {
        self.sql
    }

    fn sql(&self) -> &SqlStr {
        &self.sql
    }

    fn parameters(&self) -> Option<Either<&[DoTypeInfo], usize>> {
        None
    }

    fn columns(&self) -> &[DoColumn] {
        &[]
    }

    sqlx_core::impl_statement_query!(DoArguments);
}
