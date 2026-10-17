use sqlx_core::{
    error::{BoxDynError, Error},
    executor::Execute,
    sql_str::SqlStr,
};
use sqlx_turso::{Turso, TursoArguments, TursoStatement};

/// SQLx 0.9 consumes Execute to obtain SQL. Preserve its owned arguments,
/// prepared metadata and persistence preference while checking that SQL once.
pub(crate) struct GuardedQuery {
    sql: SqlStr,
    arguments: Option<TursoArguments>,
    statement: Option<TursoStatement>,
    persistent: bool,
}

impl GuardedQuery {
    pub(crate) fn capture<'query, E: Execute<'query, Turso>>(
        mut query: E,
        check: impl FnOnce(&str) -> Result<(), Error>,
    ) -> Result<Self, Error> {
        let arguments = query.take_arguments().map_err(Error::Encode)?;
        let statement = query.statement().cloned();
        let persistent = query.persistent();
        let sql = query.sql();
        check(sql.as_str())?;
        Ok(Self {
            sql,
            arguments,
            statement,
            persistent,
        })
    }
}

impl Execute<'_, Turso> for GuardedQuery {
    fn sql(self) -> SqlStr {
        self.sql
    }
    fn statement(&self) -> Option<&TursoStatement> {
        self.statement.as_ref()
    }
    fn take_arguments(&mut self) -> Result<Option<TursoArguments>, BoxDynError> {
        Ok(self.arguments.take())
    }
    fn persistent(&self) -> bool {
        self.persistent
    }
}
