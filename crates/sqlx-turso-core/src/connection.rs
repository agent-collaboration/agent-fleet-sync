use std::{collections::VecDeque, fmt, future};

use sqlx_core::{
    common::StatementCache, connection::Connection, error::Error, transaction::Transaction,
};

use crate::{
    Turso, TursoConnectOptions, TursoStatement, busy_wait::BusyWait,
    engine_connection::EngineConnection, error::map_turso_error,
};

/// One owned Turso engine connection
pub struct TursoConnection {
    options: TursoConnectOptions,
    raw: turso::Connection,
    statements: StatementCache<TursoStatement>,
    /// The schema cookie the cached statements were prepared against
    statements_schema_version: Option<i64>,
    /// `PRAGMA schema_version`, prepared on the first check and kept outside the evictable cache
    schema_version_statement: Option<turso::Statement>,
    transaction_state: TransactionState,
}

impl fmt::Debug for TursoConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TursoConnection")
            .field("options", &self.options)
            .field("transaction_state", &self.transaction_state)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct TransactionState {
    depth: usize,
    pending_rollback_depths: VecDeque<usize>,
    rollback_failed: bool,
    engine_outcome: EngineTransactionOutcome,
}

#[derive(Debug, Default, Eq, PartialEq)]
enum EngineTransactionOutcome {
    #[default]
    Managed,
    RolledBack,
    Uncertain,
}

#[derive(Clone, Copy)]
pub(crate) struct StatementRollbackObservation {
    live_managed_transaction: bool,
    may_witness_rollback: bool,
    check_success: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum CallerTransactionControl {
    Ordinary,
    PreservesOuterTransaction,
    MayEndTransaction,
}

impl TransactionState {
    fn depth(&self) -> usize {
        self.depth
    }

    fn increment_depth(&mut self) {
        self.depth += 1;
    }

    fn decrement_depth(&mut self) {
        if self.depth > 0 {
            self.depth -= 1;
        }
    }

    fn mark_rollback_needed(&mut self) {
        if self.depth == 0 {
            return;
        }

        self.pending_rollback_depths.push_back(self.depth);
        self.decrement_depth();
    }

    fn pop_pending_rollback(&mut self) -> Option<usize> {
        self.pending_rollback_depths.pop_front()
    }

    fn has_failed_rollback(&self) -> bool {
        self.rollback_failed
    }

    fn mark_rollback_failed(&mut self) {
        self.rollback_failed = true;
        self.pending_rollback_depths.clear();
    }
}

impl TursoConnection {
    pub(crate) fn new(options: TursoConnectOptions, connection: EngineConnection) -> Self {
        Self {
            statements: StatementCache::new(options.get_statement_cache_capacity()),
            options,
            raw: connection.raw,
            statements_schema_version: None,
            schema_version_statement: None,
            transaction_state: TransactionState::default(),
        }
    }

    pub(crate) fn transaction_depth(&self) -> usize {
        self.transaction_state.depth()
    }

    pub(crate) fn increment_transaction_depth(&mut self) {
        if self.transaction_state.depth() == 0 {
            self.transaction_state.engine_outcome = EngineTransactionOutcome::Managed;
        }
        self.transaction_state.increment_depth();
    }

    pub(crate) fn decrement_transaction_depth(&mut self) {
        self.transaction_state.decrement_depth();
    }

    pub(crate) fn mark_rollback_needed(&mut self) {
        self.transaction_state.mark_rollback_needed();
    }

    pub(crate) async fn clear_pending_rollback(&mut self) -> Result<(), Error> {
        if self.transaction_state.has_failed_rollback() {
            return Err(Error::WorkerCrashed);
        }

        while let Some(depth) = self.transaction_state.pop_pending_rollback() {
            self.forget_cached_statements();
            if self.engine_rolled_back() {
                continue;
            }
            let sql = sqlx_core::transaction::rollback_ansi_transaction_sql(depth);
            if let Err(error) = self.execute_raw_statement(sql.as_str()).await {
                if depth == 1 && rollback_error_is_inactive_transaction(&error) {
                    continue;
                }

                self.transaction_state.mark_rollback_failed();
                return Err(error);
            }
        }

        Ok(())
    }

    pub(crate) fn engine_rolled_back(&self) -> bool {
        self.transaction_state.engine_outcome == EngineTransactionOutcome::RolledBack
    }

    pub(crate) fn require_live_transaction(&self) -> Result<(), Error> {
        if self.transaction_depth() > 0 && self.engine_rolled_back() {
            return Err(Error::Protocol(
                "Turso transaction was rolled back by the engine".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn require_certain_transaction(&self, boundary: &'static str) -> Result<(), Error> {
        match self.transaction_state.engine_outcome {
            EngineTransactionOutcome::Managed => Ok(()),
            EngineTransactionOutcome::RolledBack => Err(Error::Protocol(format!(
                "Turso {boundary} cannot complete: transaction was rolled back by the engine"
            ))),
            EngineTransactionOutcome::Uncertain => Err(Error::Protocol(format!(
                "Turso {boundary} cannot be certified: transaction boundary is not tracked by the driver"
            ))),
        }
    }

    pub(crate) fn before_statement(
        &mut self,
        control: CallerTransactionControl,
    ) -> Result<StatementRollbackObservation, Error> {
        self.require_live_transaction()?;
        let tracked = self.transaction_depth() > 0;
        let engine_live = self.raw().is_autocommit().map(|autocommit| !autocommit);
        if tracked
            && (control == CallerTransactionControl::MayEndTransaction
                || !matches!(engine_live, Ok(true)))
        {
            self.transaction_state.engine_outcome = EngineTransactionOutcome::Uncertain;
        }
        Ok(StatementRollbackObservation {
            live_managed_transaction: tracked
                && matches!(engine_live, Ok(true))
                && self.transaction_state.engine_outcome == EngineTransactionOutcome::Managed,
            may_witness_rollback: control == CallerTransactionControl::Ordinary,
            check_success: control == CallerTransactionControl::PreservesOuterTransaction,
        })
    }

    pub(crate) fn after_statement_error(&mut self, observation: StatementRollbackObservation) {
        if !observation.live_managed_transaction
            || self.transaction_state.engine_outcome != EngineTransactionOutcome::Managed
        {
            return;
        }
        match self.raw().is_autocommit() {
            Ok(true) => {
                self.transaction_state.engine_outcome = if observation.may_witness_rollback {
                    EngineTransactionOutcome::RolledBack
                } else {
                    EngineTransactionOutcome::Uncertain
                };
                self.forget_cached_statements();
            }
            Ok(false) => {}
            Err(_) => {
                self.transaction_state.engine_outcome = EngineTransactionOutcome::Uncertain;
            }
        }
    }

    pub(crate) fn after_statement_success(&mut self, observation: StatementRollbackObservation) {
        if observation.live_managed_transaction
            && observation.check_success
            && !matches!(self.raw().is_autocommit(), Ok(false))
        {
            self.transaction_state.engine_outcome = EngineTransactionOutcome::Uncertain;
        }
    }

    /// Returns the options used to create this connection
    pub fn options(&self) -> &TursoConnectOptions {
        &self.options
    }

    pub(crate) fn raw(&self) -> &turso::Connection {
        &self.raw
    }

    pub(crate) async fn execute_raw_statement(&self, sql: &str) -> Result<u64, Error> {
        let mut waiting = BusyWait::new(self.options.get_busy_timeout());
        loop {
            match self.raw().execute(sql, ()).await {
                Ok(changes) => return Ok(changes),
                Err(error) => waiting.wait_after_error(error).await?,
            }
        }
    }

    pub(crate) async fn prepare_raw_statement(&self, sql: &str) -> Result<turso::Statement, Error> {
        let mut waiting = BusyWait::new(self.options.get_busy_timeout());
        loop {
            match self.raw().prepare(sql).await {
                Ok(statement) => return Ok(statement),
                Err(error) => waiting.wait_after_error(error).await?,
            }
        }
    }

    pub(crate) fn cached_statement(&mut self, sql: &str) -> Option<TursoStatement> {
        self.statements.get_mut(sql).cloned()
    }

    /// Drops every cached statement when the schema changed since they were prepared
    ///
    /// The engine reprepares a stale statement only when it first steps, after the driver and
    /// the SDK have already taken its column list, so a statement cached before a schema change
    /// (a local migration or another connection's) would keep returning the old
    /// result shape. The schema cookie changes with every schema change, so it is compared
    /// before every single statement this driver prepares, cached or not.
    ///
    /// The cookie only grows, except across a rollback: that restores an earlier schema and its
    /// cookie, and a later change can then reach a cookie the cache recorded for a schema that
    /// no longer exists. Whatever can roll back between two comparisons therefore forgets the
    /// cache instead ([`Self::forget_cached_statements`]): the transaction manager's rollbacks,
    /// deferred rollbacks and multi-statement batches. Each forgets before it starts, so a
    /// future dropped part-way cannot skip it. A rollback written as a single statement is seen
    /// by the comparison before the next one.
    pub(crate) async fn discard_statements_prepared_for_another_schema(
        &mut self,
    ) -> Result<(), Error> {
        if !self.statements.is_enabled() {
            return Ok(());
        }
        let schema_version = self.read_schema_version().await?;
        if self.statements_schema_version != Some(schema_version) {
            self.statements.clear();
            self.statements_schema_version = Some(schema_version);
        }
        Ok(())
    }

    /// Drops every cached statement and the schema cookie they were checked against
    pub(crate) fn forget_cached_statements(&mut self) {
        self.statements.clear();
        self.statements_schema_version = None;
    }

    async fn read_schema_version(&mut self) -> Result<i64, Error> {
        let mut statement = match self.schema_version_statement.take() {
            Some(statement) => statement,
            None => self.prepare_raw_statement("PRAGMA schema_version").await?,
        };
        let schema_version =
            read_schema_cookie(&mut statement, self.options.get_busy_timeout()).await;
        self.schema_version_statement = Some(statement);
        schema_version
    }

    pub(crate) fn cache_statement(&mut self, sql: &str, statement: TursoStatement) {
        if self.statements.is_enabled() {
            self.statements.insert(sql, statement);
        }
    }
}

async fn read_schema_cookie(
    statement: &mut turso::Statement,
    timeout: std::time::Duration,
) -> Result<i64, Error> {
    let mut rows = statement.query(()).await.map_err(map_turso_error)?;
    let value = match BusyWait::new(timeout).next_row(&mut rows).await? {
        Some(row) => row.get_value(0).map_err(map_turso_error)?,
        None => turso::Value::Null,
    };
    drop(rows);
    // The statement stopped on its row; an unfinished statement would hold its read snapshot
    // and pin the connection to the schema it saw.
    statement.reset().map_err(map_turso_error)?;
    match value {
        turso::Value::Integer(schema_version) => Ok(schema_version),
        _ => Err(crate::TursoAdapterError::SchemaVersionUnreadable.into()),
    }
}

fn rollback_error_is_inactive_transaction(error: &Error) -> bool {
    error
        .to_string()
        .contains("cannot rollback - no transaction is active")
}

impl Connection for TursoConnection {
    type Database = Turso;
    type Options = TursoConnectOptions;

    async fn close(self) -> Result<(), Error> {
        Ok(())
    }

    async fn close_hard(self) -> Result<(), Error> {
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), Error> {
        // SQLx pool return calls ping before admitting an idle connection. A
        // queued rollback must release its native lock before that closing fact.
        self.clear_pending_rollback().await
    }

    async fn begin(&mut self) -> Result<Transaction<'_, Self::Database>, Error> {
        Transaction::begin(self, None).await
    }

    fn shrink_buffers(&mut self) {}

    async fn flush(&mut self) -> Result<(), Error> {
        self.clear_pending_rollback().await
    }

    fn should_flush(&self) -> bool {
        !self.transaction_state.pending_rollback_depths.is_empty()
    }

    fn cached_statements_size(&self) -> usize
    where
        Self::Database: sqlx_core::database::HasStatementCache,
    {
        self.statements.len()
    }

    fn clear_cached_statements(&mut self) -> impl Future<Output = Result<(), Error>> + Send + '_
    where
        Self::Database: sqlx_core::database::HasStatementCache,
    {
        self.forget_cached_statements();
        future::ready(Ok(()))
    }
}

impl AsRef<TursoConnectOptions> for TursoConnection {
    fn as_ref(&self) -> &TursoConnectOptions {
        &self.options
    }
}
