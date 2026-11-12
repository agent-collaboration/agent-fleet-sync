//! SQLx migrations inside a host's already-open storage transaction.
//!
//! [`run_migrations`] hands SQLx's [`Migrator`] a [`Migrate`] adapter over the
//! caller's [`TransactionScope`]. SQLx alone decides which versions are
//! pending, compares checksums and refuses dirty or unknown history; the
//! adapter only reads and validates the stored history, runs the one migration
//! SQLx asks for, and writes its receipt. Nothing here begins or commits: the
//! host's transaction does, so a failed migration leaves neither its SQL nor
//! its receipt.
//!
//! SQLx does not enforce every rule the existing histories rely on, so the
//! adapter also refuses, before SQLx traverses anything:
//! - a source that is not strictly ordered, has a repeated version, contains a
//!   `no_tx` migration or has no up migration;
//! - a stored receipt whose fields do not have their recorded storage classes;
//! - an unapplied source version below the highest applied one, which stock
//!   SQLx would otherwise apply.
use crate::{BoxFuture, StoreError, storage::TransactionScope};
use runtime_primitives::Clock;
use sqlx_core::{
    arguments::IntoArguments,
    column::ColumnIndex,
    database::Database,
    decode::Decode,
    encode::Encode,
    migrate::{AppliedMigration, Migrate, MigrateError, Migration, Migrator},
    query::query,
    raw_sql::raw_sql,
    row::Row,
    sql_str::AssertSqlSafe,
    types::Type,
};
use std::{borrow::Cow, collections::BTreeSet, time::Duration};

/// What one successful run did. Call-local observations, not another history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationProgress {
    /// Versions applied by this run, each counted after its receipt was written.
    pub count: usize,
    /// The highest version the stored history held when SQLx read it.
    pub from: Option<i64>,
    /// The source's highest up version.
    pub to: i64,
}

/// Why a run stopped. The host's transaction must roll back on every variant.
#[derive(Debug)]
pub enum MigrationRunError {
    /// SQLx refused the history: dirty, unknown or changed versions.
    Policy(MigrateError),
    /// The source or the stored history broke a rule SQLx does not check, or
    /// the history could not be read.
    History(HistoryFault),
    /// SQLx asked to apply `version` and it failed.
    AtVersion {
        version: i64,
        cause: MigrationVersionCause,
    },
}

/// The phase of a pending version that failed, with its original error.
#[derive(Debug)]
pub enum MigrationVersionCause {
    /// The host's check before this version's SQL.
    Preflight(StoreError),
    /// The migration's own SQL.
    Statement(sqlx_core::Error),
    /// Writing the version's receipt after its SQL succeeded.
    Receipt(sqlx_core::Error),
}

#[derive(Debug)]
pub enum HistoryFault {
    /// Creating or reading the history table failed.
    Storage(sqlx_core::Error),
    /// The configured history table is not a plain SQL identifier.
    TableName,
    Source(SourceFault),
    /// A stored receipt field lacks its recorded storage class or value.
    Record {
        version: Option<i64>,
        field: ReceiptField,
    },
    DuplicateVersion(i64),
    /// `version` was never applied, but `highest` was.
    LowerHole {
        version: i64,
        highest: i64,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub enum SourceFault {
    /// No up migration.
    Empty,
    /// `version` does not follow its predecessor strictly.
    Unordered { version: i64 },
    /// `version` asks to run outside a transaction.
    NoTransaction { version: i64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceiptField {
    Version,
    Description,
    InstalledOn,
    Success,
    Checksum,
    ExecutionTime,
}

impl ReceiptField {
    /// The history table's column for this field.
    pub const fn column(self) -> &'static str {
        match self {
            Self::Version => "version",
            Self::Description => "description",
            Self::InstalledOn => "installed_on",
            Self::Success => "success",
            Self::Checksum => "checksum",
            Self::ExecutionTime => "execution_time",
        }
    }
}

/// How a history records its receipts. Each existing history keeps its own.
#[derive(Clone, Copy)]
pub enum ReceiptRepresentation<'clock> {
    /// `installed_on` is the injected clock's UTC time as `%Y-%m-%d %H:%M:%S`
    /// and `execution_time` the nonnegative milliseconds between the clock's
    /// readings before the SQL and after it. Every stored field is validated.
    Clocked(&'clock dyn Clock),
    /// `installed_on` is SQLite's `CURRENT_TIMESTAMP` default and
    /// `execution_time` is `-1`, an unknown duration. Only the version,
    /// success and checksum classes are validated, so that sentinel stays valid.
    StorageDefault,
}

/// The host's check before a pending version's SQL, in the same transaction.
///
/// Called exactly when SQLx applies `version`. An error stops the run with
/// [`MigrationVersionCause::Preflight`] carrying that error unchanged.
pub trait MigrationPreflight<S: ?Sized>: Send {
    fn before_apply<'call>(
        &'call mut self,
        scope: &'call mut S,
        version: i64,
    ) -> BoxFuture<'call, Result<(), StoreError>>;
}

/// A preflight that checks nothing.
pub struct NoPreflight;

impl<S: ?Sized + Send> MigrationPreflight<S> for NoPreflight {
    fn before_apply<'call>(
        &'call mut self,
        _scope: &'call mut S,
        _version: i64,
    ) -> BoxFuture<'call, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }
}

/// A migrator for one fixed history table, keeping `migrations` in the order
/// given. `Migrator::with_migrations` sorts, which would hide an unordered
/// source from [`run_migrations`]'s refusal. This is the one place that sets
/// SQLx's semver-exempt fields; the pinned SQLx version's tests cover it.
pub fn history_migrator(table: &'static str, migrations: Vec<Migration>) -> Migrator {
    Migrator {
        migrations: Cow::Owned(migrations),
        table_name: Cow::Borrowed(table),
        ..Migrator::DEFAULT
    }
}

/// Runs `migrator` on the history table it names, inside `scope`'s transaction.
///
/// # Errors
///
/// Any [`MigrationRunError`]. The caller must let its transaction roll back;
/// progress is only returned when every pending version and receipt succeeded.
pub async fn run_migrations<S, P>(
    scope: &mut S,
    migrator: &Migrator,
    preflight: &mut P,
    receipts: ReceiptRepresentation<'_>,
) -> Result<MigrationProgress, MigrationRunError>
where
    S: TransactionScope,
    P: MigrationPreflight<S>,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> i64: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<i64>: Type<S::Database> + Decode<'value, S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'value> Vec<u8>: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<Vec<u8>>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    if !is_plain_identifier(&migrator.table_name) {
        return Err(MigrationRunError::History(HistoryFault::TableName));
    }
    let to = validate_source(migrator)
        .map_err(|fault| MigrationRunError::History(HistoryFault::Source(fault)))?;
    let mut adapter = ScopeMigrate {
        scope,
        preflight,
        receipts,
        source: migrator,
        history: None,
        from: None,
        applied: 0,
        fault: None,
    };
    let outcome = migrator.run_direct(None, &mut adapter, false).await;
    // The adapter's own error, when it set one, is the run's cause: SQLx only
    // saw the placeholder it had to return.
    match (adapter.fault.take(), outcome) {
        (Some(fault), _) => Err(fault),
        (None, Err(error)) => Err(MigrationRunError::Policy(error)),
        (None, Ok(())) => Ok(MigrationProgress {
            count: adapter.applied,
            from: adapter.from,
            to,
        }),
    }
}

/// The highest up version, after checking the source's order and transactions.
fn validate_source(migrator: &Migrator) -> Result<i64, SourceFault> {
    let mut previous: Option<i64> = None;
    for migration in migrator
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        if migration.no_tx {
            return Err(SourceFault::NoTransaction {
                version: migration.version,
            });
        }
        if previous.is_some_and(|version| migration.version <= version) {
            return Err(SourceFault::Unordered {
                version: migration.version,
            });
        }
        previous = Some(migration.version);
    }
    previous.ok_or(SourceFault::Empty)
}

/// Fixed library- and domain-owned table names only: `[A-Za-z_][A-Za-z0-9_]*`.
fn is_plain_identifier(name: &str) -> bool {
    let mut characters = name.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

/// The existing history DDL, byte for byte apart from the table name, so a
/// table created here stores the same schema text as one created before.
fn history_table_sql(table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {table} (\n    version BIGINT PRIMARY KEY,\n    description TEXT NOT NULL,\n    installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n    success BOOLEAN NOT NULL,\n    checksum BLOB NOT NULL,\n    execution_time BIGINT NOT NULL\n);\n"
    )
}

fn history_read_sql(table: &str) -> String {
    format!(
        "SELECT CAST(version AS INTEGER) AS version,description,CAST(success AS INTEGER) AS success,checksum,CAST(execution_time AS INTEGER) AS execution_time,\nCAST(installed_on AS TEXT) AS installed_on,\ntypeof(version) AS version_class,typeof(success) AS success_class,typeof(checksum) AS checksum_class,\ntypeof(execution_time) AS execution_class,typeof(installed_on) AS installed_class\nFROM {table} ORDER BY version;\n"
    )
}

/// One stored receipt, as read, before validation.
struct StoredReceipt {
    version: Option<i64>,
    description: Option<String>,
    success: Option<i64>,
    checksum: Option<Vec<u8>>,
    execution_time: Option<i64>,
    installed_on: Option<String>,
    version_class: Option<String>,
    success_class: Option<String>,
    checksum_class: Option<String>,
    execution_class: Option<String>,
    installed_class: Option<String>,
}

/// A receipt that passed validation. `dirty` is SQLx's to judge.
struct ValidReceipt {
    version: i64,
    checksum: Vec<u8>,
    dirty: bool,
}

fn validate_receipt(
    receipt: StoredReceipt,
    representation: ReceiptRepresentation<'_>,
) -> Result<ValidReceipt, HistoryFault> {
    let invalid = |field| HistoryFault::Record {
        version: receipt.version,
        field,
    };
    let class = |class: &Option<String>, expected: &str| class.as_deref() == Some(expected);
    let version = receipt
        .version
        .filter(|_| class(&receipt.version_class, "integer"))
        .ok_or_else(|| HistoryFault::Record {
            version: None,
            field: ReceiptField::Version,
        })?;
    // 1 is applied; 0 is a dirty record SQLx refuses; nothing else is a receipt.
    let dirty = match receipt
        .success
        .filter(|_| class(&receipt.success_class, "integer"))
    {
        Some(1) => false,
        Some(0) => true,
        _ => return Err(invalid(ReceiptField::Success)),
    };
    let checksum = receipt
        .checksum
        .clone()
        .filter(|_| class(&receipt.checksum_class, "blob"))
        .ok_or_else(|| invalid(ReceiptField::Checksum))?;
    if let ReceiptRepresentation::Clocked(_) = representation {
        if !receipt
            .description
            .as_ref()
            .is_some_and(|description| !description.is_empty())
        {
            return Err(invalid(ReceiptField::Description));
        }
        let installed = receipt.installed_on.as_deref().is_some_and(|text| {
            class(&receipt.installed_class, "text") && parse_installed_on(text).is_some()
        });
        if !installed {
            return Err(invalid(ReceiptField::InstalledOn));
        }
        if !(class(&receipt.execution_class, "integer")
            && receipt.execution_time.is_some_and(|elapsed| elapsed >= 0))
        {
            return Err(invalid(ReceiptField::ExecutionTime));
        }
    }
    Ok(ValidReceipt {
        version,
        checksum,
        dirty,
    })
}

fn parse_installed_on(text: &str) -> Option<chrono::NaiveDateTime> {
    chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S").ok()
}

/// SQLx's `installed_on` text, as SQLite's `CURRENT_TIMESTAMP` writes it.
fn installed_on_text(unix_millis: i64) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(unix_millis)
        .map(|time| time.format("%Y-%m-%d %H:%M:%S").to_string())
}

/// The adapter SQLx drives. It exists only for one [`run_migrations`] call,
/// inside the scope's open transaction; that exclusivity is why `lock` and
/// `unlock` do nothing.
struct ScopeMigrate<'run, S, P> {
    scope: &'run mut S,
    preflight: &'run mut P,
    receipts: ReceiptRepresentation<'run>,
    source: &'run Migrator,
    /// The validated history, read once when SQLx first asks about it.
    history: Option<Vec<ValidReceipt>>,
    from: Option<i64>,
    applied: usize,
    /// The original cause when the adapter, not SQLx, stopped the run.
    fault: Option<MigrationRunError>,
}

/// What the adapter returns to SQLx after recording its own cause in `fault`.
fn stopped() -> MigrateError {
    MigrateError::Execute(sqlx_core::Error::Protocol(
        "the migration run stopped; its cause is returned by run_migrations".into(),
    ))
}

impl<S, P> ScopeMigrate<'_, S, P>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> Option<i64>: Type<S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'value> Option<Vec<u8>>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    /// Reads and validates the history once; later calls reuse it.
    async fn history(&mut self, table: &str) -> Result<&[ValidReceipt], MigrateError> {
        if self.history.is_none() {
            match self.read_history(table).await {
                Ok(history) => {
                    self.from = history.last().map(|receipt| receipt.version);
                    self.history = Some(history);
                }
                Err(fault) => {
                    self.fault = Some(MigrationRunError::History(fault));
                    return Err(stopped());
                }
            }
        }
        Ok(self.history.as_deref().unwrap_or_default())
    }

    async fn read_history(&mut self, table: &str) -> Result<Vec<ValidReceipt>, HistoryFault> {
        let rows = query::<S::Database>(AssertSqlSafe(history_read_sql(table)))
            .fetch_all(self.scope.executor())
            .await
            .map_err(HistoryFault::Storage)?;
        let mut history = Vec::with_capacity(rows.len());
        let mut versions = BTreeSet::new();
        for row in &rows {
            let stored = decode_receipt(row).map_err(HistoryFault::Storage)?;
            let receipt = validate_receipt(stored, self.receipts)?;
            if !versions.insert(receipt.version) {
                return Err(HistoryFault::DuplicateVersion(receipt.version));
            }
            history.push(receipt);
        }
        // Stock SQLx would apply a source version below the highest applied
        // one. The existing histories are an applied prefix; keep them so.
        // Only versions the source knows count: an applied version missing
        // from the source is SQLx's `VersionMissing` to refuse.
        let up_versions = || {
            self.source
                .iter()
                .filter(|migration| migration.migration_type.is_up_migration())
                .map(|migration| migration.version)
        };
        if let Some(highest) = up_versions()
            .filter(|version| versions.contains(version))
            .max()
            && let Some(hole) =
                up_versions().find(|version| *version < highest && !versions.contains(version))
        {
            return Err(HistoryFault::LowerHole {
                version: hole,
                highest,
            });
        }
        Ok(history)
    }
}

fn decode_receipt<R: Row>(row: &R) -> Result<StoredReceipt, sqlx_core::Error>
where
    for<'value> Option<i64>: Type<R::Database> + Decode<'value, R::Database>,
    for<'value> Option<String>: Type<R::Database> + Decode<'value, R::Database>,
    for<'value> Option<Vec<u8>>: Type<R::Database> + Decode<'value, R::Database>,
    for<'row> &'row str: ColumnIndex<R>,
{
    Ok(StoredReceipt {
        version: row.try_get("version")?,
        description: row.try_get("description")?,
        success: row.try_get("success")?,
        checksum: row.try_get("checksum")?,
        execution_time: row.try_get("execution_time")?,
        installed_on: row.try_get("installed_on")?,
        version_class: row.try_get("version_class")?,
        success_class: row.try_get("success_class")?,
        checksum_class: row.try_get("checksum_class")?,
        execution_class: row.try_get("execution_class")?,
        installed_class: row.try_get("installed_class")?,
    })
}

impl<S, P> Migrate for ScopeMigrate<'_, S, P>
where
    S: TransactionScope,
    P: MigrationPreflight<S>,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> i64: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<i64>: Type<S::Database> + Decode<'value, S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'value> Vec<u8>: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<Vec<u8>>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    fn create_schema_if_not_exists<'e>(
        &'e mut self,
        schema_name: &'e str,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        // `run_migrations` never configures schemas.
        Box::pin(async move {
            Err(MigrateError::CreateSchemasNotSupported(
                schema_name.to_owned(),
            ))
        })
    }

    fn ensure_migrations_table<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        Box::pin(async move {
            if let Err(error) = raw_sql(AssertSqlSafe(history_table_sql(table_name)))
                .execute(self.scope.executor())
                .await
            {
                self.fault = Some(MigrationRunError::History(HistoryFault::Storage(error)));
                return Err(stopped());
            }
            Ok(())
        })
    }

    fn dirty_version<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<Option<i64>, MigrateError>> {
        Box::pin(async move {
            Ok(self
                .history(table_name)
                .await?
                .iter()
                .find(|receipt| receipt.dirty)
                .map(|receipt| receipt.version))
        })
    }

    fn list_applied_migrations<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<Vec<AppliedMigration>, MigrateError>> {
        Box::pin(async move {
            Ok(self
                .history(table_name)
                .await?
                .iter()
                .map(|receipt| AppliedMigration {
                    version: receipt.version,
                    checksum: Cow::Owned(receipt.checksum.clone()),
                })
                .collect())
        })
    }

    fn lock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async { Ok(()) })
    }

    fn unlock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async { Ok(()) })
    }

    fn apply<'e>(
        &'e mut self,
        table_name: &'e str,
        migration: &'e Migration,
    ) -> BoxFuture<'e, Result<Duration, MigrateError>> {
        Box::pin(async move {
            let version = migration.version;
            let mut stop = |cause| {
                self.fault = Some(MigrationRunError::AtVersion { version, cause });
                stopped()
            };
            if let Err(error) = self.preflight.before_apply(self.scope, version).await {
                return Err(stop(MigrationVersionCause::Preflight(error)));
            }
            let started = match self.receipts {
                ReceiptRepresentation::Clocked(clock) => Some(clock.now().unix_millis()),
                ReceiptRepresentation::StorageDefault => None,
            };
            if let Err(error) = raw_sql(migration.sql.clone())
                .execute(self.scope.executor())
                .await
            {
                return Err(stop(MigrationVersionCause::Statement(error)));
            }
            let written = match (self.receipts, started) {
                (ReceiptRepresentation::Clocked(clock), Some(started)) => {
                    let installed = clock.now().unix_millis();
                    let elapsed = installed.saturating_sub(started).max(0);
                    match installed_on_text(installed) {
                        Some(installed_on) => query::<S::Database>(AssertSqlSafe(format!(
                            "INSERT INTO {table_name}(version,description,installed_on,success,checksum,execution_time)\nVALUES(?1,?2,?3,1,?4,?5);\n"
                        )))
                        .bind(version)
                        .bind(migration.description.to_string())
                        .bind(installed_on)
                        .bind(migration.checksum.to_vec())
                        .bind(elapsed)
                        .execute(self.scope.executor())
                        .await
                        .map(|_| Duration::from_millis(elapsed.unsigned_abs())),
                        None => Err(sqlx_core::Error::Encode(
                            "migration time is outside UTC range".into(),
                        )),
                    }
                }
                _ => query::<S::Database>(AssertSqlSafe(format!(
                    "INSERT INTO {table_name}(version,description,success,checksum,execution_time) VALUES(CAST(? AS INTEGER),?,TRUE,?,-1)"
                )))
                .bind(version.to_string())
                .bind(migration.description.to_string())
                .bind(migration.checksum.to_vec())
                .execute(self.scope.executor())
                .await
                .map(|_| Duration::ZERO),
            };
            match written {
                Ok(elapsed) => {
                    self.applied += 1;
                    Ok(elapsed)
                }
                Err(error) => Err(stop(MigrationVersionCause::Receipt(error))),
            }
        })
    }

    fn revert<'e>(
        &'e mut self,
        _table_name: &'e str,
        migration: &'e Migration,
    ) -> BoxFuture<'e, Result<Duration, MigrateError>> {
        // `run_migrations` never asks SQLx to undo; histories are append-only.
        let version = migration.version;
        Box::pin(async move {
            Err(MigrateError::ExecuteMigration(
                sqlx_core::Error::Protocol("down migrations are not supported".into()),
                version,
            ))
        })
    }
}

#[cfg(test)]
#[path = "scope_migrations_test.rs"]
mod tests;
