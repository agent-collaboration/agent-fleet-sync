//! SQLx receipts within the host's already-open storage transaction.
#[cfg(feature = "durable-object")]
use crate::scope_migrations::{
    HistoryFault, MigrationRunError, MigrationVersionCause, NoPreflight, ReceiptRepresentation,
    run_migrations,
};
#[cfg(feature = "durable-object")]
use crate::storage::TransactionScope;
use crate::{StoreError, checked_migration_queries};
#[cfg(feature = "durable-object")]
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, migrate::MigrateError, types::Type,
};

pub(crate) static JOURNAL_CORPUS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

fn corrupt(field: &str) -> StoreError {
    StoreError::Corrupt {
        field: field.into(),
        message: "invalid journal migration receipt".into(),
    }
}

/// The native journal's check before it adopts historical extended receipts.
#[cfg(feature = "turso")]
pub(crate) fn validate_receipts(
    rows: &[checked_migration_queries::MigrationReceiptRow],
) -> Result<(), StoreError> {
    for (index, row) in rows.iter().enumerate() {
        if row.version_class.as_deref() != Some("integer")
            || row.success_class.as_deref() != Some("integer")
            || row.checksum_class.as_deref() != Some("blob")
        {
            return Err(corrupt("migration receipt storage class"));
        }
        let version = row
            .version
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok())
            .ok_or_else(|| corrupt("migration version"))?;
        let expected = JOURNAL_CORPUS
            .iter()
            .nth(index)
            .filter(|migration| migration.version == version)
            .ok_or_else(|| corrupt("migration version"))?;
        if row.success.as_deref() != Some("1") {
            return Err(corrupt("migration success"));
        }
        if row.checksum.as_deref() != Some(expected.checksum.as_ref()) {
            return Err(corrupt("migration checksum"));
        }
    }
    Ok(())
}

/// The journal's history, in its own table, for [`run_in_scope`].
#[cfg(feature = "durable-object")]
static JOURNAL_HISTORY: std::sync::LazyLock<sqlx_core::migrate::Migrator> =
    std::sync::LazyLock::new(|| {
        crate::scope_migrations::history_migrator(
            "_fleet_sync_migrations",
            JOURNAL_CORPUS.iter().cloned().collect(),
        )
    });

/// The caller must roll back on every error, including a receipt refusal.
///
/// Tables the journal owns without its receipt table are refused before SQLx
/// runs: an empty history must not authorize overwriting them. SQLx then
/// decides what is pending; receipts keep the journal's SQL-default time and
/// `-1` unknown duration.
#[cfg(feature = "durable-object")]
pub(crate) async fn run_in_scope<S>(scope: &mut S) -> Result<(), StoreError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> i64: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<i64>: Type<S::Database> + Decode<'value, S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'value> Vec<u8>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<Vec<u8>>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let tables = checked_migration_queries::read_migration_tables(scope).await?;
    if !tables.is_empty()
        && !tables
            .iter()
            .any(|row| row.name.as_deref() == Some("_fleet_sync_migrations"))
    {
        return Err(corrupt("unregistered existing journal tables"));
    }
    run_migrations(
        scope,
        &JOURNAL_HISTORY,
        &mut NoPreflight,
        ReceiptRepresentation::StorageDefault,
    )
    .await
    .map(|_| ())
    .map_err(journal_refusal)
}

/// The journal's existing refusal for each way a run can stop.
#[cfg(feature = "durable-object")]
fn journal_refusal(error: MigrationRunError) -> StoreError {
    match error {
        MigrationRunError::Policy(
            MigrateError::VersionMissing(_)
            | MigrateError::VersionNotPresent(_)
            | MigrateError::VersionTooOld(..)
            | MigrateError::VersionTooNew(..),
        )
        | MigrationRunError::History(HistoryFault::LowerHole { .. })
        | MigrationRunError::History(HistoryFault::DuplicateVersion(_)) => {
            corrupt("migration version")
        }
        MigrationRunError::Policy(MigrateError::VersionMismatch(_)) => {
            corrupt("migration checksum")
        }
        MigrationRunError::Policy(MigrateError::Dirty(_)) => corrupt("migration success"),
        MigrationRunError::History(HistoryFault::Record { .. }) => {
            corrupt("migration receipt storage class")
        }
        MigrationRunError::History(HistoryFault::Storage(error)) => StoreError::from(error),
        MigrationRunError::History(fault @ (HistoryFault::TableName | HistoryFault::Source(_))) => {
            StoreError::backend(format!("journal corpus: {fault:?}"))
        }
        MigrationRunError::Policy(error) => StoreError::backend(error),
        MigrationRunError::AtVersion { cause, .. } => match cause {
            MigrationVersionCause::Preflight(error) => error,
            MigrationVersionCause::Statement(error) | MigrationVersionCause::Receipt(error) => {
                StoreError::from(error)
            }
        },
    }
}
