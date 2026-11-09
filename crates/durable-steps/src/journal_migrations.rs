//! SQLx receipts within the host's already-open storage transaction.
#[cfg(feature = "durable-object")]
use crate::storage::TransactionScope;
use crate::{StoreError, checked_migration_queries};
#[cfg(feature = "durable-object")]
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, migrate::Migration, types::Type,
};

pub(crate) static JOURNAL_CORPUS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

fn corrupt(field: &str) -> StoreError {
    StoreError::Corrupt {
        field: field.into(),
        message: "invalid journal migration receipt".into(),
    }
}

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

/// The caller must roll back on every error, including a receipt refusal.
#[cfg(feature = "durable-object")]
pub(crate) async fn run_in_scope<S>(scope: &mut S) -> Result<(), StoreError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Vec<u8>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let tables = checked_migration_queries::read_migration_tables(scope).await?;
    if !tables
        .iter()
        .any(|row| row.name.as_deref() == Some("_fleet_sync_migrations"))
    {
        if !tables.is_empty() {
            return Err(corrupt("unregistered existing journal tables"));
        }
        sqlx_core::raw_sql::raw_sql(include_str!(
            "../query-schema/99990000_journal_receipts.sql"
        ))
        .execute(scope.executor())
        .await?;
    }
    let rows = checked_migration_queries::read_migration_receipts(scope).await?;
    validate_receipts(&rows)?;
    for migration in JOURNAL_CORPUS.iter().skip(rows.len()) {
        apply_in_scope(scope, migration).await?;
    }
    Ok(())
}

#[cfg(feature = "durable-object")]
async fn apply_in_scope<S>(scope: &mut S, migration: &Migration) -> Result<(), StoreError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Vec<u8>: Type<S::Database> + Encode<'value, S::Database>,
{
    sqlx_core::raw_sql::raw_sql(migration.sql.clone())
        .execute(scope.executor())
        .await?;
    checked_migration_queries::write_migration_receipt(
        scope,
        migration.version.to_string(),
        migration.description.to_string(),
        migration.checksum.to_vec(),
    )
    .await?;
    Ok(())
}
