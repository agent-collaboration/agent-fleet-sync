//! Adopt historical extended receipts without changing SQLx's recorded values.
use crate::{
    StoreError, checked_migration_queries, journal_migrations::validate_receipts,
    storage::TransactionScope,
};
use sqlx_turso::{Turso, TursoConnection};

const STANDARD_RECEIPT_COLUMNS: &[(&str, &str)] = &[
    ("version", "BIGINT"),
    ("description", "TEXT"),
    ("installed_on", "TIMESTAMP"),
    ("success", "BOOLEAN"),
    ("checksum", "BLOB"),
    ("execution_time", "BIGINT"),
];
const RELEASED_EXTENDED_COLUMNS: &[(&str, &str)] = &[
    ("id", "TEXT"),
    ("version", "BIGINT"),
    ("description", "TEXT"),
    ("installed_on", "TIMESTAMP"),
    ("success", "BOOLEAN"),
    ("checksum", "BLOB"),
    ("execution_time", "BIGINT"),
    ("created_at", "INTEGER"),
    ("updated_at", "INTEGER"),
];

fn has_receipt_columns(
    columns: &[checked_migration_queries::MigrationColumnRow],
    expected: &[(&str, &str)],
) -> bool {
    columns.len() == expected.len()
        && expected.iter().all(|(name, declared_type)| {
            columns.iter().any(|column| {
                column.column_name.as_deref() == Some(*name)
                    && column
                        .declared_type
                        .as_deref()
                        .is_some_and(|actual| actual.eq_ignore_ascii_case(declared_type))
            })
        })
}

struct ReceiptScope<'connection>(&'connection mut TursoConnection);
impl TransactionScope for ReceiptScope<'_> {
    type Database = Turso;
    type Executor<'query>
        = &'query mut TursoConnection
    where
        Self: 'query;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}

pub(crate) async fn adopt_standard_receipts(
    connection: &mut TursoConnection,
) -> Result<(), StoreError> {
    let columns =
        checked_migration_queries::read_migration_columns(&mut ReceiptScope(connection)).await?;
    if columns.is_empty() || has_receipt_columns(&columns, STANDARD_RECEIPT_COLUMNS) {
        return Ok(());
    }
    if !has_receipt_columns(&columns, RELEASED_EXTENDED_COLUMNS) {
        return Err(StoreError::Corrupt {
            field: "migration receipt schema".into(),
            message: "expected standard SQLx receipts or the released extended receipt columns"
                .into(),
        });
    }
    let receipts =
        checked_migration_queries::read_migration_receipts(&mut ReceiptScope(connection)).await?;
    validate_receipts(&receipts)?;
    sqlx_core::raw_sql::raw_sql(include_str!(
        "../native-migrations/0002_standard_receipts.sql"
    ))
    .execute(&mut *connection)
    .await?;
    Ok(())
}
