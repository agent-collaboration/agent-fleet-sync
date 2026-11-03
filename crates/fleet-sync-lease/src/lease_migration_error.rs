#[derive(Debug, thiserror::Error)]
pub enum LeaseMigrationError {
    #[error("invalid legacy {table} row {key}: {field}")]
    InvalidLegacyRow {
        table: &'static str,
        key: String,
        field: &'static str,
    },
    #[error(transparent)]
    Sql(#[from] sqlx_core::Error),
    #[error(transparent)]
    Migration(#[from] sqlx_core::migrate::MigrateError),
}
pub(crate) fn invalid(table: &'static str, key: &str, field: &'static str) -> LeaseMigrationError {
    LeaseMigrationError::InvalidLegacyRow {
        table,
        key: key.into(),
        field,
    }
}
