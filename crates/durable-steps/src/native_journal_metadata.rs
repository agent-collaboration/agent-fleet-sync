//! Adopt historical extended receipts without changing SQLx's recorded values.
use crate::{
    StoreError, checked_migration_queries, journal_migrations::validate_receipts,
    storage::TransactionScope,
};
use sqlx_core::query_as::query_as;
use sqlx_turso::{Turso, TursoConnection};

#[derive(sqlx::FromRow)]
struct ReceiptColumn {
    name: String,
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
    // This bootstrap read also works before a migration history exists.
    let columns = query_as::<Turso, ReceiptColumn>("PRAGMA table_info(_fleet_sync_migrations)")
        .fetch_all(&mut *connection)
        .await?;
    if !columns.iter().any(|column| column.name == "id") {
        return Ok(());
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
