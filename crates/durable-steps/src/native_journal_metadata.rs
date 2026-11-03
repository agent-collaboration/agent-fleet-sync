//! Native SQLx owns receipt columns; this adapter adds its library metadata.
use crate::{
    StoreError,
    journal_schema::{JournalMetadataRow, validate_metadata_ids, validate_schema},
};
use sqlx_core::{executor::Executor, query_as::query_as};
use sqlx_turso::{Turso, TursoConnection};

#[derive(sqlx::FromRow)]
struct ReceiptColumn {
    name: String,
}

pub(crate) async fn migrate_receipt_metadata(
    connection: &mut TursoConnection,
) -> Result<(), StoreError> {
    // Bootstrap introspection must work before the new columns exist. Like the
    // SQLx receipt driver, it uses fixed SQL independently of checked task queries.
    let columns = query_as::<Turso, ReceiptColumn>("PRAGMA table_info(_fleet_sync_migrations)")
        .fetch_all(&mut *connection)
        .await?;
    if !columns.iter().any(|column| column.name == "id") {
        connection
            .execute(include_str!(
                "../native-migrations/0001_receipt_metadata.sql"
            ))
            .await?;
    }
    let receipts = query_as::<Turso, JournalMetadataRow>(
        "SELECT version,CASE WHEN typeof(id)='text' THEN id END AS id FROM _fleet_sync_migrations",
    )
    .fetch_all(&mut *connection)
    .await?;
    validate_metadata_ids("_fleet_sync_migrations", &receipts)?;
    let schema = query_as::<Turso, JournalMetadataRow>(
        "SELECT version,CASE WHEN typeof(id)='text' THEN id END AS id FROM _fleet_sync_schema",
    )
    .fetch_all(&mut *connection)
    .await?;
    validate_schema(&schema)
}
