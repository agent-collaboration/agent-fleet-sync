use crate::{
    BoundStatement, ReceiverError, bound_sql::quoted_identifier, row_apply::primary_columns,
    schema_reads::SchemaReads,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{SnapshotPage, SqlValue};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// Schema history may contain data migrations. Snapshot rows replace all of it.
pub(crate) async fn clear_snapshot_rows<S>(scope: &mut S) -> Result<(), ReceiverError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'a> String: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Option<String>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> f64: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Vec<u8>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    for table in SchemaReads(scope).table_names().await? {
        BoundStatement::new(
            format!("DELETE FROM {}", quoted_identifier(&table)),
            Vec::new(),
        )
        .execute(scope)
        .await?;
    }
    Ok(())
}
pub(crate) async fn insert_snapshot_rows<S>(
    scope: &mut S,
    page: &SnapshotPage,
) -> Result<(), ReceiverError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'a> String: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Option<String>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> f64: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Vec<u8>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    let tables = SchemaReads(scope).table_names().await?;
    if page.table.is_empty() && page.rows.is_empty() && tables.is_empty() {
        return Ok(());
    }
    if !tables.contains(&page.table) {
        return Err(ReceiverError::InvalidInput {
            field: "seed table absent from schema".into(),
        });
    }
    let columns = SchemaReads(scope).table_columns(&page.table).await?;
    let keys = primary_columns(&columns);
    let column_sql = columns
        .iter()
        .map(|column| quoted_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = columns.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
    for row in &page.rows {
        if row.len() != columns.len()
            || keys.iter().any(|key| {
                columns
                    .iter()
                    .position(|column| column.name == key.name)
                    .and_then(|index| row.get(index))
                    .is_none_or(|value| !matches!(value, SqlValue::Text(key) if fleet_sync_protocol::canonical_uuid_v7(key)))
            })
        {
            return Err(crate::StoreError::Corrupt {field:"snapshot UUIDv7 row key or row shape".into()}.into());
        }
        BoundStatement::new(
            format!(
                "INSERT INTO {}({column_sql}) VALUES({placeholders})",
                quoted_identifier(&page.table)
            ),
            row.clone(),
        )
        .execute(scope)
        .await?;
    }
    Ok(())
}
