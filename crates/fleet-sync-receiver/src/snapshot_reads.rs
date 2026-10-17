use crate::{
    BoundStatement, ReceiverError, StoreError, bound_sql::quoted_identifier,
    copy_lifecycle::following_status, row_apply::primary_columns, schema_reads::SchemaReads,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{SnapshotCursor, SnapshotPage, SnapshotRead};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub(crate) const PAGE_SIZE: usize = 256;

/// Read one primary-key ordered page at a fixed acknowledged position.
// Explicit Send is required by borrowed-scope host dispatch.
#[allow(clippy::manual_async_fn)]
pub fn snapshot_page<'scope, S>(
    scope: &'scope mut S,
    cursor: Option<SnapshotCursor>,
) -> impl std::future::Future<Output = Result<SnapshotRead, ReceiverError>> + Send + 'scope
where
    S: TransactionScope + 'scope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> f64: Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Vec<u8>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    async move {
        let status = following_status(scope).await?;
        if cursor
            .as_ref()
            .is_some_and(|cursor| cursor.position != status.acked)
        {
            return Ok(SnapshotRead::Moved {
                position: status.acked,
            });
        }
        let tables = SchemaReads(scope).table_names().await?;
        let table_index = match cursor.as_ref() {
            Some(cursor) => tables
                .iter()
                .position(|table| table == &cursor.table)
                .ok_or_else(|| ReceiverError::InvalidInput {
                    field: "snapshot cursor table unavailable".into(),
                })?,
            None => 0,
        };
        let table = tables.get(table_index).cloned().unwrap_or_default();
        let schema_steps = SchemaReads(scope).steps().await?;
        let mut rows = Vec::new();
        let mut next = None;
        if !table.is_empty() {
            let columns = SchemaReads(scope).table_columns(&table).await?;
            let keys = primary_columns(&columns);
            let column_names = columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            let column_sql = column_names
                .iter()
                .map(|name| quoted_identifier(name))
                .collect::<Vec<_>>()
                .join(", ");
            let key_sql = keys
                .iter()
                .map(|column| quoted_identifier(&column.name))
                .collect::<Vec<_>>()
                .join(", ");
            let after = cursor
                .as_ref()
                .map(|cursor| cursor.after_key.as_slice())
                .unwrap_or_default();
            if !after.is_empty()
                && (after.len() != keys.len()
                    || after
                        .iter()
                        .any(|value| matches!(value, fleet_sync_protocol::SqlValue::Null)))
            {
                return Err(ReceiverError::InvalidInput {
                    field: "snapshot cursor key shape mismatch".into(),
                });
            }
            let predicate = if after.is_empty() {
                String::new()
            } else {
                format!(
                    " WHERE ({key_sql}) > ({})",
                    after.iter().map(|_| "?").collect::<Vec<_>>().join(", ")
                )
            };
            let result = BoundStatement::query(
                format!(
                    "SELECT {column_sql} FROM {}{predicate} ORDER BY {key_sql} LIMIT {}",
                    quoted_identifier(&table),
                    PAGE_SIZE + 1
                ),
                after.to_vec(),
                column_names,
            )
            .fetch_rows(scope)
            .await?;
            for row in result.iter().take(PAGE_SIZE) {
                rows.push(
                    columns
                        .iter()
                        .map(|column| row.value(&column.name).cloned())
                        .collect::<Result<Vec<_>, StoreError>>()?,
                );
            }
            if result.len() > PAGE_SIZE {
                let last = result
                    .get(PAGE_SIZE - 1)
                    .ok_or_else(|| StoreError::Corrupt {
                        field: "snapshot page".into(),
                    })?;
                next = Some(SnapshotCursor {
                    position: status.acked,
                    table: table.clone(),
                    after_key: keys
                        .iter()
                        .map(|column| last.value(&column.name).cloned())
                        .collect::<Result<Vec<_>, StoreError>>()?,
                });
            } else if let Some(table) = tables.get(table_index + 1) {
                next = Some(SnapshotCursor {
                    position: status.acked,
                    table: table.clone(),
                    after_key: Vec::new(),
                });
            }
        }
        Ok(SnapshotRead::Page(Box::new(SnapshotPage {
            database: status.database,
            position: status.acked,
            schema: status.schema,
            schema_steps,
            table,
            rows,
            next,
        })))
    }
}
