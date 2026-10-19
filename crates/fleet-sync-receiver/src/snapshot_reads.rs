use crate::{
    BoundStatement, ReceiverError, StoreError, bound_sql::quoted_identifier,
    row_apply::primary_columns, schema_reads::SchemaReads,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::unit_budget::{
    MAX_UNIT_BYTES, TARGET_UNIT_BYTES, UnitTooLarge, measure_snapshot_unit, snapshot_size_after_row,
};
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
        let status = crate::snapshot_install::readable_status(scope).await?;
        snapshot_page_at(scope, cursor, status).await
    }
}

pub(crate) async fn snapshot_page_at<'scope, S>(
    scope: &'scope mut S,
    cursor: Option<SnapshotCursor>,
    status: fleet_sync_protocol::CopyStatus,
) -> Result<SnapshotRead, ReceiverError>
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
    let schema_steps = SchemaReads(scope).snapshot_statements().await?;
    let next_table = tables.get(table_index + 1).map(|table| SnapshotCursor {
        position: status.acked,
        table: table.clone(),
        after_key: Vec::new(),
    });
    let page = SnapshotPage {
        database: status.database,
        position: status.acked,
        schema: status.schema.clone(),
        schema_steps,
        table: table.clone(),
        rows: Vec::new(),
        next: next_table.clone(),
    };
    let measured = measure_page(page).map_err(|_| ReceiverError::InvalidInput {
        field: "snapshot encoding".into(),
    })?;
    if measured.encoded_size > MAX_UNIT_BYTES {
        return Err(UnitTooLarge::SnapshotSchema {
            encoded_size: measured.encoded_size,
        }
        .into());
    }
    let mut encoded_size = measured.encoded_size;
    let mut page = measured.unit;
    if table.is_empty() {
        return checked_page(page, encoded_size);
    }
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
    let read_next = |after: &[fleet_sync_protocol::SqlValue]| {
        let predicate = if after.is_empty() {
            String::new()
        } else {
            format!(
                " WHERE ({key_sql}) > ({})",
                after.iter().map(|_| "?").collect::<Vec<_>>().join(", ")
            )
        };
        BoundStatement::query(
            format!(
                "SELECT {column_sql} FROM {}{predicate} ORDER BY {key_sql} LIMIT 1",
                quoted_identifier(&table)
            ),
            after.to_vec(),
            column_names.clone(),
        )
    };
    // The DO SQLx executor eagerly drains each SQL query. LIMIT 1 bounds that
    // backend buffer while every keyset read stays in the host-owned transaction.
    let mut pending = read_next(after).fetch_rows(scope).await?.pop();
    while let Some(row) = pending {
        let key = keys
            .iter()
            .map(|column| row.value(&column.name).cloned())
            .collect::<Result<Vec<_>, StoreError>>()?;
        let values = columns
            .iter()
            .map(|column| row.value(&column.name).cloned())
            .collect::<Result<Vec<_>, StoreError>>()?;
        let following = read_next(&key).fetch_rows(scope).await?.pop();
        let next = if following.is_some() {
            Some(SnapshotCursor {
                position: status.acked,
                table: table.clone(),
                after_key: key.clone(),
            })
        } else {
            next_table.clone()
        };
        let candidate_size = snapshot_size_after_row(
            encoded_size,
            !page.rows.is_empty(),
            &values,
            &page.next,
            &next,
        )
        .map_err(|_| ReceiverError::InvalidInput {
            field: "snapshot encoding".into(),
        })?;
        let previous_next = page.next.take();
        page.rows.push(values);
        page.next = next;

        if page.rows.len() > 1 && candidate_size > TARGET_UNIT_BYTES {
            page.rows.pop();
            page.next = previous_next;
            break;
        }
        if candidate_size > MAX_UNIT_BYTES {
            let key = key
                .into_iter()
                .map(|value| match value {
                    fleet_sync_protocol::SqlValue::Text(value) => Ok(value),
                    _ => Err(StoreError::Corrupt {
                        field: "snapshot row key".into(),
                    }),
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Err(UnitTooLarge::Row {
                table: table.clone(),
                key,
                encoded_size: candidate_size,
            }
            .into());
        }
        encoded_size = candidate_size;
        if candidate_size >= TARGET_UNIT_BYTES || page.rows.len() == PAGE_SIZE {
            break;
        }
        pending = following;
    }
    checked_page(page, encoded_size)
}

fn checked_page(page: SnapshotPage, expected: usize) -> Result<SnapshotRead, ReceiverError> {
    let measured = measure_page(page).map_err(|_| ReceiverError::InvalidInput {
        field: "snapshot encoding".into(),
    })?;
    if measured.encoded_size != expected {
        return Err(StoreError::Corrupt {
            field: "snapshot size changed".into(),
        }
        .into());
    }
    Ok(SnapshotRead::Page(Box::new(measured.unit)))
}

fn measure_page(
    page: SnapshotPage,
) -> Result<
    fleet_sync_protocol::unit_budget::MeasuredUnit<SnapshotPage>,
    fleet_sync_protocol::unit_budget::UnitEncodingError,
> {
    #[cfg(test)]
    FULL_MEASURES.with(|count| count.set(count.get() + 1));
    measure_snapshot_unit(page)
}
#[cfg(test)]
std::thread_local! { static FULL_MEASURES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "snapshot_sizing_oracle.rs"]
mod sizing_oracle;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "snapshot_sizing_tests.rs"]
mod sizing_tests;
