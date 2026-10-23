// Pre-W-C40 copy pager retained only as a differential oracle.
use super::*;
pub(crate) async fn snapshot_page_oracle<'scope, S>(
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
    let measured = measure_snapshot_unit(page).map_err(|_| ReceiverError::InvalidInput {
        field: "snapshot encoding".into(),
    })?;
    if measured.encoded_size > MAX_UNIT_BYTES {
        return Err(UnitTooLarge::SnapshotSchema {
            encoded_size: measured.encoded_size,
        }
        .into());
    }
    let mut page = measured.unit;
    if table.is_empty() {
        return Ok(SnapshotRead::Page(Box::new(page)));
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
        let previous_next = page.next.take();
        page.rows.push(values);
        page.next = next;
        let measured = measure_snapshot_unit(page).map_err(|_| ReceiverError::InvalidInput {
            field: "snapshot encoding".into(),
        })?;
        page = measured.unit;
        if page.rows.len() > 1 && measured.encoded_size > TARGET_UNIT_BYTES {
            page.rows.pop();
            page.next = previous_next;
            break;
        }
        if measured.encoded_size > MAX_UNIT_BYTES {
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
                encoded_size: measured.encoded_size,
            }
            .into());
        }
        if measured.encoded_size >= TARGET_UNIT_BYTES || page.rows.len() == PAGE_SIZE {
            break;
        }
        pending = following;
    }
    Ok(SnapshotRead::Page(Box::new(page)))
}
