use crate::{
    LocalStateCarryError,
    native_carry_schema::{SchemaObject, columns, invalid, quoted},
};
use fleet_sync_protocol::SqlValue;
use futures_util::TryStreamExt;
use sqlx_core::{
    query::query,
    row::Row,
    sql_str::{AssertSqlSafe, SqlSafeStr},
    transaction::Transaction,
};
use sqlx_turso::{Turso, TursoConnection};

/// Both shapes come from inspected native schema. These are variable column
/// lists, not fixed application SQL; quote identifiers and bind every value.
pub(crate) async fn copy_rows(
    source: &mut TursoConnection,
    target: &mut Transaction<'_, Turso>,
    table: &SchemaObject,
) -> Result<u64, LocalStateCarryError> {
    let columns = columns(source, table).await?;
    let projection = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let name = quoted(column);
            format!("{name} AS _fleet_value_{index},typeof({name}) AS _fleet_class_{index}")
        })
        .collect::<Vec<_>>()
        .join(",");
    let select = format!("SELECT {projection} FROM {}", quoted(&table.object_name));
    let insert = format!(
        "INSERT INTO {}({}) VALUES({})",
        quoted(&table.object_name),
        columns
            .iter()
            .map(|name| quoted(name))
            .collect::<Vec<_>>()
            .join(","),
        columns.iter().map(|_| "?").collect::<Vec<_>>().join(",")
    );
    let mut rows = query::<Turso>(AssertSqlSafe(select).into_sql_str()).fetch(source);
    let mut count = 0_u64;
    while let Some(row) = rows.try_next().await? {
        let mut values = Vec::with_capacity(columns.len());
        for index in 0..columns.len() {
            let alias = format!("_fleet_value_{index}");
            let class = row.try_get::<String, _>(format!("_fleet_class_{index}").as_str())?;
            values.push(match class.as_str() {
                "null" => SqlValue::Null,
                "integer" => SqlValue::Integer(row.try_get_unchecked::<i64, _>(alias.as_str())?),
                "real" => {
                    let value = row.try_get_unchecked::<f64, _>(alias.as_str())?;
                    if value.is_nan() {
                        return Err(invalid("carry.nan_real"));
                    }
                    SqlValue::Real(value)
                }
                "text" => SqlValue::Text(row.try_get_unchecked::<String, _>(alias.as_str())?),
                "blob" => SqlValue::Blob(row.try_get_unchecked::<Vec<u8>, _>(alias.as_str())?),
                _ => return Err(invalid("carry.storage_class")),
            });
        }
        let mut statement = query::<Turso>(AssertSqlSafe(insert.clone()).into_sql_str());
        for value in values {
            statement = match value {
                SqlValue::Null => statement.bind(None::<String>),
                SqlValue::Integer(value) => statement.bind(value),
                SqlValue::Real(value) => statement.bind(value),
                SqlValue::Text(value) => statement.bind(value),
                SqlValue::Blob(value) => statement.bind(value),
            };
        }
        statement.execute(&mut **target).await?;
        count = count
            .checked_add(1)
            .ok_or_else(|| invalid("carry.row_count"))?;
    }
    Ok(count)
}
