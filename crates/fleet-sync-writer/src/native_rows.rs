use crate::{OpenError, TableLayout, foreign_key_integrity::quote_identifier};
use fleet_sync_protocol::SqlValue;
use sqlx_core::{
    query::query,
    row::Row,
    sql_str::{AssertSqlSafe, SqlSafeStr},
};
use sqlx_turso::{Turso, TursoConnection};

/// Consumer columns and keys are schema-bound, so this query cannot have a
/// fixed checked-query shape. Quote only inspected identifiers, bind values,
/// and read named aliases with an explicit native storage-class projection.
pub(crate) async fn read_rows(
    connection: &mut TursoConnection,
    table: &TableLayout,
    after: &[SqlValue],
    limit: usize,
) -> Result<Vec<Vec<SqlValue>>, OpenError> {
    if !after.is_empty()
        && (after.len() != table.primary_key().len()
            || after.iter().any(|value| matches!(value, SqlValue::Null)))
    {
        return Err(OpenError::corrupt("snapshot.cursor_key"));
    }
    let projection = table
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let column = quote_identifier(column);
            format!("{column} AS _fleet_value_{index},typeof({column}) AS _fleet_class_{index}")
        })
        .collect::<Vec<_>>()
        .join(",");
    let keys = table
        .primary_key()
        .iter()
        .map(|index| {
            table
                .columns()
                .get(*index)
                .map(|name| quote_identifier(name))
                .ok_or_else(|| OpenError::corrupt("snapshot.key_columns"))
        })
        .collect::<Result<Vec<_>, _>>()?
        .join(",");
    let predicate = if after.is_empty() {
        String::new()
    } else {
        format!(
            " WHERE ({keys}) > ({})",
            after.iter().map(|_| "?").collect::<Vec<_>>().join(",")
        )
    };
    let sql = format!(
        "SELECT {projection} FROM {}{predicate} ORDER BY {keys} LIMIT {limit}",
        quote_identifier(table.name())
    );
    let mut statement = query::<Turso>(AssertSqlSafe(sql).into_sql_str());
    for value in after {
        statement = match value {
            SqlValue::Null => statement.bind(None::<String>),
            SqlValue::Integer(value) => statement.bind(*value),
            SqlValue::Real(value) => statement.bind(*value),
            SqlValue::Text(value) => statement.bind(value),
            SqlValue::Blob(value) => statement.bind(value),
        };
    }
    let rows = statement.fetch_all(connection).await?;
    let mut decoded = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values = Vec::with_capacity(table.columns().len());
        for index in 0..table.columns().len() {
            let alias = format!("_fleet_value_{index}");
            let class = row.try_get::<String, _>(format!("_fleet_class_{index}").as_str())?;
            let value = match class.as_str() {
                "null" => SqlValue::Null,
                "integer" => SqlValue::Integer(row.try_get_unchecked::<i64, _>(alias.as_str())?),
                "real" => {
                    let value = row.try_get_unchecked::<f64, _>(alias.as_str())?;
                    if value.is_nan() {
                        return Err(OpenError::corrupt("snapshot.nan_real"));
                    }
                    SqlValue::Real(value)
                }
                "text" => SqlValue::Text(row.try_get_unchecked::<String, _>(alias.as_str())?),
                "blob" => SqlValue::Blob(row.try_get_unchecked::<Vec<u8>, _>(alias.as_str())?),
                _ => return Err(OpenError::corrupt("snapshot.storage_class")),
            };
            values.push(value);
        }
        decoded.push(values);
    }
    Ok(decoded)
}

pub(crate) fn row_key(table: &TableLayout, row: &[SqlValue]) -> Result<Vec<SqlValue>, OpenError> {
    table
        .primary_key()
        .iter()
        .map(|index| {
            row.get(*index)
                .cloned()
                .ok_or_else(|| OpenError::corrupt("snapshot.row_key"))
        })
        .collect()
}
