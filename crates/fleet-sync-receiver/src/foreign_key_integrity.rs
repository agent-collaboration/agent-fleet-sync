//! Final relational integrity across replay order, on both SQL backends.
use crate::{
    BoundStatement, ReceiverError, StoreError,
    bound_sql::quoted_identifier,
    checked_queries,
    row_apply::primary_columns,
    schema_reads::{SchemaReads, required_text},
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::TableName;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::collections::BTreeMap;

pub(crate) struct IntegrityViolation {
    pub table: TableName,
    pub references: TableName,
}
struct ForeignKeyGroup {
    parent: TableName,
    columns: BTreeMap<usize, (String, Option<String>)>,
}
pub(crate) struct ForeignKeyIntegrity<'scope, S>(pub &'scope mut S);
fn corrupt() -> StoreError {
    StoreError::Corrupt {
        field: "replicated foreign key metadata".into(),
    }
}
fn ordinal(value: Option<String>, class: Option<String>) -> Result<usize, StoreError> {
    if class.as_deref() != Some("integer") {
        return Err(corrupt());
    }
    value
        .and_then(|value| value.parse().ok())
        .ok_or_else(corrupt)
}
fn parent_column(
    value: Option<String>,
    class: Option<String>,
) -> Result<Option<String>, StoreError> {
    match (value, class.as_deref()) {
        (None, Some("null")) => Ok(None),
        (Some(value), Some("text")) if value.is_empty() => Ok(None),
        (Some(value), Some("text")) => Ok(Some(value)),
        _ => Err(corrupt()),
    }
}
impl<S> ForeignKeyIntegrity<'_, S>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'a> String: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Option<String>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> f64: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'a> Vec<u8>: Type<S::Database> + Encode<'a, S::Database> + Decode<'a, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub(crate) async fn defer(&mut self) -> Result<(), ReceiverError> {
        // Do is the owned SQLx driver's Database::NAME. Turso 0.8.1 does not
        // implement this pragma; its integrity comes from validate below.
        if S::Database::NAME == "Do" {
            BoundStatement::new("PRAGMA defer_foreign_keys=ON", Vec::new())
                .execute(self.0)
                .await?;
        }
        Ok(())
    }
    pub(crate) async fn restore(&mut self) -> Result<(), ReceiverError> {
        if S::Database::NAME == "Do" {
            BoundStatement::new("PRAGMA defer_foreign_keys=OFF", Vec::new())
                .execute(self.0)
                .await?;
        }
        Ok(())
    }
    pub(crate) async fn validate(&mut self) -> Result<Option<IntegrityViolation>, ReceiverError> {
        let tables = SchemaReads(self.0).table_names().await?;
        for table in &tables {
            let child_columns = SchemaReads(self.0).table_columns(table).await?;
            let rows = checked_queries::read_foreign_keys(self.0, table.clone())
                .await
                .map_err(crate::sql_transport::sql_error)?;
            let mut groups = BTreeMap::<usize, ForeignKeyGroup>::new();
            for row in rows {
                let id = ordinal(row.foreign_key_id, row.id_class)?;
                let order = ordinal(row.column_order, row.order_class)?;
                let parent =
                    required_text(row.parent_table, row.table_class, "foreign key parent")?;
                if !tables.iter().any(|name| name.eq_ignore_ascii_case(&parent)) {
                    return Err(corrupt().into());
                }
                let parent = TableName::new(parent).map_err(|_| corrupt())?;
                let child = required_text(
                    row.child_column,
                    row.child_class,
                    "foreign key child column",
                )?;
                if !child_columns
                    .iter()
                    .any(|column| column.name.eq_ignore_ascii_case(&child))
                {
                    return Err(corrupt().into());
                }
                let referenced = parent_column(row.parent_column, row.parent_class)?;
                let update = required_text(
                    row.update_action,
                    row.update_class,
                    "foreign key update action",
                )?;
                let delete = required_text(
                    row.delete_action,
                    row.delete_class,
                    "foreign key delete action",
                )?;
                if ![update.as_str(), delete.as_str()]
                    .into_iter()
                    .all(|action| matches!(action, "NO ACTION" | "RESTRICT"))
                {
                    return Err(corrupt().into());
                }
                let group = groups.entry(id).or_insert_with(|| ForeignKeyGroup {
                    parent: parent.clone(),
                    columns: BTreeMap::new(),
                });
                if group.parent != parent
                    || group.columns.insert(order, (child, referenced)).is_some()
                {
                    return Err(corrupt().into());
                }
            }
            for ForeignKeyGroup { parent, columns } in groups.into_values() {
                if columns.keys().copied().ne(0..columns.len()) {
                    return Err(corrupt().into());
                }
                let mut columns = columns.into_values().collect::<Vec<_>>();
                let layout = SchemaReads(self.0).table_columns(parent.as_str()).await?;
                if columns.iter().all(|(_, parent)| parent.is_none()) {
                    let keys = primary_columns(&layout);
                    if keys.len() != columns.len() {
                        return Err(corrupt().into());
                    }
                    for ((_, name), key) in columns.iter_mut().zip(keys) {
                        *name = Some(key.name.clone());
                    }
                }
                let mut matching = Vec::new();
                let mut non_null = Vec::new();
                for (child, referenced) in columns {
                    let referenced = referenced.ok_or_else(corrupt)?;
                    if !layout
                        .iter()
                        .any(|column| column.name.eq_ignore_ascii_case(&referenced))
                    {
                        return Err(corrupt().into());
                    }
                    matching.push(format!(
                        "parent.{}=+child.{}",
                        quoted_identifier(&referenced),
                        quoted_identifier(&child)
                    ));
                    non_null.push(format!("child.{} IS NOT NULL", quoted_identifier(&child)));
                }
                if matching.is_empty() {
                    return Err(corrupt().into());
                }
                // Identifiers are validated schema metadata and quoted. Parent
                // columns stay on the left for their collation; unary + removes
                // the child's declared affinity without changing its value. This
                // makes SQLite apply only the parent's affinity, as FK equality
                // requires (foreignkeys.html#fk_basics, datatype3.html sections
                // "Affinity Of Expressions" and "Type Conversions Prior To
                // Comparison"). CAST is unsuitable: it can be lossy whereas
                // affinity leaves nonnumeric text unchanged.
                let query = format!(
                    "SELECT 1 AS violation FROM {} AS child WHERE {} AND NOT EXISTS (SELECT 1 FROM {} AS parent WHERE {}) LIMIT 1",
                    quoted_identifier(table),
                    non_null.join(" AND "),
                    quoted_identifier(parent.as_str()),
                    matching.join(" AND ")
                );
                if !BoundStatement::query(query, Vec::new(), vec!["violation".into()])
                    .fetch_rows(self.0)
                    .await?
                    .is_empty()
                {
                    return Ok(Some(IntegrityViolation {
                        table: TableName::new(table).map_err(|_| corrupt())?,
                        references: parent,
                    }));
                }
            }
        }
        Ok(None)
    }
}
