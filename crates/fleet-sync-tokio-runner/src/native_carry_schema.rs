use crate::{LocalStateCarryError, checked_native_queries};
use durable_steps::storage::TransactionScope;
use sqlx_turso::{Turso, TursoConnection};

pub(crate) struct FileScope<'a>(pub &'a mut TursoConnection);
impl TransactionScope for FileScope<'_> {
    type Database = Turso;
    type Executor<'a>
        = &'a mut TursoConnection
    where
        Self: 'a;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}
pub(crate) struct SchemaObject {
    pub object_kind: String,
    pub object_name: String,
    pub owner_table: String,
    pub create_sql: String,
}
pub(crate) fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('\"', "\"\""))
}
pub(crate) fn text(
    value: Option<String>,
    class: Option<String>,
    field: &str,
) -> Result<String, LocalStateCarryError> {
    if class.as_deref() != Some("text") {
        return Err(invalid(field));
    }
    value.ok_or_else(|| invalid(field))
}
pub(crate) fn invalid(field: &str) -> LocalStateCarryError {
    LocalStateCarryError::InvalidMetadata {
        field: field.into(),
    }
}
pub(crate) fn native_engine_object(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.starts_with("sqlite_")
        || name.starts_with("turso_")
        || name.starts_with(turso::core::schema::TURSO_INTERNAL_PREFIX)
}
pub(crate) async fn schema(
    connection: &mut TursoConnection,
) -> Result<Vec<SchemaObject>, LocalStateCarryError> {
    checked_native_queries::read_file_schema(&mut FileScope(connection))
        .await?
        .into_iter()
        .map(|row| {
            Ok(SchemaObject {
                object_kind: text(row.object_kind, row.kind_class, "carry.object_kind")?,
                object_name: text(row.object_name, row.name_class, "carry.object_name")?,
                owner_table: text(row.owner_table, row.owner_class, "carry.owner_table")?,
                create_sql: text(row.create_sql, row.sql_class, "carry.create_sql")?,
            })
        })
        .collect()
}
pub(crate) async fn columns(
    connection: &mut TursoConnection,
    table: &SchemaObject,
) -> Result<Vec<String>, LocalStateCarryError> {
    let mut columns = Vec::new();
    for row in checked_native_queries::read_file_columns(
        &mut FileScope(connection),
        table.object_name.clone(),
    )
    .await?
    {
        if row.index_class.as_deref() != Some("integer")
            || row
                .column_index
                .and_then(|value| value.parse::<usize>().ok())
                != Some(columns.len())
        {
            return Err(invalid("carry.column_index"));
        }
        columns.push(text(row.column_name, row.name_class, "carry.column_name")?);
    }
    if columns.is_empty() {
        return Err(invalid("carry.empty_columns"));
    }
    let physical = turso::core::schema::BTreeTable::from_sql(&table.create_sql, 0)
        .map_err(|_| invalid("carry.table_definition"))?;
    // Decision46: when all aliases are user columns, copy their content.
    // Hidden rowids have no stable identity without INTEGER PRIMARY KEY:
    // https://www.sqlite.org/lang_vacuum.html (VACUUM may renumber them).
    if physical.has_rowid
        && physical.get_rowid_alias_column().is_none()
        && let Some(alias) = ["rowid", "_rowid_", "oid"].into_iter().find(|alias| {
            !columns
                .iter()
                .any(|column| column.eq_ignore_ascii_case(alias))
        })
    {
        columns.insert(0, alias.into());
    }
    Ok(columns)
}
