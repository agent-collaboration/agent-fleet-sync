use crate::{OpenError, SchemaForm, TableLayout, checked_queries, writer_scope::WriterScope};
use fleet_sync_protocol::{SchemaEntry, SchemaObjectKind, TableName, reserved_namespace};
use sqlx_turso::TursoConnection;
use std::collections::BTreeMap;

pub(crate) fn text(
    value: Option<String>,
    class: Option<String>,
    field: &str,
) -> Result<String, OpenError> {
    if class.as_deref() != Some("text") {
        return Err(OpenError::corrupt(field));
    }
    value.ok_or_else(|| OpenError::corrupt(field))
}

pub(crate) fn unsigned(
    value: Option<String>,
    class: Option<String>,
    field: &str,
) -> Result<usize, OpenError> {
    if class.as_deref() != Some("integer") {
        return Err(OpenError::corrupt(field));
    }
    value
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| OpenError::corrupt(field))
}

pub(crate) async fn schema_entries(
    connection: &mut TursoConnection,
) -> Result<Vec<(SchemaEntry, String)>, OpenError> {
    checked_queries::read_schema_entries(&mut WriterScope(connection))
        .await?
        .into_iter()
        .map(|row| {
            let kind = text(row.object_kind, row.kind_class, "schema.kind")?;
            Ok((
                SchemaEntry {
                    kind: SchemaObjectKind::try_from(kind.as_str())
                        .map_err(|_| OpenError::corrupt("schema.kind"))?,
                    name: text(row.object_name, row.name_class, "schema.name")?,
                    sql: text(row.statement, row.statement_class, "schema.sql")?,
                },
                text(row.table_name, row.table_class, "schema.table")?,
            ))
        })
        .collect()
}

pub(crate) async fn read_layout(
    connection: &mut TursoConnection,
    table: &TableName,
) -> Result<TableLayout, OpenError> {
    if reserved_namespace(table.as_str()) {
        return Err(OpenError::ReservedTableName {
            table: table.clone(),
        });
    }
    let rows =
        checked_queries::read_table_columns(&mut WriterScope(connection), table.as_str().into())
            .await?;
    if rows.is_empty() {
        return Err(OpenError::SchemaUnsupported {
            form: SchemaForm::MissingTable {
                table: table.clone(),
            },
        });
    }
    let mut columns = Vec::new();
    let mut declared_types = Vec::new();
    let mut not_null = Vec::new();
    let mut keys = BTreeMap::new();
    for row in rows {
        let index = unsigned(row.column_index, row.index_class, "column.index")?;
        if index != columns.len() {
            return Err(OpenError::corrupt("column.index"));
        }
        columns.push(text(row.column_name, row.name_class, "column.name")?);
        declared_types.push(text(row.declared_type, row.type_class, "column.type")?);
        let required = unsigned(row.not_null, row.not_null_class, "column.not_null")?;
        if required > 1 {
            return Err(OpenError::corrupt("column.not_null"));
        }
        not_null.push(required == 1);
        let order = unsigned(row.key_order, row.key_class, "column.key_order")?;
        if order > 0 && keys.insert(order, index).is_some() {
            return Err(OpenError::corrupt("column.key_order"));
        }
    }
    if keys.is_empty() {
        return Err(OpenError::SchemaUnsupported {
            form: SchemaForm::KeyNotUuidV7 {
                table: table.clone(),
                column: None,
            },
        });
    }
    if keys.keys().copied().ne(1..=keys.len()) {
        return Err(OpenError::corrupt("column.key_order"));
    }
    let primary_key = keys.into_values().collect::<Vec<_>>();
    let entries = schema_entries(connection).await?;
    let entry = entries
        .iter()
        .find(|(entry, _)| {
            entry.kind == SchemaObjectKind::Table && entry.name.eq_ignore_ascii_case(table.as_str())
        })
        .map(|(entry, _)| entry)
        .ok_or_else(|| OpenError::corrupt("schema.table_sql"))?;
    for index in &primary_key {
        if not_null.get(*index) != Some(&true)
            || declared_types
                .get(*index)
                .is_none_or(|kind| !kind.eq_ignore_ascii_case("TEXT"))
        {
            return Err(OpenError::SchemaUnsupported {
                form: SchemaForm::KeyNotUuidV7 {
                    table: table.clone(),
                    column: Some(
                        columns
                            .get(*index)
                            .cloned()
                            .ok_or_else(|| OpenError::corrupt("schema.key_column"))?,
                    ),
                },
            });
        }
    }
    TableLayout::new(entry.name.clone(), columns, primary_key)
        .map_err(|_| OpenError::corrupt("schema.layout"))
}

pub(crate) async fn inspect_replicated_tables(
    connection: &mut TursoConnection,
    tables: &[TableName],
) -> Result<Vec<TableLayout>, OpenError> {
    let entries = schema_entries(connection).await?;
    reject_invalid_key_schemas(&entries, tables)?;
    let mut layouts = Vec::new();
    for table in tables {
        for (entry, owner) in &entries {
            if !owner.eq_ignore_ascii_case(table.as_str()) {
                continue;
            }
            if entry.kind == SchemaObjectKind::Trigger {
                return Err(OpenError::SchemaUnsupported {
                    form: SchemaForm::Trigger {
                        name: entry.name.clone(),
                    },
                });
            }
            let mut parser = turso_parser::parser::Parser::new(entry.sql.as_bytes());
            if matches!(
                parser
                    .next_cmd()
                    .map_err(|_| OpenError::corrupt("schema.sql"))?,
                Some(turso_parser::ast::Cmd::Stmt(
                    turso_parser::ast::Stmt::CreateVirtualTable(_)
                ))
            ) {
                return Err(OpenError::SchemaUnsupported {
                    form: SchemaForm::VirtualTable {
                        table: table.clone(),
                    },
                });
            }
        }
        layouts.push(read_layout(connection, table).await?);
        crate::foreign_key_integrity::read_foreign_keys(connection, table).await?;
    }
    crate::native_rows::validate_stored_keys(connection, &layouts).await?;
    Ok(layouts)
}

pub(crate) fn reject_invalid_key_schemas(
    entries: &[(fleet_sync_protocol::SchemaEntry, String)],
    tables: &[TableName],
) -> Result<(), OpenError> {
    for (entry, _) in entries.iter().filter(|(entry, _)| {
        entry.kind == fleet_sync_protocol::SchemaObjectKind::Table
            && tables
                .iter()
                .any(|table| table.as_str().eq_ignore_ascii_case(&entry.name))
    }) {
        let command = turso_parser::parser::Parser::new(entry.sql.as_bytes())
            .next_cmd()
            .map_err(|_| OpenError::corrupt("schema.sql"))?
            .ok_or_else(|| OpenError::corrupt("schema.sql"))?;
        if let Err(error) = fleet_sync_protocol::replicated_key_columns(&command) {
            return Err(OpenError::SchemaUnsupported {
                form: SchemaForm::KeyNotUuidV7 {
                    table: TableName::new(entry.name.clone())
                        .map_err(|_| OpenError::corrupt("schema.name"))?,
                    column: error.column,
                },
            });
        }
    }
    Ok(())
}
