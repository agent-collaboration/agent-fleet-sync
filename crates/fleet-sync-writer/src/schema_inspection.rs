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
    let mut keys = BTreeMap::new();
    for row in rows {
        let index = unsigned(row.column_index, row.index_class, "column.index")?;
        if index != columns.len() {
            return Err(OpenError::corrupt("column.index"));
        }
        columns.push(text(row.column_name, row.name_class, "column.name")?);
        declared_types.push(text(row.declared_type, row.type_class, "column.type")?);
        let order = unsigned(row.key_order, row.key_class, "column.key_order")?;
        if order > 0 && keys.insert(order, index).is_some() {
            return Err(OpenError::corrupt("column.key_order"));
        }
    }
    if keys.is_empty() {
        return Err(OpenError::SchemaUnsupported {
            form: SchemaForm::NoPrimaryKey {
                table: table.clone(),
            },
        });
    }
    if keys.keys().copied().ne(1..=keys.len()) {
        return Err(OpenError::corrupt("column.key_order"));
    }
    let primary_key = keys.into_values().collect::<Vec<_>>();
    let integer_primary_key = if primary_key.len() == 1 {
        primary_key.first().copied().filter(|index| {
            declared_types
                .get(*index)
                .is_some_and(|name| name.eq_ignore_ascii_case("INTEGER"))
        })
    } else {
        None
    };
    TableLayout::new(
        table.as_str().into(),
        columns,
        primary_key,
        integer_primary_key,
    )
    .map_err(|_| OpenError::corrupt("schema.layout"))
}

pub(crate) async fn inspect_replicated_tables(
    connection: &mut TursoConnection,
    tables: &[TableName],
) -> Result<Vec<TableLayout>, OpenError> {
    let entries = schema_entries(connection).await?;
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
    Ok(layouts)
}
