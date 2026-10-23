use crate::{
    OpenError, SchemaForm, checked_queries,
    schema_inspection::{read_layout, text, unsigned},
    writer_scope::WriterScope,
};
use fleet_sync_protocol::TableName;
use sqlx_core::{
    query::query,
    sql_str::{AssertSqlSafe, SqlSafeStr},
};
use sqlx_turso::{Turso, TursoConnection};
use std::collections::BTreeMap;

struct ForeignKeyGroup {
    parent: TableName,
    columns: BTreeMap<usize, (String, String)>,
}

pub(crate) struct ForeignKey {
    parent: TableName,
    columns: Vec<(String, String)>,
}

fn metadata_error(table: &TableName, detail: impl Into<String>) -> OpenError {
    OpenError::SchemaUnsupported {
        form: SchemaForm::ForeignKeyMetadata {
            table: table.clone(),
            detail: detail.into(),
        },
    }
}

pub(crate) async fn read_foreign_keys(
    connection: &mut TursoConnection,
    table: &TableName,
) -> Result<Vec<ForeignKey>, OpenError> {
    let rows =
        checked_queries::read_foreign_keys(&mut WriterScope(connection), table.as_str().into())
            .await?;
    let mut grouped = BTreeMap::<usize, ForeignKeyGroup>::new();
    for row in rows {
        let id = unsigned(row.foreign_key_id, row.id_class, "foreign_key.id")?;
        let order = unsigned(row.column_order, row.order_class, "foreign_key.order")?;
        let parent = TableName::new(text(
            row.parent_table,
            row.table_class,
            "foreign_key.parent",
        )?)
        .map_err(|_| metadata_error(table, "empty parent"))?;
        let child_column = text(
            row.child_column,
            row.child_class,
            "foreign_key.child_column",
        )?;
        let parent_column = text(
            row.parent_column,
            row.parent_class,
            "foreign_key.parent_column",
        )?;
        let update = text(
            row.update_action,
            row.update_class,
            "foreign_key.update_action",
        )?;
        let delete = text(
            row.delete_action,
            row.delete_class,
            "foreign_key.delete_action",
        )?;
        if ![update.as_str(), delete.as_str()]
            .into_iter()
            .all(|action| matches!(action, "NO ACTION" | "RESTRICT"))
        {
            return Err(OpenError::SchemaUnsupported {
                form: SchemaForm::ForeignKeyAction {
                    table: table.clone(),
                    constraint: format!("{id}: ON UPDATE {update}, ON DELETE {delete}"),
                },
            });
        }
        let group = grouped.entry(id).or_insert_with(|| ForeignKeyGroup {
            parent: parent.clone(),
            columns: BTreeMap::new(),
        });
        if group.parent != parent
            || group
                .columns
                .insert(order, (child_column, parent_column))
                .is_some()
        {
            return Err(metadata_error(table, "inconsistent foreign key columns"));
        }
    }
    let mut foreign_keys = Vec::new();
    for ForeignKeyGroup { parent, columns } in grouped.into_values() {
        if columns.keys().copied().ne(0..columns.len()) {
            return Err(metadata_error(table, "noncontiguous column order"));
        }
        let mut columns = columns.into_values().collect::<Vec<_>>();
        if columns
            .iter()
            .any(|(_, parent_column)| parent_column.is_empty())
        {
            if !columns
                .iter()
                .all(|(_, parent_column)| parent_column.is_empty())
            {
                return Err(metadata_error(table, "partially implicit parent key"));
            }
            let parent_layout = read_layout(connection, &parent).await.map_err(|_| {
                metadata_error(table, "implicit parent has no declared primary key")
            })?;
            if parent_layout.primary_key().len() != columns.len() {
                return Err(metadata_error(
                    table,
                    "implicit parent key column count differs",
                ));
            }
            for ((_, parent_column), index) in columns.iter_mut().zip(parent_layout.primary_key()) {
                *parent_column = parent_layout
                    .columns()
                    .get(*index)
                    .ok_or_else(|| metadata_error(table, "parent key index"))?
                    .clone();
            }
        }
        foreign_keys.push(ForeignKey { parent, columns });
    }
    Ok(foreign_keys)
}

pub(crate) fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn orphan_statement(table: &TableName, foreign_key: &ForeignKey) -> Result<String, OpenError> {
    let non_null = foreign_key
        .columns
        .iter()
        .map(|(child, _)| format!("child.{} IS NOT NULL", quote_identifier(child)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let matching = foreign_key
        .columns
        .iter()
        .map(|(child, parent)| {
            format!(
                "parent.{}=child.{}",
                quote_identifier(parent),
                quote_identifier(child)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let (_, first_parent_column) = foreign_key
        .columns
        .first()
        .ok_or_else(|| metadata_error(table, "empty foreign key"))?;
    // Every checked child key is non-null. A matched parent's first key
    // therefore cannot be null; only the join's unmatched row passes this guard.
    Ok(format!(
        "SELECT 1 FROM {} AS child LEFT JOIN {} AS parent ON {matching} WHERE {non_null} AND parent.{} IS NULL LIMIT 1",
        quote_identifier(table.as_str()),
        quote_identifier(foreign_key.parent.as_str()),
        quote_identifier(first_parent_column)
    ))
}

pub(crate) async fn reject_orphans(
    connection: &mut TursoConnection,
    tables: &[TableName],
) -> Result<(), OpenError> {
    for table in tables {
        for foreign_key in read_foreign_keys(connection, table).await? {
            let statement = orphan_statement(table, &foreign_key)?;
            // Identifiers come from checked schema metadata and are quoted. The
            // number and names of these columns are consumer-defined, so this
            // schema-bound existence query cannot be a fixed generated statement.
            if query::<Turso>(AssertSqlSafe(statement).into_sql_str())
                .fetch_optional(&mut *connection)
                .await?
                .is_some()
            {
                return Err(OpenError::SchemaUnsupported {
                    form: SchemaForm::ForeignKeyOrphan {
                        table: table.clone(),
                        parent: foreign_key.parent,
                    },
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "foreign_key_integrity_tests.rs"]
mod tests;
