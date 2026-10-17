use crate::{
    checked_native_queries,
    native_carry_rows::copy_rows,
    native_carry_schema::{
        FileScope, SchemaObject, invalid, native_engine_object, quoted, schema, text,
    },
};
use sqlx_core::{
    executor::Executor,
    sql_str::{AssertSqlSafe, SqlSafeStr},
    transaction::Transaction,
};
use sqlx_turso::{Turso, TursoConnection};
use std::collections::BTreeSet;

/// The file's domain owner supplies its table/view membership and replacement
/// namespace. The file mechanics do not interpret replication state or JSON.
pub struct LocalStateSelection {
    pub replicated_before: BTreeSet<String>,
    pub replicated_after: BTreeSet<String>,
    pub replaced_tables: BTreeSet<String>,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LocalStateCarried {
    pub copied_tables: u64,
    pub copied_rows: u64,
    pub preserved_sequences: u64,
}
#[derive(Debug, thiserror::Error)]
pub enum LocalStateCarryError {
    #[error(transparent)]
    Storage(#[from] sqlx_core::Error),
    #[error("invalid native schema metadata: {field}")]
    InvalidMetadata { field: String },
    #[error("local object {name} conflicts with the snapshot schema")]
    LocalObjectConflict { name: String },
}
fn contains(names: &BTreeSet<String>, name: &str) -> bool {
    names.iter().any(|entry| entry.eq_ignore_ascii_case(name))
}
fn protected(
    object: &SchemaObject,
    replicated: &BTreeSet<String>,
    replaced: &BTreeSet<String>,
) -> bool {
    native_engine_object(&object.object_name)
        || contains(replicated, &object.object_name)
        || contains(replicated, &object.owner_table)
        || contains(replaced, &object.object_name)
        || contains(replaced, &object.owner_table)
}
/// The owner has drained the source. All destination writes share the caller's
/// staging transaction with its typed fence merge. A repeated carry replaces
/// earlier local state, including tables that the live target since dropped.
pub async fn carry_local_state(
    source: &mut TursoConnection,
    staging: &mut Transaction<'_, Turso>,
    selection: &LocalStateSelection,
) -> Result<LocalStateCarried, LocalStateCarryError> {
    let source_schema = schema(source).await?;
    let staged_schema = schema(staging).await?;
    let local_tables = source_schema
        .iter()
        .filter(|object| {
            object.object_kind == "table"
                && !protected(
                    object,
                    &selection.replicated_before,
                    &selection.replaced_tables,
                )
        })
        .collect::<Vec<_>>();
    let local_names = local_tables
        .iter()
        .map(|object| object.object_name.clone())
        .collect::<BTreeSet<_>>();
    let associated = source_schema
        .iter()
        .filter(|object| {
            matches!(object.object_kind.as_str(), "index" | "trigger")
                && contains(&local_names, &object.owner_table)
        })
        .collect::<Vec<_>>();
    // Check every object before dropping any previous carry. A new replicated
    // table/index may not consume a target-owned local name.
    for object in local_tables
        .iter()
        .copied()
        .chain(associated.iter().copied())
    {
        if staged_schema.iter().any(|staged| {
            staged.object_name.eq_ignore_ascii_case(&object.object_name)
                && protected(
                    staged,
                    &selection.replicated_after,
                    &selection.replaced_tables,
                )
        }) {
            return Err(LocalStateCarryError::LocalObjectConflict {
                name: object.object_name.clone(),
            });
        }
    }
    for kind in ["view", "table"] {
        for object in staged_schema.iter().filter(|object| {
            object.object_kind == kind
                && !protected(
                    object,
                    &selection.replicated_after,
                    &selection.replaced_tables,
                )
        }) {
            let statement = format!(
                "DROP {} {}",
                if kind == "view" { "VIEW" } else { "TABLE" },
                quoted(&object.object_name)
            );
            (&mut **staging)
                .execute(AssertSqlSafe(statement).into_sql_str())
                .await?;
        }
    }
    let mut carried = LocalStateCarried::default();
    for table in local_tables {
        (&mut **staging)
            .execute(AssertSqlSafe(table.create_sql.clone()).into_sql_str())
            .await?;
        carried.copied_rows = carried
            .copied_rows
            .checked_add(copy_rows(source, staging, table).await?)
            .ok_or_else(|| invalid("carry.total_rows"))?;
        carried.copied_tables = carried
            .copied_tables
            .checked_add(1)
            .ok_or_else(|| invalid("carry.table_count"))?;
    }
    // Create indexes/triggers after inserting data: local triggers must not run
    // while their already-committed rows are being carried.
    for object in associated {
        (&mut **staging)
            .execute(AssertSqlSafe(object.create_sql.clone()).into_sql_str())
            .await?;
    }
    if source_schema
        .iter()
        .any(|object| object.object_name == "sqlite_sequence")
    {
        let sequences =
            checked_native_queries::read_local_sequences(&mut FileScope(source)).await?;
        for row in sequences {
            let table = text(row.table_name, row.name_class, "carry.sequence_table")?;
            if !contains(&local_names, &table) {
                continue;
            }
            if row.sequence_class.as_deref() != Some("integer") {
                return Err(invalid("carry.sequence_class"));
            }
            let sequence = row
                .sequence_value
                .and_then(|value| value.parse::<i64>().ok())
                .ok_or_else(|| invalid("carry.sequence_value"))?;
            let mut scope = FileScope(staging);
            checked_native_queries::erase_local_sequence(&mut scope, table.clone()).await?;
            checked_native_queries::write_local_sequence(&mut scope, table, sequence).await?;
            carried.preserved_sequences = carried
                .preserved_sequences
                .checked_add(1)
                .ok_or_else(|| invalid("carry.sequence_count"))?;
        }
    }
    Ok(carried)
}
