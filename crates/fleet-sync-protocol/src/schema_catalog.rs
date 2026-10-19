//! Pure migration ownership and selection of the current replicated catalog.
use crate::{SchemaEntry, SchemaObjectKind, TableName};
use std::collections::{BTreeMap, HashMap};
use turso_parser::{ast::Cmd, parser::Parser};

#[derive(Clone, Copy)]
pub enum MigrationKind {
    Replicated,
    Local,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MigrationError {
    #[error("invalid catalog field: {field}")]
    InvalidCatalog { field: String },
    #[error("invalid SQL: {message}")]
    InvalidSql { statement: String, message: String },
    #[error("replicated key column {column:?} of {table:?} must be TEXT NOT NULL")]
    KeyNotUuidV7 {
        table: TableName,
        column: Option<String>,
    },
    #[error("migration accesses undeclared table {table:?}")]
    UndeclaredTable { table: TableName },
    #[error("local migration changes replicated table {table:?}")]
    ReplicatedTableInLocalMigration { table: TableName },
    #[error("migration statement is not admitted: {statement}: {detail}")]
    InvalidMigration { statement: String, detail: String },
    #[error("migration statement calls nondeterministic function {function}: {statement}")]
    NonDeterministicMigration { statement: String, function: String },
    #[error("statement assigns the immutable key of {table}")]
    KeyChange { table: String },
}
impl MigrationError {
    pub(crate) fn invalid_catalog(field: impl Into<String>) -> Self {
        Self::InvalidCatalog {
            field: field.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MigrationStatement {
    pub sql: String,
    pub command: Cmd,
    pub inspection: crate::migration_inspection::MigrationInspection,
}

pub struct MigrationPlan {
    pub statements: Vec<String>,
    pub tables: Vec<TableName>,
}

/// Caches only parsed SQL; every plan rechecks the supplied current ownership.
#[derive(Debug, Default)]
pub struct MigrationClassifier {
    parsed: HashMap<String, Result<Vec<MigrationStatement>, MigrationError>>,
}
impl MigrationClassifier {
    pub fn plan(
        &mut self,
        sql: &str,
        current: &[TableName],
        declared: &[TableName],
        catalog: &[(SchemaEntry, String)],
        kind: MigrationKind,
    ) -> Result<MigrationPlan, MigrationError> {
        let mut views = BTreeMap::new();
        for (entry, _) in catalog
            .iter()
            .filter(|(entry, _)| entry.kind == SchemaObjectKind::View)
        {
            if let Some(statement) = self.statements(&entry.sql)?.first() {
                views.insert(
                    entry.name.to_ascii_lowercase(),
                    statement.inspection.clone(),
                );
            }
        }
        let statements = self.statements(sql)?;
        let tables = crate::migration_ownership::validate_ownership(
            statements, current, declared, catalog, views, kind,
        )?;
        Ok(MigrationPlan {
            statements: statements
                .iter()
                .map(|statement| statement.sql.clone())
                .collect(),
            tables,
        })
    }

    fn statements(&mut self, sql: &str) -> Result<&[MigrationStatement], MigrationError> {
        self.parsed
            .entry(sql.to_owned())
            .or_insert_with(|| parse(sql))
            .as_deref()
            .map_err(Clone::clone)
    }
}

fn parse(sql: &str) -> Result<Vec<MigrationStatement>, MigrationError> {
    let mut parser = Parser::new(sql.as_bytes());
    let mut statements = Vec::new();
    let mut start = 0;
    while let Some(command) = parser
        .next_cmd()
        .map_err(|error| MigrationError::InvalidSql {
            statement: sql.into(),
            message: error.to_string(),
        })?
    {
        let end = parser.offset();
        let statement = sql
            .get(start..end)
            .ok_or_else(|| MigrationError::InvalidSql {
                statement: sql.into(),
                message: "invalid parser offset".into(),
            })?;
        crate::admit_schema_command(&command).map_err(|error| {
            MigrationError::InvalidMigration {
                statement: statement.into(),
                detail: error.to_string(),
            }
        })?;
        statements.push(MigrationStatement {
            sql: statement.into(),
            inspection: crate::migration_inspection::inspect(&command),
            command,
        });
        start = end;
    }
    Ok(statements)
}

/// Select by physical owner, validate view dependencies, then order table/index/view.
pub fn current_replicated_catalog(
    catalog: &[(SchemaEntry, String)],
    tables: &[TableName],
) -> Vec<SchemaEntry> {
    let mut classifier = MigrationClassifier::default();
    let mut entries = catalog
        .iter()
        .filter(|(entry, owner)| {
            if crate::reserved_namespace(&entry.name) {
                return false;
            }
            match entry.kind {
                SchemaObjectKind::Table | SchemaObjectKind::Index => tables
                    .iter()
                    .any(|table| table.as_str().eq_ignore_ascii_case(owner)),
                SchemaObjectKind::View => classifier
                    .plan(
                        &entry.sql,
                        tables,
                        tables,
                        catalog,
                        MigrationKind::Replicated,
                    )
                    .is_ok(),
                SchemaObjectKind::Trigger => false,
            }
        })
        .map(|(entry, _)| entry.clone())
        .collect::<Vec<_>>();
    let rank = |kind| match kind {
        SchemaObjectKind::Table => 0,
        SchemaObjectKind::Index => 1,
        SchemaObjectKind::View => 2,
        SchemaObjectKind::Trigger => 3,
    };
    entries
        .sort_by(|left, right| (rank(left.kind), &left.name).cmp(&(rank(right.kind), &right.name)));
    entries
}

pub const MAX_SNAPSHOT_SCHEMA_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("current snapshot schema encodes to {encoded_size} bytes, exceeding 256 KiB")]
pub struct SnapshotSchemaTooLarge {
    pub encoded_size: usize,
}

/// Bound the repeated JSON array, including delimiters and escaped SQL strings.
pub fn snapshot_schema_statements(
    entries: Vec<SchemaEntry>,
) -> Result<Vec<String>, SnapshotSchemaTooLarge> {
    let statements = entries
        .into_iter()
        .map(|entry| entry.sql)
        .collect::<Vec<_>>();
    // Strings serialize infallibly. Count through the serializer without keeping
    // a second potentially large encoded copy of the catalog.
    let mut counter = EncodedBytes(0);
    if serde_json::to_writer(&mut counter, &statements).is_err()
        || counter.0 > MAX_SNAPSHOT_SCHEMA_BYTES
    {
        return Err(SnapshotSchemaTooLarge {
            encoded_size: counter.0,
        });
    }
    Ok(statements)
}
struct EncodedBytes(usize);
impl std::io::Write for EncodedBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;

    #[test]
    fn catalog_selection_follows_owners_and_transitive_view_dependencies()
    -> Result<(), Box<dyn std::error::Error>> {
        let catalog = [
            (
                SchemaObjectKind::View,
                "bad_view",
                "bad_view",
                "CREATE VIEW bad_view AS SELECT id FROM local_notes",
            ),
            (
                SchemaObjectKind::Table,
                "local_notes",
                "local_notes",
                "CREATE TABLE local_notes(id TEXT NOT NULL PRIMARY KEY)",
            ),
            (
                SchemaObjectKind::View,
                "second_view",
                "second_view",
                "CREATE VIEW second_view AS SELECT id FROM first_view",
            ),
            (
                SchemaObjectKind::Index,
                "records_named_private",
                "local_notes",
                "CREATE INDEX records_named_private ON local_notes(id)",
            ),
            (
                SchemaObjectKind::View,
                "first_view",
                "first_view",
                "CREATE VIEW first_view AS SELECT id FROM records",
            ),
            (
                SchemaObjectKind::Trigger,
                "records_trigger",
                "records",
                "CREATE TRIGGER records_trigger AFTER INSERT ON records BEGIN SELECT 1; END",
            ),
            (
                SchemaObjectKind::Index,
                "local_named_replicated",
                "records",
                "CREATE INDEX local_named_replicated ON records(id)",
            ),
            (
                SchemaObjectKind::Table,
                "records",
                "records",
                "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY)",
            ),
            (
                SchemaObjectKind::View,
                "cycle",
                "cycle",
                "CREATE VIEW cycle AS SELECT id FROM cycle",
            ),
            (
                SchemaObjectKind::Index,
                "_fleet_sync_hidden",
                "records",
                "CREATE INDEX _fleet_sync_hidden ON records(id)",
            ),
        ]
        .into_iter()
        .map(|(kind, name, owner, sql)| {
            (
                SchemaEntry {
                    kind,
                    name: name.into(),
                    sql: sql.into(),
                },
                owner.into(),
            )
        })
        .collect::<Vec<_>>();
        let selected = current_replicated_catalog(&catalog, &[TableName::new("RECORDS")?]);
        assert_eq!(
            selected
                .into_iter()
                .map(|entry| (entry.kind, entry.name))
                .collect::<Vec<_>>(),
            vec![
                (SchemaObjectKind::Table, "records".into()),
                (SchemaObjectKind::Index, "local_named_replicated".into()),
                (SchemaObjectKind::View, "first_view".into()),
                (SchemaObjectKind::View, "second_view".into()),
            ]
        );
        Ok(())
    }

    #[test]
    fn aggregate_bound_counts_json_escaping_and_includes_the_exact_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let entry = |sql: String| SchemaEntry {
            kind: SchemaObjectKind::Table,
            name: "records".into(),
            sql,
        };
        let at_limit = entry("x".repeat(MAX_SNAPSHOT_SCHEMA_BYTES - 4));
        assert_eq!(
            serde_json::to_vec(&vec![at_limit.sql.clone()])?.len(),
            MAX_SNAPSHOT_SCHEMA_BYTES
        );
        snapshot_schema_statements(vec![at_limit])?;
        let escaped = vec![entry("\"".repeat(MAX_SNAPSHOT_SCHEMA_BYTES / 2))];
        let expected =
            serde_json::to_vec(&escaped.iter().map(|entry| &entry.sql).collect::<Vec<_>>())?.len();
        assert_eq!(expected, MAX_SNAPSHOT_SCHEMA_BYTES + 4);
        assert_eq!(
            snapshot_schema_statements(escaped),
            Err(SnapshotSchemaTooLarge {
                encoded_size: expected
            })
        );
        let aggregate = vec![entry("x".repeat(150 * 1024)), entry("y".repeat(150 * 1024))];
        assert!(
            matches!(snapshot_schema_statements(aggregate), Err(SnapshotSchemaTooLarge { encoded_size }) if encoded_size > MAX_SNAPSHOT_SCHEMA_BYTES)
        );
        Ok(())
    }
}
