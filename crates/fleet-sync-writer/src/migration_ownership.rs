use crate::{OpenError, SchemaForm, statement_guard::MigrationStatement};
use fleet_sync_protocol::{SchemaEntry, SchemaObjectKind, TableName};
use std::collections::{BTreeMap, BTreeSet};
use turso_parser::ast::{AlterTableBody, Cmd, QualifiedName, Stmt};

#[derive(Clone, Copy)]
pub(crate) enum MigrationKind {
    Replicated,
    Local,
}

struct TableOwnership {
    declared: BTreeSet<String>,
    objects: BTreeMap<String, (SchemaObjectKind, String)>,
    transient: BTreeSet<String>,
    views: BTreeMap<String, crate::migration_inspection::MigrationInspection>,
    kind: MigrationKind,
    key_columns: BTreeMap<String, Vec<String>>,
}

fn table_error(name: &str, local: bool) -> OpenError {
    let Ok(table) = TableName::new(name) else {
        return OpenError::corrupt("migration.table");
    };
    OpenError::SchemaUnsupported {
        form: if local {
            SchemaForm::ReplicatedTableInLocalMigration { table }
        } else {
            SchemaForm::UndeclaredTable { table }
        },
    }
}

fn invalid(statement: &MigrationStatement, detail: &str) -> OpenError {
    OpenError::SchemaUnsupported {
        form: SchemaForm::InvalidMigration {
            statement: statement.sql.clone(),
            detail: detail.into(),
        },
    }
}

impl TableOwnership {
    fn access(&self, schema: Option<&str>, name: &str, write: bool) -> Result<(), OpenError> {
        if schema.is_some_and(|schema| !schema.eq_ignore_ascii_case("main")) {
            return Err(table_error(name, false));
        }
        let name_key = name.to_ascii_lowercase();
        match self.kind {
            MigrationKind::Replicated => {
                let mut pending = vec![(name_key, false)];
                let mut ancestors = BTreeSet::new();
                let mut complete = BTreeSet::new();
                while let Some((name, leaving)) = pending.pop() {
                    if leaving {
                        ancestors.remove(&name);
                        complete.insert(name);
                        continue;
                    }
                    if self.declared.contains(&name)
                        || self.transient.contains(&name)
                        || complete.contains(&name)
                    {
                        continue;
                    }
                    if !ancestors.insert(name.clone()) {
                        return Err(table_error(&name, false));
                    }
                    let Some(view) = self.views.get(&name) else {
                        return Err(table_error(&name, false));
                    };
                    pending.push((name, true));
                    for (schema, source) in &view.tables {
                        if schema
                            .as_ref()
                            .is_some_and(|schema| !schema.eq_ignore_ascii_case("main"))
                        {
                            return Err(table_error(source, false));
                        }
                        pending.push((source.to_ascii_lowercase(), false));
                    }
                }
            }
            MigrationKind::Local => {
                if write && self.declared.contains(&name_key) {
                    return Err(table_error(name, true));
                }
            }
        }
        Ok(())
    }

    fn target(&self, name: &QualifiedName) -> Result<(), OpenError> {
        self.access(
            name.db_name.as_ref().map(|schema| schema.as_str()),
            name.name.as_str(),
            true,
        )
    }

    fn create(&mut self, name: &QualifiedName) -> Result<(), OpenError> {
        let name_key = name.name.as_str().to_ascii_lowercase();
        if matches!(self.kind, MigrationKind::Replicated)
            && !self.declared.contains(&name_key)
            && !self.transient.contains(&name_key)
            && !self.objects.contains_key(&name_key)
            && name
                .db_name
                .as_ref()
                .is_none_or(|schema| schema.as_str().eq_ignore_ascii_case("main"))
        {
            self.transient.insert(name_key.clone());
        }
        self.target(name)?;
        self.objects
            .insert(name_key.clone(), (SchemaObjectKind::Table, name_key));
        Ok(())
    }

    fn remove(&mut self, name: &QualifiedName) -> Result<(), OpenError> {
        self.target(name)?;
        let name_key = name.name.as_str().to_ascii_lowercase();
        self.objects
            .retain(|key, (_, owner)| key != &name_key && owner != &name_key);
        self.transient.remove(&name_key);
        Ok(())
    }
}

/// The catalog distinguishes a newly created rebuild table from an existing
/// local table. Only the former may temporarily sit outside the declared set.
pub(crate) fn validate_ownership(
    statements: &[MigrationStatement],
    declared: &[TableName],
    catalog: &[(SchemaEntry, String)],
    views: BTreeMap<String, crate::migration_inspection::MigrationInspection>,
    kind: MigrationKind,
) -> Result<(), OpenError> {
    let mut ownership = TableOwnership {
        declared: declared
            .iter()
            .map(|table| table.as_str().to_ascii_lowercase())
            .collect(),
        objects: catalog
            .iter()
            .map(|(entry, owner)| {
                (
                    entry.name.to_ascii_lowercase(),
                    (entry.kind, owner.to_ascii_lowercase()),
                )
            })
            .collect(),
        transient: BTreeSet::new(),
        views,
        kind,
        key_columns: catalog
            .iter()
            .filter_map(|(entry, _)| {
                let command = turso_parser::parser::Parser::new(entry.sql.as_bytes())
                    .next_cmd()
                    .ok()
                    .flatten()?;
                let (_, keys) = fleet_sync_protocol::replicated_key_columns(&command).ok()??;
                Some((
                    entry.name.to_ascii_lowercase(),
                    keys.into_iter()
                        .map(|key| key.as_str().to_owned())
                        .collect(),
                ))
            })
            .collect(),
    };
    for statement in statements {
        if matches!(kind, MigrationKind::Replicated)
            && let Err(error) = fleet_sync_protocol::replicated_key_columns(&statement.command)
        {
            return Err(OpenError::SchemaUnsupported {
                form: SchemaForm::KeyNotUuidV7 {
                    table: TableName::new(error.table)
                        .map_err(|_| OpenError::corrupt("migration.table"))?,
                    column: error.column,
                },
            });
        }
        if matches!(kind, MigrationKind::Replicated)
            && let Some(function) = &statement.inspection.nondeterministic
        {
            return Err(OpenError::SchemaUnsupported {
                form: SchemaForm::NonDeterministicMigration {
                    statement: statement.sql.clone(),
                    function: function.clone(),
                },
            });
        }
        let Cmd::Stmt(command) = &statement.command else {
            return Err(invalid(statement, "expected a schema or data statement"));
        };
        if matches!(kind, MigrationKind::Replicated) {
            let target = match command {
                Stmt::Insert { tbl_name, .. } => Some(tbl_name),
                Stmt::Update(update) => Some(&update.tbl_name),
                _ => None,
            };
            if let Some(target) = target
                && ownership
                    .key_columns
                    .get(&target.name.as_str().to_ascii_lowercase())
                    .is_some_and(|keys| {
                        keys.iter().any(|key| {
                            fleet_sync_protocol::assigns_replicated_key(&statement.command, key)
                        })
                    })
            {
                return Err(OpenError::WriteRefused {
                    refusal: crate::WriteRefusal::KeyChange {
                        table: target.name.as_str().into(),
                    },
                });
            }
            if let Some((table, keys)) =
                fleet_sync_protocol::replicated_key_columns(&statement.command)
                    .map_err(|_| OpenError::corrupt("migration.key_schema"))?
            {
                ownership.key_columns.insert(
                    table.name.as_str().to_ascii_lowercase(),
                    keys.iter().map(|key| key.as_str().into()).collect(),
                );
            }
        }
        match command {
            Stmt::CreateTable {
                tbl_name,
                temporary,
                ..
            } => {
                if *temporary {
                    return Err(invalid(
                        statement,
                        "temporary connection-local tables cannot replicate",
                    ));
                }
                ownership.create(tbl_name)?;
            }
            Stmt::DropTable { tbl_name, .. } => {
                ownership.remove(tbl_name)?;
                ownership
                    .key_columns
                    .remove(&tbl_name.name.as_str().to_ascii_lowercase());
            }
            Stmt::AlterTable(alter) => {
                ownership.target(&alter.name)?;
                if let AlterTableBody::RenameTo(next) = &alter.body {
                    let next_name = QualifiedName::single(next.clone());
                    ownership.create(&next_name)?;
                    let old_name = alter.name.name.as_str().to_ascii_lowercase();
                    let new_name = next.as_str().to_ascii_lowercase();
                    if let Some(keys) = ownership.key_columns.remove(&old_name) {
                        ownership.key_columns.insert(new_name.clone(), keys);
                    }
                    ownership.objects.remove(&old_name);
                    ownership.transient.remove(&old_name);
                    for (_, owner) in ownership.objects.values_mut() {
                        if *owner == old_name {
                            *owner = new_name.clone();
                        }
                    }
                    for view in ownership.views.values_mut() {
                        for (_, source) in &mut view.tables {
                            if source.eq_ignore_ascii_case(&old_name) {
                                *source = new_name.clone();
                            }
                        }
                    }
                }
                if let AlterTableBody::RenameColumn { old, new } = &alter.body
                    && let Some(keys) = ownership
                        .key_columns
                        .get_mut(&alter.name.name.as_str().to_ascii_lowercase())
                {
                    for key in keys {
                        if key.eq_ignore_ascii_case(old.as_str()) {
                            *key = new.as_str().into();
                        }
                    }
                }
                if let AlterTableBody::AlterColumn { old, new } = &alter.body
                    && let Some(keys) = ownership
                        .key_columns
                        .get_mut(&alter.name.name.as_str().to_ascii_lowercase())
                {
                    for key in keys {
                        if key.eq_ignore_ascii_case(old.as_str()) {
                            *key = new.col_name.as_str().into();
                        }
                    }
                }
            }
            Stmt::Insert { tbl_name, .. } | Stmt::Delete { tbl_name, .. } => {
                ownership.target(tbl_name)?
            }
            Stmt::Update(update) => ownership.target(&update.tbl_name)?,
            Stmt::CreateIndex {
                idx_name, tbl_name, ..
            } => {
                ownership.access(
                    idx_name.db_name.as_ref().map(|name| name.as_str()),
                    tbl_name.as_str(),
                    true,
                )?;
                ownership.objects.insert(
                    idx_name.name.as_str().to_ascii_lowercase(),
                    (
                        SchemaObjectKind::Index,
                        tbl_name.as_str().to_ascii_lowercase(),
                    ),
                );
            }
            Stmt::DropIndex { idx_name, .. } => {
                if let Some((_, owner)) = ownership
                    .objects
                    .get(&idx_name.name.as_str().to_ascii_lowercase())
                {
                    ownership.access(
                        idx_name.db_name.as_ref().map(|name| name.as_str()),
                        owner,
                        true,
                    )?;
                }
                ownership
                    .objects
                    .remove(&idx_name.name.as_str().to_ascii_lowercase());
            }
            Stmt::CreateView {
                view_name,
                temporary,
                ..
            } => {
                if matches!(kind, MigrationKind::Local) {
                    return Err(invalid(
                        statement,
                        "local migrations cannot create or drop views",
                    ));
                }
                if *temporary {
                    return Err(invalid(
                        statement,
                        "connection-local views cannot replicate",
                    ));
                }
                if view_name
                    .db_name
                    .as_ref()
                    .is_some_and(|schema| !schema.as_str().eq_ignore_ascii_case("main"))
                {
                    return Err(table_error(view_name.name.as_str(), false));
                }
                ownership.views.insert(
                    view_name.name.as_str().to_ascii_lowercase(),
                    statement.inspection.clone(),
                );
            }
            Stmt::DropView { view_name, .. } => {
                if matches!(kind, MigrationKind::Local) {
                    return Err(invalid(
                        statement,
                        "local migrations cannot create or drop views",
                    ));
                }
                if ownership
                    .views
                    .contains_key(&view_name.name.as_str().to_ascii_lowercase())
                {
                    ownership.target(view_name)?;
                }
                ownership
                    .views
                    .remove(&view_name.name.as_str().to_ascii_lowercase());
            }
            _ => {
                return Err(invalid(
                    statement,
                    "statement is outside migration admission",
                ));
            }
        }
        for (schema, name) in &statement.inspection.tables {
            ownership.access(schema.as_deref(), name, false)?;
        }
    }
    if matches!(kind, MigrationKind::Replicated) {
        for statement in statements {
            if let Cmd::Stmt(Stmt::CreateView { view_name, .. }) = &statement.command
                && ownership
                    .views
                    .contains_key(&view_name.name.as_str().to_ascii_lowercase())
            {
                ownership.target(view_name)?;
            }
        }
    }
    if let Some(name) = ownership.transient.iter().next() {
        return Err(table_error(name, false));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic_in_result_fn)]
    use super::*;
    use crate::StatementGuard;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
    fn catalog() -> Vec<(SchemaEntry, String)> {
        ["records", "local_notes"]
            .into_iter()
            .map(|name| {
                (
                    SchemaEntry {
                        kind: SchemaObjectKind::Table,
                        name: name.into(),
                        sql: format!("CREATE TABLE {name}(id TEXT NOT NULL PRIMARY KEY)"),
                    },
                    name.into(),
                )
            })
            .collect()
    }
    fn check(sql: &str, kind: MigrationKind) -> TestResult<Result<Vec<String>, OpenError>> {
        Ok(StatementGuard::new(Vec::new()).migration_statements(
            sql,
            &[TableName::new("records")?],
            &catalog(),
            kind,
        ))
    }

    #[test]
    fn rebuild_tables_must_be_created_here_and_leave_no_undeclared_survivor() -> TestResult {
        for sql in [
            "DROP TABLE records; CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY)",
            "CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY); INSERT INTO rebuilt SELECT id FROM records; DROP TABLE records; ALTER TABLE rebuilt RENAME TO records",
            "CREATE TABLE discarded(id TEXT NOT NULL PRIMARY KEY); DROP TABLE discarded",
            "ALTER TABLE records RENAME TO previous; CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY); INSERT INTO records SELECT id FROM previous; DROP TABLE previous",
        ] {
            check(sql, MigrationKind::Replicated)??;
        }
        for sql in [
            "CREATE TABLE leftover(id TEXT NOT NULL PRIMARY KEY)",
            "CREATE TABLE IF NOT EXISTS local_notes(id TEXT NOT NULL PRIMARY KEY); DROP TABLE local_notes",
            "INSERT INTO records SELECT id FROM local_notes",
            "ALTER TABLE local_notes RENAME TO records",
            "CREATE INDEX local_index ON local_notes(id)",
        ] {
            assert!(
                matches!(
                    check(sql, MigrationKind::Replicated)?,
                    Err(OpenError::SchemaUnsupported {
                        form: SchemaForm::UndeclaredTable { .. }
                    })
                ),
                "{sql}"
            );
        }
        Ok(())
    }

    #[test]
    fn physical_references_are_found_inside_subqueries_and_cte_aliases_stay_in_scope() -> TestResult
    {
        for sql in [
            "WITH local_notes AS(SELECT id FROM records) INSERT INTO records SELECT id FROM local_notes",
            "INSERT INTO records SELECT (WITH local_notes AS(SELECT id FROM records) SELECT id FROM local_notes)",
        ] {
            check(sql, MigrationKind::Replicated)??;
        }
        for sql in [
            "INSERT INTO records SELECT (WITH local_notes AS(SELECT id FROM records) SELECT id FROM local_notes) FROM local_notes",
            "WITH local_notes AS(SELECT id FROM records) INSERT INTO records SELECT id FROM main.local_notes",
            "UPDATE records SET payload=(SELECT id FROM local_notes)",
            "DELETE FROM records WHERE id IN local_notes",
            "CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY, parent TEXT REFERENCES local_notes(id)); DROP TABLE rebuilt",
            "WITH source AS(SELECT id FROM local_notes) INSERT INTO records SELECT id FROM source",
        ] {
            assert!(
                matches!(
                    check(sql, MigrationKind::Replicated)?,
                    Err(OpenError::SchemaUnsupported {
                        form: SchemaForm::UndeclaredTable { .. }
                    })
                ),
                "{sql}"
            );
        }
        Ok(())
    }

    #[test]
    fn views_resolve_transitive_dependencies_without_a_primary_key() -> TestResult {
        check(
            "CREATE VIEW first_view AS SELECT id FROM records; CREATE VIEW second_view AS SELECT id FROM first_view; INSERT INTO records SELECT id FROM second_view",
            MigrationKind::Replicated,
        )??;
        check(
            "CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY); CREATE VIEW renamed_view AS SELECT id FROM rebuilt; DROP TABLE records; ALTER TABLE rebuilt RENAME TO records",
            MigrationKind::Replicated,
        )??;
        let mut entries = catalog();
        entries.push((
            SchemaEntry {
                kind: SchemaObjectKind::View,
                name: "allowed_view".into(),
                sql: "CREATE VIEW allowed_view AS SELECT id FROM records".into(),
            },
            "allowed_view".into(),
        ));
        entries.push((
            SchemaEntry {
                kind: SchemaObjectKind::View,
                name: "local_view".into(),
                sql: "CREATE VIEW local_view AS SELECT id FROM local_notes".into(),
            },
            "local_view".into(),
        ));
        let mut guard = StatementGuard::new(Vec::new());
        let declared = [TableName::new("records")?];
        guard.migration_statements(
            "INSERT INTO records SELECT id FROM allowed_view",
            &declared,
            &entries,
            MigrationKind::Replicated,
        )?;
        for sql in [
            "CREATE VIEW bad_view AS SELECT id FROM local_notes",
            "INSERT INTO records SELECT id FROM local_view",
            "CREATE VIEW cycle AS SELECT id FROM cycle",
        ] {
            assert!(
                matches!(
                    guard.migration_statements(sql, &declared, &entries, MigrationKind::Replicated),
                    Err(OpenError::SchemaUnsupported {
                        form: SchemaForm::UndeclaredTable { .. }
                    })
                ),
                "{sql}"
            );
        }
        Ok(())
    }

    #[test]
    fn local_migrations_cannot_mutate_replicated_tables_or_take_over_connections() -> TestResult {
        check(
            "CREATE TABLE local_notes(id TEXT NOT NULL PRIMARY KEY); INSERT INTO local_notes SELECT id FROM records",
            MigrationKind::Local,
        )??;
        for sql in [
            "UPDATE records SET id='changed'",
            "DROP TABLE records",
            "CREATE INDEX replica_index ON records(id)",
            "ALTER TABLE local_notes RENAME TO records",
        ] {
            assert!(
                matches!(
                    check(sql, MigrationKind::Local)?,
                    Err(OpenError::SchemaUnsupported {
                        form: SchemaForm::ReplicatedTableInLocalMigration { .. }
                    })
                ),
                "{sql}"
            );
        }
        for sql in [
            "CREATE VIEW local_view AS SELECT id FROM records",
            "DROP VIEW existing_view",
            "PRAGMA foreign_keys=OFF",
            "ATTACH ':memory:' AS other",
            "BEGIN",
            "UPDATE _fleet_sync_writer SET record='bad'",
        ] {
            assert!(check(sql, MigrationKind::Local)?.is_err(), "{sql}");
        }
        Ok(())
    }
}
