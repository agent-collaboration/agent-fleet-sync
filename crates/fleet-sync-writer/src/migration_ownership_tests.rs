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
fn guard() -> TestResult<StatementGuard> {
    Ok(StatementGuard::new(vec![crate::TableLayout::new(
        "records".into(),
        vec!["id".into()],
        vec![0],
    )?]))
}
fn check(sql: &str, kind: MigrationKind) -> TestResult<Result<Vec<String>, OpenError>> {
    Ok(guard()?.migration_statements(sql, &[TableName::new("records")?], &catalog(), kind))
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
fn physical_references_are_found_inside_subqueries_and_cte_aliases_stay_in_scope() -> TestResult {
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
    let mut guard = guard()?;
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
