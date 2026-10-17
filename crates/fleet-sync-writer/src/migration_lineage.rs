use crate::{OpenError, SchemaForm};
use fleet_sync_protocol::TableName;
use sqlx_core::migrate::Migration;
use std::collections::{BTreeMap, BTreeSet};
use turso_parser::{
    ast::{AlterTableBody, Cmd, Stmt},
    parser::Parser,
};

/// Candidate names come only from final declarations and their future renames.
/// Ownership still has to prove an existing table or admit its creation.
pub(crate) fn migration_lineage(
    migrations: &[Migration],
    declared: &[TableName],
) -> Result<BTreeMap<i64, Vec<TableName>>, OpenError> {
    let mut names = declared
        .iter()
        .cloned()
        .map(|name| (name.as_str().to_ascii_lowercase(), name))
        .collect::<BTreeMap<_, _>>();
    let mut lineage = BTreeMap::new();
    for migration in migrations.iter().rev() {
        let mut parser = Parser::new(migration.sql.as_str().as_bytes());
        let mut renames = Vec::new();
        while let Some(command) =
            parser
                .next_cmd()
                .map_err(|error| OpenError::SchemaUnsupported {
                    form: SchemaForm::InvalidMigration {
                        statement: migration.sql.as_str().into(),
                        detail: error.to_string(),
                    },
                })?
        {
            if let Cmd::Stmt(Stmt::AlterTable(alter)) = command
                && alter
                    .name
                    .db_name
                    .as_ref()
                    .is_none_or(|schema| schema.as_str().eq_ignore_ascii_case("main"))
                && let AlterTableBody::RenameTo(next) = alter.body
            {
                renames.push((
                    alter.name.name.as_str().to_owned(),
                    next.as_str().to_owned(),
                ));
            }
        }
        for (previous, next) in renames.into_iter().rev() {
            if names.contains_key(&next.to_ascii_lowercase()) {
                let name = TableName::new(previous)
                    .map_err(|_| OpenError::corrupt("migration.lineage"))?;
                names.insert(name.as_str().to_ascii_lowercase(), name);
            }
        }
        lineage.insert(migration.version, names.values().cloned().collect());
    }
    Ok(lineage)
}

pub(crate) fn require_final_membership(
    current: &[crate::TableLayout],
    declared: &[TableName],
) -> Result<(), OpenError> {
    let final_names = declared
        .iter()
        .map(|table| table.as_str().to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    for table in current {
        if !final_names.contains(&table.name().to_ascii_lowercase()) {
            return Err(OpenError::SchemaUnsupported {
                form: SchemaForm::UndeclaredTable {
                    table: TableName::new(table.name())
                        .map_err(|_| OpenError::corrupt("migration.membership"))?,
                },
            });
        }
    }
    Ok(())
}
