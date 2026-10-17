use crate::{
    AdoptionHistory, OpenError, ReplicatedSchema, TableLayout, checked_queries,
    migration_execution::{inspect_present_tables, offered_migrations},
    writer_scope::WriterScope,
};
use fleet_sync_protocol::{SchemaEntry, SchemaObjectKind, TableName, reserved_namespace};
use sqlx_turso::TursoConnection;

fn invalid(reason: AdoptionHistory) -> OpenError {
    OpenError::AdoptionHistory { reason }
}

/// Runs inside the initial transaction before any managed metadata is written.
pub(crate) async fn validate_adoption(
    connection: &mut TursoConnection,
    schema: &impl ReplicatedSchema,
    entries: &[(SchemaEntry, String)],
) -> Result<Vec<TableLayout>, OpenError> {
    if !entries
        .iter()
        .any(|(entry, _)| entry.kind == SchemaObjectKind::Table && entry.name == "_sqlx_migrations")
    {
        return Err(invalid(AdoptionHistory::Missing));
    }
    let offered = offered_migrations(schema)?;
    let history = checked_queries::read_adoption_history(&mut WriterScope(connection)).await?;
    if history.is_empty() {
        return Err(invalid(AdoptionHistory::Missing));
    }
    let at_latest = history.len() == offered.len();
    for (index, row) in history.into_iter().enumerate() {
        let version = text_integer(row.version, row.version_class, "version")?;
        let Some(expected) = offered
            .get(index)
            .filter(|migration| migration.version == version)
        else {
            return Err(invalid(AdoptionHistory::NonPrefix {
                version,
                expected: offered.get(index).map(|migration| migration.version),
            }));
        };
        if text_integer(row.success, row.success_class, "success")? != 1 {
            return Err(invalid(AdoptionHistory::Failed { version }));
        }
        if row.checksum_class.as_deref() != Some("blob") {
            return Err(invalid(AdoptionHistory::Malformed {
                field: "checksum".into(),
            }));
        }
        if row.checksum.as_deref() != Some(expected.checksum.as_ref()) {
            return Err(invalid(AdoptionHistory::Checksum { version }));
        }
    }
    for table in schema.replicated_tables() {
        if reserved_namespace(table.as_str()) {
            return Err(OpenError::ReservedTableName {
                table: table.clone(),
            });
        }
    }
    crate::schema_inspection::reject_autoincrement(entries, schema.replicated_tables())?;
    for (entry, _) in entries {
        // SQLite's own sequence table is engine metadata. The caller must not
        // supply pre-existing replication/CDC metadata under a reserved name.
        if reserved_namespace(&entry.name)
            && entry.name != "_sqlx_migrations"
            && entry.name != "sqlite_sequence"
            && !entry
                .name
                .starts_with(turso::core::schema::SEQ_BACKING_TABLE_PREFIX)
        {
            return Err(OpenError::ReservedTableName {
                table: TableName::new(entry.name.clone())
                    .map_err(|_| OpenError::corrupt("schema.name"))?,
            });
        }
    }
    if at_latest {
        let layouts = crate::schema_inspection::inspect_replicated_tables(
            connection,
            schema.replicated_tables(),
        )
        .await?;
        crate::foreign_key_integrity::reject_orphans(connection, schema.replicated_tables())
            .await?;
        Ok(layouts)
    } else {
        inspect_present_tables(connection, schema.replicated_tables()).await
    }
}

fn text_integer(
    value: Option<String>,
    class: Option<String>,
    field: &str,
) -> Result<i64, OpenError> {
    // History must be an integer in storage, not merely coercible text.
    if class.as_deref() != Some("integer") {
        return Err(invalid(AdoptionHistory::Malformed {
            field: field.into(),
        }));
    }
    value.and_then(|value| value.parse().ok()).ok_or_else(|| {
        invalid(AdoptionHistory::Malformed {
            field: field.into(),
        })
    })
}
