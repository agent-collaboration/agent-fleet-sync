use crate::{
    OpenError, StatementGuard, TableLayout, migration_ownership::MigrationKind,
    schema_inspection::schema_entries, writer_scope::WriterScope,
};
use fleet_sync_protocol::{
    SchemaEntry, SchemaIdentity, SchemaObjectKind, TableName, schema_fingerprint,
};
use sqlx_turso::TursoConnection;

pub(crate) async fn snapshot_schema(
    connection: &mut TursoConnection,
    tables: &[TableName],
    layouts: &[TableLayout],
) -> Result<(SchemaIdentity, Vec<String>), OpenError> {
    let mut entries = replicated_entries(connection, tables, layouts).await?;
    let versions =
        crate::checked_queries::read_applied_migrations(&mut WriterScope(connection)).await?;
    let mut version = 0;
    for row in versions {
        if row.version_class.as_deref() != Some("integer") {
            return Err(OpenError::corrupt("snapshot.schema_version"));
        }
        let applied = row
            .version
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| OpenError::corrupt("snapshot.schema_version"))?;
        version = version.max(applied);
    }
    let fingerprint = schema_fingerprint(&entries)
        .map_err(|_| OpenError::corrupt("snapshot.schema_fingerprint"))?;
    let identity = SchemaIdentity::new(version, fingerprint)
        .map_err(|_| OpenError::corrupt("snapshot.schema_fingerprint"))?;
    let rank = |kind| match kind {
        SchemaObjectKind::Table => 0,
        SchemaObjectKind::Index => 1,
        SchemaObjectKind::View => 2,
        SchemaObjectKind::Trigger => 3,
    };
    entries
        .sort_by(|left, right| (rank(left.kind), &left.name).cmp(&(rank(right.kind), &right.name)));
    Ok((
        identity,
        entries.into_iter().map(|entry| entry.sql).collect(),
    ))
}
pub(crate) async fn replicated_entries(
    connection: &mut TursoConnection,
    tables: &[TableName],
    layouts: &[TableLayout],
) -> Result<Vec<SchemaEntry>, OpenError> {
    let catalog = schema_entries(connection).await?;
    let mut guard = StatementGuard::new(layouts.to_vec());
    let mut entries = Vec::<SchemaEntry>::new();
    for (entry, owner) in &catalog {
        let replicated = match entry.kind {
            SchemaObjectKind::Table | SchemaObjectKind::Index => tables
                .iter()
                .any(|table| table.as_str().eq_ignore_ascii_case(owner)),
            SchemaObjectKind::View => guard
                .migration_statements(&entry.sql, tables, &catalog, MigrationKind::Replicated)
                .is_ok(),
            SchemaObjectKind::Trigger => false,
        };
        if replicated && !fleet_sync_protocol::reserved_namespace(&entry.name) {
            entries.push(entry.clone());
        }
    }
    Ok(entries)
}
