use crate::{OpenError, schema_inspection::schema_entries, writer_scope::WriterScope};
use fleet_sync_protocol::schema_catalog::{current_replicated_catalog, snapshot_schema_statements};
use fleet_sync_protocol::{SchemaEntry, SchemaIdentity, TableName, schema_fingerprint};
use sqlx_turso::TursoConnection;

pub(crate) async fn snapshot_schema(
    connection: &mut TursoConnection,
    tables: &[TableName],
) -> Result<(SchemaIdentity, Vec<String>), OpenError> {
    let entries = replicated_entries(connection, tables).await?;
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
    Ok((
        identity,
        snapshot_schema_statements(entries).map_err(|error| OpenError::SchemaUnsupported {
            form: crate::SchemaForm::SnapshotSchema {
                encoded_size: error.encoded_size,
            },
        })?,
    ))
}

pub(crate) async fn replicated_entries(
    connection: &mut TursoConnection,
    tables: &[TableName],
) -> Result<Vec<SchemaEntry>, OpenError> {
    Ok(current_replicated_catalog(
        &schema_entries(connection).await?,
        tables,
    ))
}
