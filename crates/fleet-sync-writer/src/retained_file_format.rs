//! Read the histories and schema without admitting or running a migration.
use crate::{
    OpenError, ReplicatedSchema,
    foreign_key_integrity::reject_orphans,
    migration_execution::{LOCAL_HISTORY_TABLE, offered_migrations},
    schema_inspection::inspect_replicated_tables,
    writer_file_record::{BOOKKEEPING_HISTORY_TABLE, BOOKKEEPING_MIGRATOR},
};
use sqlx_core::migrate::{Migrate, Migration};
use sqlx_turso::TursoConnection;

pub(crate) async fn validate(
    connection: &mut TursoConnection,
    schema: &impl ReplicatedSchema,
) -> Result<(), OpenError> {
    require_history(
        connection,
        BOOKKEEPING_HISTORY_TABLE,
        BOOKKEEPING_MIGRATOR.iter().as_slice(),
    )
    .await?;
    let replicated = offered_migrations(schema)?;
    require_history(connection, "_sqlx_migrations", &replicated).await?;
    let local = schema.local_migrations();
    let mut previous_version = 0;
    for migration in local {
        if migration.version <= previous_version
            || migration.no_tx
            || !migration.migration_type.is_up_migration()
        {
            return Err(OpenError::RetainedFormatUnsupported);
        }
        previous_version = migration.version;
    }
    require_history(connection, LOCAL_HISTORY_TABLE, local).await?;
    inspect_replicated_tables(connection, schema.replicated_tables()).await?;
    reject_orphans(connection, schema.replicated_tables()).await?;
    Ok(())
}

async fn require_history(
    connection: &mut TursoConnection,
    table: &str,
    expected: &[Migration],
) -> Result<(), OpenError> {
    // These Migrate methods only SELECT existing rows. In particular, do not
    // call ensure_migrations_table or Migrator::run_direct on a retained file.
    if connection
        .dirty_version(table)
        .await
        .map_err(storage)?
        .is_some()
    {
        return Err(OpenError::RetainedFormatUnsupported);
    }
    let applied = connection
        .list_applied_migrations(table)
        .await
        .map_err(storage)?;
    if applied.len() != expected.len()
        || applied.iter().zip(expected).any(|(applied, expected)| {
            applied.version != expected.version
                || applied.checksum.as_ref() != expected.checksum.as_ref()
        })
    {
        return Err(OpenError::RetainedFormatUnsupported);
    }
    Ok(())
}

fn storage(error: impl std::fmt::Display) -> OpenError {
    OpenError::Storage {
        message: error.to_string(),
    }
}
