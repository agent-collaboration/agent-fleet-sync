use crate::{
    OpenError, ReplicatedDatabase, ReplicatedSchema, StatementGuard,
    migration_execution::{inspect_present_tables, offered_migrations},
    migration_lineage::migration_lineage,
    migration_ownership::MigrationKind,
    schema_inspection::schema_entries,
    snapshot_schema::snapshot_schema,
};
use fleet_sync_protocol::{SchemaIdentity, TableName};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    migrate::{Migration, Migrator},
};
use sqlx_turso::TursoConnectOptions;

impl<S: ReplicatedSchema> ReplicatedDatabase<S> {
    /// Prove that this consumer's migration prefix produces the received live
    /// schema. The scratch database cannot change the target or its history.
    pub async fn validate_bootstrap_schema(
        &self,
        expected: &SchemaIdentity,
    ) -> Result<(), OpenError> {
        validate_prefix(&self.schema, expected).await.map(|_| ())
    }
}
pub(crate) struct ValidatedPrefix {
    pub migrations: Vec<Migration>,
    pub tables: Vec<TableName>,
}

pub(crate) async fn validate_prefix(
    schema: &impl ReplicatedSchema,
    expected: &SchemaIdentity,
) -> Result<ValidatedPrefix, OpenError> {
    let offered = offered_migrations(schema)?;
    let lineage = migration_lineage(&offered, schema.replicated_tables())?;
    let prefix = offered
        .into_iter()
        .filter(|migration| {
            u64::try_from(migration.version).is_ok_and(|version| version <= expected.version)
        })
        .collect::<Vec<_>>();
    if prefix
        .last()
        .and_then(|migration| u64::try_from(migration.version).ok())
        .unwrap_or(0)
        != expected.version
    {
        return Err(OpenError::BootstrapVersionUnsupported {
            version: expected.version,
        });
    }
    let mut scratch = TursoConnectOptions::new()
        .in_memory()
        .foreign_keys(false)
        .connect()
        .await?;
    let result = async {
        let mut transaction = scratch.begin_with("BEGIN IMMEDIATE").await?;
        Migrator::with_migrations(Vec::new())
            .run_direct(None, &mut *transaction, false)
            .await
            .map_err(|error| OpenError::Storage {
                message: error.to_string(),
            })?;
        let mut guard = StatementGuard::new(Vec::new());
        let mut tables = Vec::new();
        for migration in &prefix {
            let catalog = schema_entries(&mut transaction).await?;
            let plan = guard.migration_plan(
                migration.sql.as_str(),
                lineage
                    .get(&migration.version)
                    .ok_or_else(|| OpenError::corrupt("migration.lineage"))?,
                &catalog,
                MigrationKind::Replicated,
            )?;
            tables = plan.tables;
            let through = prefix
                .iter()
                .filter(|candidate| candidate.version <= migration.version)
                .cloned()
                .collect::<Vec<_>>();
            Migrator::with_migrations(through)
                .run_direct(None, &mut *transaction, false)
                .await
                .map_err(|error| OpenError::Storage {
                    message: error.to_string(),
                })?;
            guard = StatementGuard::new(inspect_present_tables(&mut transaction, &tables).await?);
        }
        inspect_present_tables(&mut transaction, &tables).await?;
        let (found, _) = snapshot_schema(&mut transaction, &tables).await?;
        if found != *expected {
            return Err(OpenError::BootstrapSchemaMismatch {
                expected: expected.clone(),
                found,
            });
        }
        transaction.rollback().await?;
        Ok(ValidatedPrefix {
            migrations: prefix,
            tables,
        })
    }
    .await;
    let closed = scratch.close().await;
    let prefix = result?;
    closed?;
    Ok(prefix)
}
