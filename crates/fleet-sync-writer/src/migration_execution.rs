use crate::{
    OpenError, ReplicatedDatabase, ReplicatedSchema, SchemaForm, StatementGuard, checked_queries,
    foreign_key_integrity::reject_orphans,
    migration_ownership::MigrationKind,
    schema_inspection::{inspect_replicated_tables, schema_entries},
    writer_scope::WriterScope,
};
use fleet_sync_lease::WriterState;
use fleet_sync_protocol::TableName;
use sqlx_core::{
    connection::Connection,
    executor::Executor,
    migrate::{Migration, Migrator},
};
use sqlx_turso::TursoConnection;

async fn inspect_present_tables(
    connection: &mut TursoConnection,
    tables: &[TableName],
) -> Result<Vec<crate::TableLayout>, OpenError> {
    let entries = schema_entries(connection).await?;
    let present = tables
        .iter()
        .filter(|table| {
            entries.iter().any(|(entry, _)| {
                entry.name.eq_ignore_ascii_case(table.as_str())
                    && entry.kind == fleet_sync_protocol::SchemaObjectKind::Table
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    let layouts = inspect_replicated_tables(connection, &present).await?;
    reject_orphans(connection, &present).await?;
    Ok(layouts)
}

impl<S: ReplicatedSchema> ReplicatedDatabase<S> {
    pub async fn migrate(&self) -> Result<(), OpenError> {
        let mut control = self.control.lock().await;
        {
            let mut connection = self.maintenance.acquire().await?;
            crate::writer_authority::restore_authority(&mut connection, &mut control.holder)
                .await?;
        }
        let offered = self
            .schema
            .migrations()
            .iter()
            .filter(|migration| migration.migration_type.is_up_migration())
            .cloned()
            .collect::<Vec<_>>();
        let mut previous = 0_i64;
        for migration in &offered {
            if migration.version <= previous || migration.no_tx {
                return Err(OpenError::SchemaUnsupported {form:SchemaForm::InvalidMigration {statement:migration.sql.as_str().into(),detail:"migrations must have increasing positive versions and use an owned transaction".into()}});
            }
            previous = migration.version;
        }
        let mut maintenance = self.maintenance.acquire().await?;
        let applied =
            checked_queries::read_applied_migrations(&mut WriterScope(&mut maintenance)).await?;
        let applied = applied
            .into_iter()
            .map(|row| {
                if row.version_class.as_deref() != Some("integer") {
                    return Err(OpenError::corrupt("migration.version"));
                }
                row.version
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or_else(|| OpenError::corrupt("migration.version"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let latest = applied.iter().copied().max().unwrap_or(0);
        if i64::try_from(latest)
            .ok()
            .is_none_or(|latest| latest > previous)
        {
            return Err(OpenError::SchemaUnsupported {
                form: SchemaForm::SchemaTooNew {
                    store: latest,
                    supported: u64::try_from(previous)
                        .map_err(|_| OpenError::corrupt("migration.version"))?,
                },
            });
        }
        // Validate recorded SQLx checksums without admitting an unexecuted migration.
        let history = offered
            .iter()
            .filter(|migration| {
                u64::try_from(migration.version).is_ok_and(|version| applied.contains(&version))
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut check_transaction = maintenance.begin_with("BEGIN IMMEDIATE").await?;
        Migrator::with_migrations(history)
            .run_direct(None, &mut *check_transaction, false)
            .await
            .map_err(|error| OpenError::Storage {
                message: error.to_string(),
            })?;
        check_transaction.commit().await?;
        drop(maintenance);
        for migration in &offered {
            if u64::try_from(migration.version).is_ok_and(|version| applied.contains(&version)) {
                continue;
            }
            if !matches!(
                control.holder.writer_state(self.clock.now()),
                Ok(WriterState::Provisional)
            ) {
                control
                    .admit(self.clock.now())
                    .map_err(|refusal| OpenError::WriteRefused { refusal })?;
            }
            let catalog = {
                let mut connection = self.maintenance.acquire().await?;
                schema_entries(&mut connection).await?
            };
            let statements = control.statements.migration_statements(
                migration.sql.as_str(),
                self.schema.replicated_tables(),
                &catalog,
                MigrationKind::Replicated,
            )?;
            let mut connection = self.writer.acquire().await?;
            connection.execute("PRAGMA foreign_keys=OFF").await?;
            let prefix = offered
                .iter()
                .filter(|candidate| candidate.version <= migration.version)
                .cloned()
                .collect::<Vec<_>>();
            let result = apply_migration(
                &mut connection,
                migration,
                prefix,
                statements,
                self.schema.replicated_tables(),
                self.clock.as_ref(),
                &control,
            )
            .await;
            let enforcement = connection.execute("PRAGMA foreign_keys=ON").await;
            if let Err(error) = enforcement {
                connection.close().await?;
                return Err(OpenError::Storage {
                    message: format!("could not restore writer foreign keys: {error}"),
                });
            }
            let layouts = result?;
            control.statements = StatementGuard::new(layouts);
        }
        let mut connection = self.maintenance.acquire().await?;
        let layouts =
            inspect_replicated_tables(&mut connection, self.schema.replicated_tables()).await?;
        reject_orphans(&mut connection, self.schema.replicated_tables()).await?;
        control.statements = StatementGuard::new(layouts);
        let local = self.schema.local_migrations().to_vec();
        let catalog = schema_entries(&mut connection).await?;
        let mut previous_local = 0;
        for migration in &local {
            if migration.version <= previous_local
                || migration.no_tx
                || !migration.migration_type.is_up_migration()
            {
                return Err(OpenError::SchemaUnsupported { form: SchemaForm::InvalidMigration { statement: migration.sql.as_str().into(), detail: "local migrations must have increasing positive versions and use an owned transaction".into() } });
            }
            previous_local = migration.version;
            control.statements.migration_statements(
                migration.sql.as_str(),
                self.schema.replicated_tables(),
                &catalog,
                MigrationKind::Local,
            )?;
        }
        let mut local_transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let mut local_migrator = Migrator::with_migrations(local);
        local_migrator.dangerous_set_table_name("_fleet_sync_local_migrations");
        local_migrator
            .run_direct(None, &mut *local_transaction, false)
            .await
            .map_err(|error| OpenError::Storage {
                message: error.to_string(),
            })?;
        local_transaction.commit().await?;
        Ok(())
    }
}

async fn apply_migration(
    connection: &mut TursoConnection,
    migration: &Migration,
    prefix: Vec<Migration>,
    statements: Vec<String>,
    tables: &[TableName],
    clock: &dyn runtime_primitives::Clock,
    control: &crate::write_transaction::WriterControl,
) -> Result<Vec<crate::TableLayout>, OpenError> {
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let result = async {
        Migrator::with_migrations(prefix)
            .run_direct(None, &mut *transaction, false)
            .await
            .map_err(|error| OpenError::Storage {
                message: error.to_string(),
            })?;
        let layouts = inspect_present_tables(&mut transaction, tables).await?;
        let encoded_statements = serde_json::to_string(&statements)
            .map_err(|_| OpenError::corrupt("migration.statements"))?;
        let encoded_layout =
            serde_json::to_string(&layouts).map_err(|_| OpenError::corrupt("migration.layout"))?;
        checked_queries::write_schema_step(
            &mut WriterScope(&mut transaction),
            migration.version,
            encoded_statements,
            encoded_layout,
        )
        .await?;
        if !matches!(
            control.holder.writer_state(clock.now()),
            Ok(WriterState::Provisional)
        ) {
            control
                .admit(clock.now())
                .map_err(|refusal| OpenError::WriteRefused { refusal })?;
        }
        Ok::<_, OpenError>(layouts)
    }
    .await;
    match result {
        Ok(layouts) => {
            transaction.commit().await?;
            Ok(layouts)
        }
        Err(error) => {
            transaction.rollback().await?;
            Err(error)
        }
    }
}
