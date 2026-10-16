//! Scratch stores and the owned-transaction migration policy the tests share

use std::path::Path;

use sqlx::{
    AssertSqlSafe, SqlSafeStr,
    migrate::{Migration, MigrationType, Migrator},
};
use sqlx_turso::{
    TursoConnectOptions, TursoConnection,
    sqlx::{ConnectOptions, Connection},
};

use super::TestResult;

/// The base project-store slice, embedded the way a Router store embeds its migrations
pub static PROJECT_STORE_MIGRATOR: Migrator = sqlx::migrate!("./tests/migrations");

const TASKS_LABEL_STEP: &str = include_str!("../migration_steps/2_tasks_label.sql");
const TASKS_REBUILD_STEP: &str = include_str!("../migration_steps/3_tasks_rebuild.sql");
const TASKS_COPY_RENAME_REBUILD_STEP: &str =
    include_str!("../migration_steps/3_tasks_copy_rename_rebuild.sql");

type MigrationStep = (i64, &'static str, &'static str);

/// The project-store migrations through `last_version`
///
/// Version 1 is the base slice, 2 adds `tasks.label`, 3 drops and recreates `tasks` under the
/// same name (the caller reinserts the rows).
pub fn project_store_migrator_through(last_version: i64) -> Migrator {
    let steps = [
        (2, "tasks label", TASKS_LABEL_STEP),
        (3, "tasks rebuild", TASKS_REBUILD_STEP),
    ];
    let selected: Vec<MigrationStep> = steps
        .into_iter()
        .filter(|(version, _, _)| *version <= last_version)
        .collect();
    migrator_with_steps(&selected)
}

/// The base slice, the label step, then a SQL-only rebuild of `tasks` as version 3: create the
/// new table, copy the rows, drop the old table, rename the new one
pub fn project_store_migrator_with_copy_rename_rebuild() -> Migrator {
    migrator_with_steps(&[
        (2, "tasks label", TASKS_LABEL_STEP),
        (
            3,
            "tasks copy and rename rebuild",
            TASKS_COPY_RENAME_REBUILD_STEP,
        ),
    ])
}

fn migrator_with_steps(steps: &[MigrationStep]) -> Migrator {
    let mut migrations: Vec<Migration> = PROJECT_STORE_MIGRATOR.iter().cloned().collect();
    for &(version, description, sql) in steps {
        migrations.push(Migration::new(
            version,
            description.into(),
            MigrationType::Simple,
            AssertSqlSafe(sql).into_sql_str(),
            false,
        ));
    }
    Migrator::with_migrations(migrations)
}

/// Opens a local file store, creating it if needed
pub async fn open_local_store(path: &Path) -> sqlx::Result<TursoConnection> {
    TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .connect()
        .await
}

/// Runs `migrator` in one owned `BEGIN IMMEDIATE` transaction and checks foreign keys before
/// commit, as a Router store does at startup
pub async fn migrate_in_owned_transaction(
    connection: &mut TursoConnection,
    migrator: &Migrator,
) -> TestResult {
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    migrator.run_direct(None, &mut *transaction, false).await?;
    ensure_no_dangling_foreign_keys(&mut transaction).await?;
    transaction.commit().await?;
    Ok(())
}

/// Fails when `PRAGMA foreign_key_check` reports any dangling reference
pub async fn ensure_no_dangling_foreign_keys(connection: &mut TursoConnection) -> TestResult {
    let dangling = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut *connection)
        .await?;
    if dangling.is_empty() {
        Ok(())
    } else {
        Err(format!("{} dangling foreign-key references", dangling.len()).into())
    }
}

/// Reads `PRAGMA foreign_keys` back from the connection
pub async fn foreign_keys_enabled(connection: &mut TursoConnection) -> sqlx::Result<bool> {
    let flag: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(connection)
        .await?;
    Ok(flag == 1)
}

/// Sets `PRAGMA foreign_keys` outside any transaction, where SQLite-family engines honour it
pub async fn set_foreign_keys(connection: &mut TursoConnection, enabled: bool) -> sqlx::Result<()> {
    let statement = if enabled {
        "PRAGMA foreign_keys = ON"
    } else {
        "PRAGMA foreign_keys = OFF"
    };
    sqlx::query(statement).execute(connection).await?;
    Ok(())
}

/// Table-name prefixes the engine owns: SQLite's catalog and Turso's internal tables, such as
/// the change-capture table `turso_cdc`
const ENGINE_TABLE_PREFIXES: [&str; 3] = ["sqlite_", "turso_", "__turso_internal"];

/// Every application table's name and whitespace-normalized definition, the migration history
/// included and engine-owned tables excluded
pub async fn schema_fingerprint(
    connection: &mut TursoConnection,
) -> sqlx::Result<Vec<(String, String)>> {
    let tables = sqlx_turso::query!(
        r#"SELECT name AS "name!: String", sql AS "sql!: String"
           FROM sqlite_schema
           WHERE type = 'table'
           ORDER BY name"#
    )
    .fetch_all(connection)
    .await?;
    Ok(tables
        .into_iter()
        .filter(|table| {
            !ENGINE_TABLE_PREFIXES
                .iter()
                .any(|prefix| table.name.starts_with(prefix))
        })
        .map(|table| {
            let normalized = table.sql.split_whitespace().collect::<Vec<_>>().join(" ");
            (table.name, normalized)
        })
        .collect())
}

/// The fingerprint a fresh native in-memory database has after the same migrations
pub async fn expected_fingerprint(migrator: &Migrator) -> TestResult<Vec<(String, String)>> {
    let mut fresh = TursoConnectOptions::new().connect().await?;
    migrate_in_owned_transaction(&mut fresh, migrator).await?;
    let fingerprint = schema_fingerprint(&mut fresh).await?;
    fresh.close().await?;
    Ok(fingerprint)
}
