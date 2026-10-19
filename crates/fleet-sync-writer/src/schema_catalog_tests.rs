#![allow(clippy::panic_in_result_fn)]

use crate::*;
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::{PeerId, TableName};
use sqlx_core::{
    migrate::MigrationType,
    row::Row,
    sql_str::{AssertSqlSafe, SqlSafeStr},
};
use sqlx_turso::Turso;
use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub(crate) static TABLES: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
    LazyLock::new(|| Ok(vec![TableName::new("records")?]));
static INITIAL: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    vec![Migration::new(
        1,
        "records".into(),
        MigrationType::Simple,
        "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, body TEXT)".into_sql_str(),
        false,
    )]
});
pub(crate) static TOO_LARGE: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    let mut migrations = INITIAL.clone();
    migrations.push(Migration::new(
        2,
        "oversize current catalog".into(),
        MigrationType::Simple,
        AssertSqlSafe(format!(
            "ALTER TABLE records ADD COLUMN large_default TEXT DEFAULT '{}'",
            "x".repeat(300 * 1024)
        ))
        .into_sql_str(),
        false,
    ));
    migrations
});

pub(crate) struct CatalogSchema {
    pub(crate) expanded: Arc<AtomicBool>,
    pub(crate) tables: &'static [TableName],
}
impl ReplicatedSchema for CatalogSchema {
    fn migrations(&self) -> &'static [Migration] {
        if self.expanded.load(Ordering::Acquire) {
            &TOO_LARGE
        } else {
            &INITIAL
        }
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}

// Compare durable row images, including history checksums and complete capture
// images, rather than only counts that could hide an admitted schema mutation.
async fn catalog_state(
    database: &ReplicatedDatabase<CatalogSchema>,
) -> TestResult<Vec<Vec<String>>> {
    let mut connection = database.maintenance.acquire().await?;
    catalog_state_on(&mut connection).await
}

pub(crate) async fn catalog_state_on(
    connection: &mut sqlx_turso::TursoConnection,
) -> TestResult<Vec<Vec<String>>> {
    let mut state = Vec::new();
    for sql in [
        "SELECT hex(type)||':'||hex(name)||':'||hex(tbl_name)||':'||hex(CAST(rootpage AS TEXT))||':'||hex(coalesce(sql,'')) AS image FROM sqlite_schema ORDER BY type,name",
        "SELECT hex(CAST(version AS TEXT))||':'||hex(description)||':'||hex(CAST(installed_on AS TEXT))||':'||hex(CAST(success AS TEXT))||':'||hex(checksum)||':'||hex(CAST(execution_time AS TEXT)) AS image FROM _sqlx_migrations ORDER BY version",
        "SELECT hex(CAST(version AS TEXT))||':'||hex(statements)||':'||hex(schema_layout) AS image FROM _fleet_sync_schema_steps ORDER BY version",
        "SELECT hex(CAST(change_id AS TEXT))||':'||hex(CAST(change_txn_id AS TEXT))||':'||hex(CAST(change_time AS TEXT))||':'||hex(CAST(change_type AS TEXT))||':'||hex(coalesce(table_name,''))||':'||hex(coalesce(before,''))||':'||hex(coalesce(after,'')) AS image FROM turso_cdc ORDER BY change_id",
        "SELECT hex(record) AS image FROM _fleet_sync_writer ORDER BY singleton",
    ] {
        state.push(
            sqlx_core::query::query::<Turso>(sql)
                .fetch_all(&mut *connection)
                .await?
                .into_iter()
                .map(|row| row.try_get("image"))
                .collect::<Result<Vec<String>, _>>()?,
        );
    }
    Ok(state)
}

#[tokio::test]
async fn over_bound_current_catalog_refuses_migration_without_changing_durable_state() -> TestResult
{
    // Arrange a small admitted catalog, then offer one real migration that
    // leaves a >256 KiB default in sqlite_schema.
    let root = tempfile::tempdir()?;
    let expanded = Arc::new(AtomicBool::new(false));
    let database = ReplicatedDatabase::create(
        &root.path().join("catalog.db"),
        CatalogSchema {
            expanded: expanded.clone(),
            tables: TABLES.as_ref().map_err(Clone::clone)?,
        },
        PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000002,
        ))?,
        LeasePolicy {
            lease_duration: Duration::from_secs(600),
            renew_interval: Duration::from_secs(60),
        },
        WriterOptions::default(),
    )
    .await?;
    let before = catalog_state(&database).await?;
    expanded.store(true, Ordering::Release);

    // Act through public migration admission.
    let result = database.migrate().await;
    assert!(
        matches!(result, Err(OpenError::SchemaUnsupported { form: SchemaForm::SnapshotSchema { encoded_size } }) if encoded_size > 256 * 1024),
        "over-bound current catalog migration was admitted or lost its typed size"
    );

    // Assert rollback of schema, SQLx history, schema-step history and capture.
    assert_eq!(catalog_state(&database).await?, before);
    let mut snapshot = database.snapshot().await?;
    assert_eq!(snapshot.schema().version, 1);
    assert!(serde_json::to_vec(&snapshot.page(None).await?.schema_steps)?.len() < 1024);
    snapshot.close().await?;
    database.close().await;
    Ok(())
}
