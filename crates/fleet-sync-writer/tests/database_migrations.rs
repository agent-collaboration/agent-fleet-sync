#![allow(clippy::panic_in_result_fn)]

use durable_steps::Uuid;
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::{DatabaseId, PeerId, TableName};
use fleet_sync_writer::{
    Migration, OpenError, ReplicatedDatabase, ReplicatedSchema, SchemaForm, WriteRefusal,
    WriterOptions,
};
use sqlx_core::{executor::Executor, migrate::Migrator, query::query, row::Row};
use sqlx_turso::Turso;
use std::{sync::LazyLock, time::Duration};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static REGULAR: Migrator = sqlx::migrate!("./tests/fixtures/regular");
static LOCAL: Migrator = sqlx::migrate!("./tests/fixtures/local");
static RANDOM: Migrator = sqlx::migrate!("./tests/fixtures/nondeterministic");
static TABLES: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
    LazyLock::new(|| {
        ["text_records", "composite_records", "integer_records"]
            .into_iter()
            .map(TableName::new)
            .collect()
    });

struct TestSchema {
    migrations: &'static [Migration],
    local: &'static [Migration],
    tables: &'static [TableName],
}
impl ReplicatedSchema for TestSchema {
    fn migrations(&self) -> &'static [Migration] {
        self.migrations
    }
    fn local_migrations(&self) -> &'static [Migration] {
        self.local
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}
fn schema() -> TestResult<TestSchema> {
    Ok(TestSchema {
        migrations: REGULAR.iter().as_slice(),
        local: LOCAL.iter().as_slice(),
        tables: TABLES.as_ref().map_err(Clone::clone)?,
    })
}
fn peer() -> TestResult<PeerId> {
    Ok(PeerId::new(Uuid::from_u128(2))?)
}
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(10),
        renew_interval: Duration::from_secs(1),
    }
}

#[tokio::test]
async fn create_captures_each_migration_atomically_and_keeps_local_tables_out() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let database = ReplicatedDatabase::create(
        &path,
        schema()?,
        peer()?,
        policy(),
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    let row = query::<Turso>("SELECT amount,label FROM text_records WHERE id='initial'")
        .fetch_one(database.reader())
        .await?;
    assert_eq!(row.try_get::<i64, _>("amount")?, 9_007_199_254_740_993);
    assert_eq!(row.try_get::<String, _>("label")?, "backfilled");
    let rows = query::<Turso>("SELECT change_txn_id,table_name FROM turso_cdc ORDER BY change_id")
        .fetch_all(database.reader())
        .await?;
    let mut groups = std::collections::BTreeMap::<i64, Vec<String>>::new();
    for row in rows {
        if let Some(table) = row.try_get::<Option<String>, _>("table_name")? {
            groups
                .entry(row.try_get("change_txn_id")?)
                .or_default()
                .push(table);
        }
    }
    let schema_groups = groups
        .values()
        .filter(|tables| {
            tables
                .iter()
                .any(|table| table == "_fleet_sync_schema_steps")
        })
        .collect::<Vec<_>>();
    assert_eq!(schema_groups.len(), 2);
    for tables in schema_groups {
        assert!(tables.iter().any(|table| table == "_sqlx_migrations"));
    }
    assert!(
        !groups.values().flatten().any(|table| table == "local_notes"
            || table == "_fleet_sync_local_migrations"
            || table == "_fleet_sync_writer")
    );
    let mut write = database.begin_write().await?;
    (&mut write).execute("INSERT INTO local_notes VALUES('local','never copied'); UPDATE text_records SET id='renamed' WHERE id='initial'").await?;
    write.commit().await?;
    let id = database.database();
    database.close().await;
    let reopened = ReplicatedDatabase::open(
        &path,
        id,
        schema()?,
        LeaseHolder::provisional(id, peer()?, policy()),
    )
    .await?;
    assert!(matches!(
        reopened.begin_write().await,
        Err(WriteRefusal::Provisional)
    ));
    assert_eq!(
        query::<Turso>("SELECT id FROM text_records")
            .fetch_one(reopened.reader())
            .await?
            .try_get::<String, _>("id")?,
        "renamed"
    );
    assert_eq!(
        query::<Turso>("SELECT version FROM _fleet_sync_schema_steps ORDER BY version")
            .fetch_all(reopened.reader())
            .await?
            .len(),
        2
    );
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn nondeterministic_migration_is_refused_before_it_changes_schema_or_history() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let mut offered = schema()?;
    offered.migrations = RANDOM.iter().as_slice();
    let result =
        ReplicatedDatabase::create(&path, offered, peer()?, policy(), WriterOptions::default())
            .await;
    assert!(matches!(
        result,
        Err(OpenError::SchemaUnsupported {
            form: SchemaForm::NonDeterministicMigration { .. }
        })
    ));
    let raw = sqlx_core::pool::PoolOptions::<Turso>::new()
        .connect_with(sqlx_turso::TursoConnectOptions::new().filename(&path))
        .await?;
    assert!(
        query::<Turso>("SELECT name FROM sqlite_schema WHERE name='text_records'")
            .fetch_optional(&raw)
            .await?
            .is_none()
    );
    assert!(
        query::<Turso>("SELECT version FROM _sqlx_migrations")
            .fetch_all(&raw)
            .await?
            .is_empty()
    );
    assert!(
        query::<Turso>("SELECT version FROM _fleet_sync_schema_steps")
            .fetch_all(&raw)
            .await?
            .is_empty()
    );
    raw.close().await;
    Ok(())
}

#[tokio::test]
async fn refusing_a_later_undeclared_reference_prevents_earlier_statements() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let id = DatabaseId::new(Uuid::from_u128(1))?;
    let mut offered = schema()?;
    static BAD: Migrator = sqlx::migrate!("./tests/fixtures/failed");
    offered.migrations = BAD.iter().as_slice();
    assert!(matches!(
        ReplicatedDatabase::open(
            &path,
            id,
            offered,
            LeaseHolder::provisional(id, peer()?, policy())
        )
        .await,
        Err(OpenError::SchemaUnsupported {
            form: SchemaForm::UndeclaredTable { .. }
        })
    ));
    let raw = sqlx_core::pool::PoolOptions::<Turso>::new()
        .connect_with(sqlx_turso::TursoConnectOptions::new().filename(&path))
        .await?;
    assert!(
        query::<Turso>("SELECT name FROM sqlite_schema WHERE name='text_records'")
            .fetch_optional(&raw)
            .await?
            .is_none()
    );
    assert!(
        query::<Turso>("SELECT version FROM _sqlx_migrations")
            .fetch_all(&raw)
            .await?
            .is_empty()
    );
    raw.close().await;
    Ok(())
}

#[tokio::test]
async fn execution_failure_rolls_back_schema_data_history_and_capture() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    static CONFLICT: Migrator = sqlx::migrate!("./tests/fixtures/constraint_failure");
    let mut offered = schema()?;
    offered.migrations = CONFLICT.iter().as_slice();
    assert!(matches!(
        ReplicatedDatabase::create(&path, offered, peer()?, policy(), WriterOptions::default())
            .await,
        Err(OpenError::Storage { .. })
    ));
    let raw = sqlx_core::pool::PoolOptions::<Turso>::new()
        .connect_with(sqlx_turso::TursoConnectOptions::new().filename(&path))
        .await?;
    assert!(
        query::<Turso>("SELECT name FROM sqlite_schema WHERE name='text_records'")
            .fetch_optional(&raw)
            .await?
            .is_none()
    );
    for statement in [
        "SELECT version FROM _sqlx_migrations",
        "SELECT version FROM _fleet_sync_schema_steps",
        "SELECT change_id FROM turso_cdc WHERE table_name IN ('text_records','_sqlx_migrations','_fleet_sync_schema_steps')",
    ] {
        assert!(raw.fetch_all(statement).await?.is_empty(), "{statement}");
    }
    raw.close().await;
    Ok(())
}

#[tokio::test]
async fn ordered_rebuilds_and_transitive_views_keep_existing_rows_on_reopen() -> TestResult {
    use sqlx_core::{migrate::MigrationType, sql_str::SqlSafeStr};
    static REBUILDS: LazyLock<Vec<Migration>> = LazyLock::new(|| {
        let mut migrations = REGULAR.iter().cloned().collect::<Vec<_>>();
        migrations.push(Migration::new(3,"copy rename".into(),MigrationType::Simple,"CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY,amount INTEGER,label TEXT); INSERT INTO rebuilt SELECT * FROM text_records; DROP TABLE text_records; ALTER TABLE rebuilt RENAME TO text_records; CREATE VIEW records_view AS SELECT * FROM text_records; CREATE VIEW nested_view AS SELECT * FROM records_view;".into_sql_str(),false));
        migrations.push(Migration::new(4,"same name rebuild".into(),MigrationType::Simple,"DROP VIEW nested_view; DROP VIEW records_view; ALTER TABLE text_records RENAME TO previous_records; CREATE TABLE text_records(id TEXT NOT NULL PRIMARY KEY,amount INTEGER,label TEXT); INSERT INTO text_records SELECT * FROM previous_records; DROP TABLE previous_records; CREATE VIEW records_view AS SELECT * FROM text_records; CREATE VIEW nested_view AS SELECT * FROM records_view;".into_sql_str(),false));
        migrations
    });
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let mut offered = schema()?;
    offered.migrations = &REBUILDS;
    let database =
        ReplicatedDatabase::create(&path, offered, peer()?, policy(), WriterOptions::default())
            .await?;
    let row = query::<Turso>("SELECT id,amount,label FROM nested_view")
        .fetch_one(database.reader())
        .await?;
    assert_eq!(row.try_get::<String, _>("id")?, "initial");
    assert_eq!(row.try_get::<i64, _>("amount")?, 9_007_199_254_740_993);
    assert_eq!(row.try_get::<String, _>("label")?, "backfilled");
    let id = database.database();
    database.close().await;
    let mut offered = schema()?;
    offered.migrations = &REBUILDS;
    let reopened = ReplicatedDatabase::open(
        &path,
        id,
        offered,
        LeaseHolder::provisional(id, peer()?, policy()),
    )
    .await?;
    assert_eq!(
        query::<Turso>("SELECT version FROM _fleet_sync_schema_steps ORDER BY version")
            .fetch_all(reopened.reader())
            .await?
            .len(),
        4
    );
    assert!(
        query::<Turso>(
            "SELECT name FROM sqlite_schema WHERE name IN ('rebuilt','previous_records')"
        )
        .fetch_all(reopened.reader())
        .await?
        .is_empty()
    );
    reopened.close().await;
    Ok(())
}
