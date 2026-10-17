#![allow(clippy::panic_in_result_fn)]
use durable_steps::Uuid;
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::{PeerId, TableName};
use fleet_sync_writer::{
    Migration, OpenError, ReplicatedDatabase, ReplicatedSchema, SchemaForm, WriterOptions,
};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    migrate::MigrationType,
    query::query,
    row::Row,
    sql_str::SqlSafeStr,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct AdoptAutoSchema {
    tables: &'static [TableName],
}
impl ReplicatedSchema for AdoptAutoSchema {
    fn migrations(&self) -> &'static [Migration] {
        static AUTO: LazyLock<Vec<Migration>> = LazyLock::new(|| {
            vec![Migration::new(1,"existing auto".into(),MigrationType::Simple,"CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,payload TEXT); INSERT INTO records VALUES(100,'kept')".into_sql_str(),false)]
        });
        &AUTO
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}
#[tokio::test]
async fn adoption_refuses_autoincrement_even_with_matching_history_without_writing() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("existing.db");
    let tables = TABLES
        .as_ref()
        .map(Vec::as_slice)
        .map_err(Clone::clone)?
        .get(..1)
        .ok_or("no table")?;
    let schema = AdoptAutoSchema { tables };
    let mut connection = TursoConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .connect()
        .await?;
    sqlx_core::migrate::Migrator::with_migrations(schema.migrations().to_vec())
        .run_direct(None, &mut connection, false)
        .await?;
    connection.close().await?;
    let before = tokio::fs::read(&path).await?;
    let result =
        ReplicatedDatabase::adopt(&path, schema, peer()?, policy(), WriterOptions::default()).await;
    let error = match result {
        Ok(writer) => {
            writer.close().await;
            return Err("AUTOINCREMENT adoption admitted".into());
        }
        Err(error) => error,
    };
    assert!(refused(&error, "records"), "{error}");
    assert_eq!(tokio::fs::read(&path).await?, before);
    Ok(())
}
static BASE: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    vec![Migration::new(
        1,
        "base".into(),
        MigrationType::Simple,
        "CREATE TABLE records(id INTEGER PRIMARY KEY,payload TEXT)".into_sql_str(),
        false,
    )]
});
static UPGRADE: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    let mut all = BASE.clone();
    all.push(Migration::new(
        2,
        "auto".into(),
        MigrationType::Simple,
        "CREATE TABLE new_records(id INTEGER PRIMARY KEY AUTOINCREMENT,payload TEXT)"
            .into_sql_str(),
        false,
    ));
    all
});
static TABLES: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
    LazyLock::new(|| {
        ["records", "new_records"]
            .into_iter()
            .map(TableName::new)
            .collect()
    });
struct Schema {
    upgrade: Arc<AtomicBool>,
    tables: &'static [TableName],
}
impl ReplicatedSchema for Schema {
    fn local_migrations(&self) -> &'static [Migration] {
        static LOCAL: LazyLock<Vec<Migration>> = LazyLock::new(|| {
            vec![Migration::new(
                1,
                "local auto".into(),
                MigrationType::Simple,
                "CREATE TABLE local_auto(id INTEGER PRIMARY KEY AUTOINCREMENT,payload TEXT)"
                    .into_sql_str(),
                false,
            )]
        });
        &LOCAL
    }
    fn migrations(&self) -> &'static [Migration] {
        if self.upgrade.load(Ordering::SeqCst) {
            &UPGRADE
        } else {
            &BASE
        }
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        if self.upgrade.load(Ordering::SeqCst) {
            self.tables
        } else {
            self.tables.get(..1).unwrap_or_default()
        }
    }
}
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(60),
        renew_interval: Duration::from_secs(10),
    }
}
fn peer() -> TestResult<PeerId> {
    Ok(PeerId::new(Uuid::from_u128(2))?)
}
fn schema(upgrade: Arc<AtomicBool>) -> TestResult<Schema> {
    Ok(Schema {
        upgrade,
        tables: TABLES.as_ref().map(Vec::as_slice).map_err(Clone::clone)?,
    })
}
fn refused(error: &OpenError, table: &str) -> bool {
    matches!(error,OpenError::SchemaUnsupported{form:SchemaForm::Autoincrement{table:found}} if found.as_str()==table)
}
#[tokio::test]
async fn opening_managed_autoincrement_schema_refuses_before_writing_any_bytes() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let flag = Arc::new(AtomicBool::new(false));
    let writer = ReplicatedDatabase::create(
        &path,
        schema(flag.clone())?,
        peer()?,
        policy(),
        WriterOptions::default(),
    )
    .await?;
    let id = writer.database();
    writer.close().await;
    let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
    connection.execute("DROP TABLE records; CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,payload TEXT)").await?;
    connection.close().await?;
    let before = tokio::fs::read(&path).await?;
    let result = ReplicatedDatabase::open(
        &path,
        id,
        schema(flag)?,
        LeaseHolder::provisional(id, peer()?, policy()),
    )
    .await;
    let error = match result {
        Ok(writer) => {
            writer.close().await;
            return Err("AUTOINCREMENT open was admitted".into());
        }
        Err(error) => error,
    };
    assert!(refused(&error, "records"), "{error}");
    assert_eq!(tokio::fs::read(&path).await?, before);
    Ok(())
}
#[tokio::test]
async fn adding_autoincrement_rolls_back_table_migration_history_and_capture() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let flag = Arc::new(AtomicBool::new(false));
    let writer = ReplicatedDatabase::create(
        &path,
        schema(flag.clone())?,
        peer()?,
        policy(),
        WriterOptions::default(),
    )
    .await?;
    let id = writer.database();
    writer.close().await;
    let writer = ReplicatedDatabase::open(
        &path,
        id,
        schema(flag.clone())?,
        LeaseHolder::provisional(id, peer()?, policy()),
    )
    .await?;
    let before = tokio::fs::read(&path).await?;
    let capture = query::<Turso>("SELECT change_id FROM turso_cdc ORDER BY change_id")
        .fetch_all(writer.reader())
        .await?
        .len();
    flag.store(true, Ordering::SeqCst);
    let error = writer
        .migrate()
        .await
        .err()
        .ok_or("AUTOINCREMENT migration was admitted")?;
    assert!(refused(&error, "new_records"), "{error}");
    assert!(
        query::<Turso>("SELECT name FROM sqlite_schema WHERE name='new_records'")
            .fetch_optional(writer.reader())
            .await?
            .is_none()
    );
    assert_eq!(
        query::<Turso>("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(writer.reader())
            .await?
            .len(),
        1
    );
    assert_eq!(
        query::<Turso>("SELECT change_id FROM turso_cdc ORDER BY change_id")
            .fetch_all(writer.reader())
            .await?
            .len(),
        capture
    );
    writer.close().await;
    assert_eq!(tokio::fs::read(&path).await?, before);
    Ok(())
}
/// Decision45's native probe: CREATE SQL and current rows omit a deleted high-water mark.
#[tokio::test]
async fn schema_and_row_snapshot_loses_autoincrement_allocation_but_plain_rowid_is_stable()
-> TestResult {
    let mut source = TursoConnectOptions::new().connect().await?;
    source
        .execute("PRAGMA capture_data_changes_conn('full')")
        .await?;
    let mut copy = TursoConnectOptions::new().connect().await?;
    for (table, statement, expected_source, expected_copy) in [
        (
            "auto_records",
            "CREATE TABLE auto_records(id INTEGER PRIMARY KEY AUTOINCREMENT,payload TEXT)",
            101,
            1,
        ),
        (
            "ordinary_records",
            "CREATE TABLE ordinary_records(id INTEGER PRIMARY KEY,payload TEXT)",
            1,
            1,
        ),
    ] {
        source.execute(statement).await?;
        copy.execute(statement).await?;
        source
            .execute(sqlx_core::sql_str::AssertSqlSafe(format!(
                "INSERT INTO {table}(id,payload) VALUES(100,'deleted')"
            )))
            .await?;
        source
            .execute(sqlx_core::sql_str::AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE id=100"
            )))
            .await?;
        assert!(
            query::<Turso>(sqlx_core::sql_str::AssertSqlSafe(format!(
                "SELECT id FROM {table}"
            )))
            .fetch_all(&mut source)
            .await?
            .is_empty()
        );
        let insert = sqlx_core::sql_str::AssertSqlSafe(format!(
            "INSERT INTO {table}(payload) VALUES('next')"
        ))
        .into_sql_str();
        source.execute(insert.clone()).await?;
        copy.execute(insert).await?;
        let select =
            sqlx_core::sql_str::AssertSqlSafe(format!("SELECT id FROM {table}")).into_sql_str();
        assert_eq!(
            query::<Turso>(select.clone())
                .fetch_one(&mut source)
                .await?
                .try_get::<i64, _>("id")?,
            expected_source
        );
        assert_eq!(
            query::<Turso>(select)
                .fetch_one(&mut copy)
                .await?
                .try_get::<i64, _>("id")?,
            expected_copy
        );
    }
    assert_eq!(
        query::<Turso>(
            "SELECT COUNT(*) AS count FROM turso_cdc WHERE table_name='sqlite_sequence'"
        )
        .fetch_one(&mut source)
        .await?
        .try_get::<i64, _>("count")?,
        0,
        "native capture changed; revisit the sequence-state limitation"
    );
    source.close().await?;
    copy.close().await?;
    Ok(())
}
