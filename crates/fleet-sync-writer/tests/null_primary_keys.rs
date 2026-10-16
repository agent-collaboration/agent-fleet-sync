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
    sql_str::{AssertSqlSafe, SqlSafeStr},
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

static BASE: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    vec![Migration::new(
        1,
        "base".into(),
        MigrationType::Simple,
        "CREATE TABLE base_records(id INTEGER PRIMARY KEY,payload TEXT)".into_sql_str(),
        false,
    )]
});
fn addition(sql: &'static str) -> Vec<Migration> {
    let mut migrations = BASE.clone();
    migrations.push(Migration::new(
        2,
        "nullable addition".into(),
        MigrationType::Simple,
        sql.into_sql_str(),
        false,
    ));
    migrations
}
static TEXT: LazyLock<Vec<Migration>> =
    LazyLock::new(|| addition("CREATE TABLE new_records(id TEXT PRIMARY KEY,payload TEXT)"));
static COMPOSITE: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    addition(
        "CREATE TABLE new_records(account TEXT NOT NULL,sequence INTEGER,payload TEXT,PRIMARY KEY(account,sequence))",
    )
});
static BASE_TABLES: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
    LazyLock::new(|| ["base_records"].into_iter().map(TableName::new).collect());
static ALL_TABLES: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
    LazyLock::new(|| {
        ["base_records", "new_records"]
            .into_iter()
            .map(TableName::new)
            .collect()
    });
static NEW_TABLE: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
    LazyLock::new(|| ["new_records"].into_iter().map(TableName::new).collect());

struct FixedSchema {
    tables: &'static [TableName],
}
impl ReplicatedSchema for FixedSchema {
    fn migrations(&self) -> &'static [Migration] {
        &[]
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}
struct UpgradingSchema {
    next: Arc<AtomicBool>,
    offered: &'static [Migration],
    base_tables: &'static [TableName],
    all_tables: &'static [TableName],
}
impl ReplicatedSchema for UpgradingSchema {
    fn migrations(&self) -> &'static [Migration] {
        if self.next.load(Ordering::SeqCst) {
            self.offered
        } else {
            &BASE
        }
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        if self.next.load(Ordering::SeqCst) {
            self.all_tables
        } else {
            self.base_tables
        }
    }
}
fn peer() -> TestResult<PeerId> {
    Ok(PeerId::new(Uuid::from_u128(2))?)
}
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(60),
        renew_interval: Duration::from_secs(10),
    }
}

#[tokio::test]
async fn open_refuses_native_text_and_composite_keys_that_accept_duplicate_nulls() -> TestResult {
    for (statement, values, column) in [
        (
            "CREATE TABLE new_records(id TEXT PRIMARY KEY,payload TEXT)",
            "(NULL,'row')",
            "id",
        ),
        (
            "CREATE TABLE new_records(account TEXT NOT NULL,sequence INTEGER,payload TEXT,PRIMARY KEY(account,sequence))",
            "('account',NULL,'row')",
            "sequence",
        ),
    ] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("writer.db");
        let managed = ReplicatedDatabase::create(
            &path,
            FixedSchema { tables: &[] },
            peer()?,
            policy(),
            WriterOptions::default(),
        )
        .await?;
        let id = managed.database();
        managed.close().await;
        // Historical/adverse file fixture: the engine permits both rows, while
        // declared-key replication cannot distinguish their equal NULL keys.
        let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
        connection.execute(statement).await?;
        for _ in 0..2 {
            connection
                .execute(
                    AssertSqlSafe(format!("INSERT INTO new_records VALUES {values}"))
                        .into_sql_str(),
                )
                .await?;
        }
        assert_eq!(
            query::<Turso>("SELECT count(*) AS count FROM new_records")
                .fetch_one(&mut connection)
                .await?
                .try_get::<i64, _>("count")?,
            2
        );
        connection.close().await?;
        let result = ReplicatedDatabase::open(
            &path,
            id,
            FixedSchema {
                tables: NEW_TABLE.as_ref().map_err(Clone::clone)?,
            },
            LeaseHolder::provisional(id, peer()?, policy()),
        )
        .await;
        assert!(
            matches!(result,Err(OpenError::SchemaUnsupported{form:SchemaForm::NullablePrimaryKey{column:ref refused,..}}) if refused==column)
        );
    }
    Ok(())
}

#[tokio::test]
async fn nullable_key_migration_rolls_back_its_table_history_and_capture() -> TestResult {
    for (offered, column) in [(&*TEXT, "id"), (&*COMPOSITE, "sequence")] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("writer.db");
        let next = Arc::new(AtomicBool::new(false));
        let schema = UpgradingSchema {
            next: next.clone(),
            offered,
            base_tables: BASE_TABLES.as_ref().map_err(Clone::clone)?,
            all_tables: ALL_TABLES.as_ref().map_err(Clone::clone)?,
        };
        let database =
            ReplicatedDatabase::create(&path, schema, peer()?, policy(), WriterOptions::default())
                .await?;
        next.store(true, Ordering::SeqCst);
        assert!(
            matches!(database.migrate().await,Err(OpenError::SchemaUnsupported{form:SchemaForm::NullablePrimaryKey{column:ref refused,..}}) if refused==column)
        );
        assert!(
            query::<Turso>("SELECT name FROM sqlite_schema WHERE name='new_records'")
                .fetch_optional(database.reader())
                .await?
                .is_none()
        );
        assert!(
            query::<Turso>("SELECT version FROM _sqlx_migrations WHERE version=2")
                .fetch_optional(database.reader())
                .await?
                .is_none()
        );
        assert!(
            query::<Turso>("SELECT version FROM _fleet_sync_schema_steps WHERE version=2")
                .fetch_optional(database.reader())
                .await?
                .is_none()
        );
        assert!(
            query::<Turso>("SELECT change_id FROM turso_cdc WHERE table_name='new_records'")
                .fetch_optional(database.reader())
                .await?
                .is_none()
        );
        database.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn integer_primary_key_desc_does_not_receive_the_rowid_alias_exemption() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let managed = ReplicatedDatabase::create(
        &path,
        FixedSchema { tables: &[] },
        peer()?,
        policy(),
        WriterOptions::default(),
    )
    .await?;
    let id = managed.database();
    managed.close().await;
    let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
    connection
        .execute("CREATE TABLE new_records(id INTEGER PRIMARY KEY DESC,payload TEXT)")
        .await?;
    connection
        .execute("INSERT INTO new_records VALUES(NULL,'nullable descending key')")
        .await?;
    connection.close().await?;
    assert!(matches!(
        ReplicatedDatabase::open(
            &path,
            id,
            FixedSchema {
                tables: NEW_TABLE.as_ref().map_err(Clone::clone)?
            },
            LeaseHolder::provisional(id, peer()?, policy())
        )
        .await,
        Err(OpenError::SchemaUnsupported {
            form: SchemaForm::NullablePrimaryKey { .. }
        })
    ));
    Ok(())
}
