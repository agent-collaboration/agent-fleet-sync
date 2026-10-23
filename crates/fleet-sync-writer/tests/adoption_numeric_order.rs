#![allow(clippy::panic_in_result_fn)]
use durable_steps::Uuid;
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::{PeerId, TableName};
use fleet_sync_writer::{Migration, ReplicatedDatabase, ReplicatedSchema, WriterOptions};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    migrate::{MigrationType, Migrator},
    query::query,
    row::Row,
    sql_str::{AssertSqlSafe, SqlSafeStr},
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::{sync::LazyLock, time::Duration};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

static MIGRATIONS: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    (1..=101).map(|version| {
        let sql = if version == 1 {
            "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,body TEXT)".to_owned()
        } else {
            format!("INSERT INTO records VALUES('019eef00-0000-7000-8000-{version:012x}','migration {version}')")
        };
        Migration::new(version, "ordered history".into(), MigrationType::Simple, AssertSqlSafe(sql).into_sql_str(), false)
    }).collect()
});
static TABLES: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
    LazyLock::new(|| Ok(vec![TableName::new("records")?]));
struct Schema {
    tables: &'static [TableName],
}
impl ReplicatedSchema for Schema {
    fn migrations(&self) -> &'static [Migration] {
        &MIGRATIONS
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}

#[tokio::test]
async fn adopting_a_101_version_prefix_keeps_numeric_history_and_existing_rows() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("preexisting.db");
    let mut connection = TursoConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .connect()
        .await?;
    Migrator::with_migrations(MIGRATIONS.clone())
        .run_direct(None, &mut connection, false)
        .await?;
    connection.close().await?;
    let tables = TABLES.as_ref().map_err(Clone::clone)?;
    let writer = ReplicatedDatabase::adopt(
        &path,
        Schema { tables },
        PeerId::new(Uuid::from_u128(0x019eef00000070008000000000000002))?,
        LeasePolicy {
            lease_duration: Duration::from_secs(60),
            renew_interval: Duration::from_secs(10),
        },
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    let rows = query::<Turso>("SELECT body FROM records ORDER BY id")
        .fetch_all(writer.reader())
        .await?;
    let bodies = rows
        .iter()
        .map(|row| row.try_get::<String, _>("body"))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        bodies,
        (2..=101)
            .map(|version| format!("migration {version}"))
            .collect::<Vec<_>>()
    );
    let mut snapshot = writer.snapshot().await?;
    assert_eq!(snapshot.manifest().await?.schema.version, 101);
    snapshot.close().await?;
    writer.close().await;
    Ok(())
}
