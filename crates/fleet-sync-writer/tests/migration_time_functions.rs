#![allow(clippy::panic_in_result_fn)]
use durable_steps::Uuid;
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::{PeerId, TableName, TextValueError};
use fleet_sync_writer::{
    Migration, OpenError, ReplicatedDatabase, ReplicatedSchema, SchemaForm, WriterOptions,
};
use sqlx_core::{migrate::MigrationType, query::query, row::Row, sql_str::SqlSafeStr};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::{sync::LazyLock, time::Duration};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static TABLES: LazyLock<Result<Vec<TableName>, TextValueError>> =
    LazyLock::new(|| Ok(vec![TableName::new("records")?]));
static MIGRATIONS: LazyLock<[Vec<Migration>; 4]> = LazyLock::new(|| {
    [
    "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,stamped TEXT); INSERT INTO records VALUES('019eef00-0000-7000-8000-000000000001',datetime('now'));",
    "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,stamped TEXT); INSERT INTO records VALUES('019eef00-0000-7000-8000-000000000001',datetime((('now'))));",
    "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,stamped TEXT); INSERT INTO records VALUES('019eef00-0000-7000-8000-000000000001',datetime(('n')||('o'||'w')));",
    "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,stamped TEXT); INSERT INTO records VALUES('019eef00-0000-7000-8000-000000000001',datetime('2020-01-01'));",
].map(|sql| vec![Migration::new(1,"date expression".into(),MigrationType::Simple,sql.into_sql_str(),false)])
});
enum ClockMigration {
    DirectNow,
    GroupedNow,
    ConcatenatedNow,
    FixedTime,
}
impl ClockMigration {
    fn index(&self) -> usize {
        match self {
            Self::DirectNow => 0,
            Self::GroupedNow => 1,
            Self::ConcatenatedNow => 2,
            Self::FixedTime => 3,
        }
    }
}
struct ClockSchema {
    migrations: &'static [Migration],
    tables: &'static [TableName],
}
impl ReplicatedSchema for ClockSchema {
    fn migrations(&self) -> &'static [Migration] {
        self.migrations
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}
async fn migration_case(case: ClockMigration, allowed: bool) -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let migrations = MIGRATIONS
        .get(case.index())
        .ok_or("test migration missing")?;
    let schema = ClockSchema {
        migrations,
        tables: TABLES.as_ref().map_err(Clone::clone)?,
    };
    let result = ReplicatedDatabase::create(
        &path,
        schema,
        PeerId::new(Uuid::from_u128(2))?,
        LeasePolicy {
            lease_duration: Duration::from_secs(600),
            renew_interval: Duration::from_secs(30),
        },
        WriterOptions::default(),
    )
    .await;
    if allowed {
        let database = result?;
        assert_eq!(
            query::<Turso>("SELECT stamped FROM records")
                .fetch_one(database.reader())
                .await?
                .try_get::<String, _>("stamped")?,
            "2020-01-01 00:00:00"
        );
        database.close().await;
    } else {
        assert!(
            matches!(
                result,
                Err(OpenError::SchemaUnsupported {
                    form: SchemaForm::NonDeterministicMigration { .. }
                })
            ),
            "nondeterministic migration was admitted"
        );
        let raw = sqlx_core::pool::PoolOptions::<Turso>::new()
            .connect_with(TursoConnectOptions::new().filename(&path))
            .await?;
        assert!(
            query::<Turso>("SELECT name FROM sqlite_schema WHERE name='records'")
                .fetch_all(&raw)
                .await?
                .is_empty()
        );
        for sql in [
            "SELECT version FROM _sqlx_migrations",
            "SELECT version FROM _fleet_sync_schema_steps",
            "SELECT change_id FROM turso_cdc WHERE table_name IN ('records','_sqlx_migrations','_fleet_sync_schema_steps')",
        ] {
            assert!(
                query::<Turso>(sql).fetch_all(&raw).await?.is_empty(),
                "{sql}"
            );
        }
        raw.close().await;
    }
    Ok(())
}
#[tokio::test]
async fn direct_now_refuses_before_schema_history_or_capture() -> TestResult {
    migration_case(ClockMigration::DirectNow, false).await
}
#[tokio::test]
async fn grouped_now_refuses_before_schema_history_or_capture() -> TestResult {
    migration_case(ClockMigration::GroupedNow, false).await
}
#[tokio::test]
async fn concatenated_now_refuses_before_schema_history_or_capture() -> TestResult {
    migration_case(ClockMigration::ConcatenatedNow, false).await
}
#[tokio::test]
async fn fixed_time_literal_stays_deterministic_and_allowed() -> TestResult {
    migration_case(ClockMigration::FixedTime, true).await
}
