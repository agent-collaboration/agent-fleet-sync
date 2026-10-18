#![allow(clippy::panic_in_result_fn)]
use crate::*;
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::{PeerId, SchemaIdentity, TableName};
use sqlx_core::{
    migrate::{MigrationType, Migrator},
    sql_str::SqlSafeStr,
};
use std::{sync::LazyLock, time::Duration};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static REGULAR: Migrator = sqlx::migrate!("./tests/fixtures/regular");
static BASE_TABLES: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
    LazyLock::new(|| {
        ["text_records", "composite_records", "real_records"]
            .into_iter()
            .map(TableName::new)
            .collect()
    });
static EXTENDED_TABLES: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
    LazyLock::new(|| {
        [
            "text_records",
            "composite_records",
            "real_records",
            "later_records",
        ]
        .into_iter()
        .map(TableName::new)
        .collect()
    });
static LATER: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    let mut migrations = REGULAR.iter().cloned().collect::<Vec<_>>();
    migrations.push(Migration::new(
        3,
        "later schema".into(),
        MigrationType::Simple,
        "CREATE TABLE later_records(id TEXT NOT NULL PRIMARY KEY, value TEXT)".into_sql_str(),
        false,
    ));
    migrations
});
static DIFFERENT: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    vec![Migration::new(1, "different prefix".into(), MigrationType::Simple, "CREATE TABLE text_records(id TEXT NOT NULL PRIMARY KEY, different TEXT); CREATE TABLE composite_records(left_id TEXT NOT NULL, right_id TEXT NOT NULL, payload BLOB, PRIMARY KEY(left_id,right_id)); CREATE TABLE real_records(id TEXT NOT NULL PRIMARY KEY, ratio REAL)".into_sql_str(), false), Migration::new(2, "different backfill".into(), MigrationType::Simple, "UPDATE text_records SET different='backfilled'".into_sql_str(), false)]
});
struct Schema {
    migrations: &'static [Migration],
    tables: &'static [TableName],
}
impl ReplicatedSchema for Schema {
    fn migrations(&self) -> &'static [Migration] {
        self.migrations
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}
fn schema(migrations: &'static [Migration], extended: bool) -> TestResult<Schema> {
    Ok(Schema {
        migrations,
        tables: if extended {
            EXTENDED_TABLES.as_ref().map_err(Clone::clone)?
        } else {
            BASE_TABLES.as_ref().map_err(Clone::clone)?
        },
    })
}
async fn writer(path: &std::path::Path, schema: Schema) -> TestResult<ReplicatedDatabase<Schema>> {
    Ok(ReplicatedDatabase::create(
        path,
        schema,
        PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000002,
        ))?,
        LeasePolicy {
            lease_duration: Duration::from_secs(60),
            renew_interval: Duration::from_secs(10),
        },
        WriterOptions::default(),
    )
    .await?)
}
async fn identity(writer: &ReplicatedDatabase<Schema>) -> TestResult<SchemaIdentity> {
    Ok(writer.snapshot().await?.schema().clone())
}
async fn images(path: &std::path::Path) -> TestResult<(Vec<u8>, Vec<u8>)> {
    let wal = match tokio::fs::read(format!("{}-wal", path.display())).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    Ok((tokio::fs::read(path).await?, wal))
}
#[tokio::test]
async fn bootstrap_checks_exact_prefix_including_schema_before_later_declared_tables_exist()
-> TestResult {
    let root = tempfile::tempdir()?;
    let source = writer(
        &root.path().join("source.db"),
        schema(REGULAR.iter().as_slice(), false)?,
    )
    .await?;
    let expected = identity(&source).await?;
    let target = writer(&root.path().join("target.db"), schema(&LATER, true)?).await?;
    let before = images(target.path()).await?;
    target.validate_bootstrap_schema(&expected).await?;
    let empty = SchemaIdentity::new(0, fleet_sync_protocol::schema_fingerprint(&[])?)?;
    target.validate_bootstrap_schema(&empty).await?;
    assert_eq!(images(target.path()).await?, before);
    assert_eq!(
        identity(&target).await?.version,
        3,
        "scratch validation cannot replace target history"
    );
    target.close().await;
    source.close().await;
    Ok(())
}
#[tokio::test]
async fn bootstrap_refuses_different_prefix_and_unsupported_version_without_touching_target()
-> TestResult {
    let root = tempfile::tempdir()?;
    let source = writer(
        &root.path().join("source.db"),
        schema(REGULAR.iter().as_slice(), false)?,
    )
    .await?;
    let expected = identity(&source).await?;
    let target = writer(&root.path().join("target.db"), schema(&DIFFERENT, false)?).await?;
    let before = images(target.path()).await?;
    assert!(
        matches!(target.validate_bootstrap_schema(&expected).await, Err(OpenError::BootstrapSchemaMismatch { expected: actual, found }) if actual == expected && found.version == expected.version && found.fingerprint != expected.fingerprint)
    );
    let unsupported = SchemaIdentity::new(7, expected.fingerprint.clone())?;
    assert!(matches!(
        target.validate_bootstrap_schema(&unsupported).await,
        Err(OpenError::BootstrapVersionUnsupported { version: 7 })
    ));
    assert_eq!(images(target.path()).await?, before);
    target.close().await;
    source.close().await;
    Ok(())
}
