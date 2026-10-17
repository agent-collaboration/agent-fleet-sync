#![allow(clippy::panic_in_result_fn)]
use durable_steps::Uuid;
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::*;
use fleet_sync_writer::*;
use runtime_primitives::{Clock, SystemClock};
use sqlx_core::{migrate::MigrationType, query::query, row::Row, sql_str::SqlSafeStr};
use sqlx_turso::Turso;
use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const KEY: &str = "019eef00-0000-7000-8000-000000000001";
const RECORDS: &str = "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,value TEXT); INSERT INTO records VALUES('019eef00-0000-7000-8000-000000000001','before')";
static BUNDLES: LazyLock<Vec<Vec<Migration>>> = LazyLock::new(|| {
    [
    vec![RECORDS, "ALTER TABLE records RENAME TO tasks"],
    vec!["CREATE TABLE a(id TEXT NOT NULL PRIMARY KEY,value TEXT); INSERT INTO a VALUES('019eef00-0000-7000-8000-000000000001','before')", "ALTER TABLE a RENAME TO b", "ALTER TABLE b RENAME TO c"],
    vec![RECORDS, "ALTER TABLE local_notes RENAME TO tasks"],
    vec![RECORDS, "ALTER TABLE records RENAME TO tasks"],
    vec![RECORDS, "ALTER TABLE records RENAME TO tasks; UPDATE tasks SET id=id"],
    vec!["CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,value TEXT); CREATE TABLE unrelated(id TEXT NOT NULL PRIMARY KEY)", "ALTER TABLE records RENAME TO tasks", "DROP TABLE unrelated"],
].into_iter().map(migrations).collect()
});
static TABLES: LazyLock<Result<Vec<Vec<Vec<TableName>>>, TextValueError>> = LazyLock::new(|| {
    [
        vec!["records", "tasks"],
        vec!["a", "b", "c"],
        vec!["records", "tasks"],
        vec!["records", "tasks"],
        vec!["records", "tasks"],
        vec!["records", "tasks", "tasks"],
    ]
    .into_iter()
    .map(|names| {
        names
            .into_iter()
            .map(|name| Ok(vec![TableName::new(name)?]))
            .collect()
    })
    .collect()
});
static LOCAL_NOTES: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    migrations(vec![
        "CREATE TABLE local_notes(id TEXT NOT NULL PRIMARY KEY,value TEXT); INSERT INTO local_notes VALUES('local','private')",
    ])
});
static LOCAL_CONFLICT: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    migrations(vec![
        "CREATE TABLE tasks(id TEXT NOT NULL PRIMARY KEY,value TEXT); INSERT INTO tasks VALUES('local','private')",
    ])
});
static LOCAL_HISTORY: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    vec![
    Migration::new(1, "local create".into(), MigrationType::Simple, "CREATE TABLE private_history(id TEXT PRIMARY KEY); INSERT INTO private_history VALUES('private')".into_sql_str(), false),
    Migration::new(3, "local drop".into(), MigrationType::Simple, "DROP TABLE private_history".into_sql_str(), false),
]
});
fn migrations(statements: Vec<&'static str>) -> Vec<Migration> {
    statements
        .into_iter()
        .zip(1_i64..)
        .map(|(sql, version)| {
            Migration::new(
                version,
                "membership".into(),
                MigrationType::Simple,
                sql.into_sql_str(),
                false,
            )
        })
        .collect()
}
#[derive(Clone)]
struct MembershipSchema {
    migrations: &'static [Migration],
    tables: &'static [Vec<TableName>],
    count: Arc<AtomicUsize>,
    local: &'static [Migration],
    local_history: bool,
}
impl ReplicatedSchema for MembershipSchema {
    fn migrations(&self) -> &'static [Migration] {
        self.migrations
            .get(..self.count.load(Ordering::SeqCst))
            .unwrap_or_default()
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
            .get(self.count.load(Ordering::SeqCst).saturating_sub(1))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
    fn local_migrations(&self) -> &'static [Migration] {
        if self.local_history {
            LOCAL_HISTORY
                .get(
                    ..if self.count.load(Ordering::SeqCst) < 3 {
                        1
                    } else {
                        2
                    },
                )
                .unwrap_or_default()
        } else {
            self.local
        }
    }
}
fn schema(index: usize, count: usize) -> TestResult<MembershipSchema> {
    Ok(MembershipSchema {
        migrations: BUNDLES.get(index).ok_or("bundle missing")?,
        tables: TABLES
            .as_ref()
            .map_err(Clone::clone)?
            .get(index)
            .ok_or("tables missing")?,
        count: Arc::new(AtomicUsize::new(count)),
        local: match index {
            2 => &LOCAL_NOTES,
            3 => &LOCAL_CONFLICT,
            _ => &[],
        },
        local_history: false,
    })
}
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(600),
        renew_interval: Duration::from_secs(30),
    }
}
fn peer() -> TestResult<PeerId> {
    Ok(PeerId::new(Uuid::from_u128(2))?)
}
async fn create(
    path: &std::path::Path,
    schema: MembershipSchema,
) -> TestResult<ReplicatedDatabase<MembershipSchema>> {
    Ok(ReplicatedDatabase::create(
        path,
        schema,
        peer()?,
        policy(),
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?)
}
async fn write_and_check(
    database: &ReplicatedDatabase<MembershipSchema>,
    table: &'static str,
) -> TestResult {
    let mut write = database.begin_write().await?;
    let sql = if table == "tasks" {
        "UPDATE tasks SET value='after' WHERE id=?"
    } else {
        "UPDATE c SET value='after' WHERE id=?"
    };
    query::<Turso>(sql).bind(KEY).execute(&mut write).await?;
    write.commit().await?;
    let sql = if table == "tasks" {
        "SELECT id,value FROM tasks"
    } else {
        "SELECT id,value FROM c"
    };
    let row = query::<Turso>(sql).fetch_one(database.reader()).await?;
    assert_eq!(row.try_get::<String, _>("id")?, KEY);
    assert_eq!(row.try_get::<String, _>("value")?, "after");
    Ok(())
}
#[tokio::test]
async fn rename_uses_committed_membership_and_allows_the_next_write() -> TestResult {
    let root = tempfile::tempdir()?;
    let schema = schema(0, 1)?;
    let database = create(&root.path().join("writer.db"), schema.clone()).await?;
    schema.count.store(2, Ordering::SeqCst);
    database.migrate().await?;
    write_and_check(&database, "tasks").await?;
    database.close().await;
    Ok(())
}
#[tokio::test]
async fn reopen_with_the_new_bundle_restores_old_committed_membership() -> TestResult {
    let root = tempfile::tempdir()?;
    let path = root.path().join("writer.db");
    let old = create(&path, schema(0, 1)?).await?;
    let id = old.database();
    old.close().await;
    let database = ReplicatedDatabase::open_with(
        &path,
        id,
        schema(0, 2)?,
        LeaseHolder::provisional(id, peer()?, policy()),
        Arc::new(SystemClock),
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    write_and_check(&database, "tasks").await?;
    database.close().await;
    Ok(())
}
#[tokio::test]
async fn fresh_full_bundle_preserves_the_renamed_table_and_row() -> TestResult {
    let root = tempfile::tempdir()?;
    let database = create(&root.path().join("writer.db"), schema(0, 2)?).await?;
    write_and_check(&database, "tasks").await?;
    database.close().await;
    Ok(())
}
#[tokio::test]
async fn fresh_rename_chain_reconstructs_each_intermediate_membership() -> TestResult {
    let root = tempfile::tempdir()?;
    let database = create(&root.path().join("writer.db"), schema(1, 3)?).await?;
    write_and_check(&database, "c").await?;
    database.close().await;
    Ok(())
}
#[tokio::test]
async fn new_writer_can_validate_its_pre_rename_bootstrap_prefix() -> TestResult {
    let root = tempfile::tempdir()?;
    let source = create(&root.path().join("source.db"), schema(0, 1)?).await?;
    let mut snapshot = source.snapshot().await?;
    let manifest = snapshot.manifest().await?;
    snapshot.close().await?;
    let now = SystemClock.now();
    let seeded = ReplicatedDatabase::seeding(
        &root.path().join("seed.db"),
        schema(0, 2)?,
        BootstrapGrant {
            lease: LeaseStatus {
                database: source.database(),
                holder: peer()?,
                epoch: Epoch::new(2)?,
                state: LeaseState::Held,
                expires_at: now.after(Duration::from_secs(600)),
                remaining_ms: 600_000,
            },
            request_started: now,
            start: manifest.position,
        },
        policy(),
    )
    .await?;
    seeded.validate_bootstrap_schema(&manifest.schema).await?;
    seeded.close().await;
    source.close().await;
    Ok(())
}
#[tokio::test]
async fn local_catalog_table_cannot_be_promoted_by_a_future_rename() -> TestResult {
    refused(2).await
}
#[tokio::test]
async fn rename_cannot_overwrite_an_existing_local_destination() -> TestResult {
    refused(3).await
}
#[tokio::test]
async fn renamed_keys_remain_immutable_and_the_migration_rolls_back() -> TestResult {
    refused(4).await
}
async fn refused(index: usize) -> TestResult {
    let root = tempfile::tempdir()?;
    let schema = schema(index, 1)?;
    let database = create(&root.path().join("writer.db"), schema.clone()).await?;
    schema.count.store(2, Ordering::SeqCst);
    let result = database.migrate().await;
    match index {
        2 => assert!(matches!(result, Err(OpenError::SchemaUnsupported {
            form: SchemaForm::UndeclaredTable { table }
        }) if table.as_str() == "local_notes")),
        3 => assert!(matches!(result, Err(OpenError::SchemaUnsupported {
            form: SchemaForm::InvalidMigration { detail, .. }
        }) if detail == "table rename destination already exists")),
        4 => assert!(matches!(result, Err(OpenError::WriteRefused {
            refusal: WriteRefusal::KeyChange { table }
        }) if table == "tasks")),
        _ => return Err("unknown refusal fixture".into()),
    }
    let row = query::<Turso>("SELECT id,value FROM records")
        .fetch_one(database.reader())
        .await?;
    assert_eq!(row.try_get::<String, _>("id")?, KEY);
    assert_eq!(row.try_get::<String, _>("value")?, "before");
    assert_eq!(
        query::<Turso>("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(database.reader())
            .await?
            .len(),
        1
    );
    assert_eq!(
        query::<Turso>("SELECT version FROM _fleet_sync_schema_steps ORDER BY version")
            .fetch_all(database.reader())
            .await?
            .len(),
        1
    );
    if index == 3 {
        assert_eq!(
            query::<Turso>("SELECT value FROM tasks")
                .fetch_one(database.reader())
                .await?
                .try_get::<String, _>("value")?,
            "private"
        );
    }
    database.close().await;
    Ok(())
}
#[tokio::test]
async fn unrelated_create_then_later_drop_is_not_a_declared_lineage() -> TestResult {
    let root = tempfile::tempdir()?;
    let result = ReplicatedDatabase::create(
        &root.path().join("writer.db"),
        schema(5, 3)?,
        peer()?,
        policy(),
        WriterOptions::default(),
    )
    .await;
    assert!(matches!(
        result,
        Err(OpenError::SchemaUnsupported {
            form: SchemaForm::UndeclaredTable { .. }
        })
    ));
    Ok(())
}
#[tokio::test]
async fn local_v1_table_dropped_in_v3_never_enters_capture_or_schema_steps() -> TestResult {
    let root = tempfile::tempdir()?;
    let mut schema = schema(1, 1)?;
    schema.local_history = true;
    let database = create(&root.path().join("writer.db"), schema.clone()).await?;
    assert_eq!(
        query::<Turso>("SELECT id FROM private_history")
            .fetch_all(database.reader())
            .await?
            .len(),
        1
    );
    schema.count.store(3, Ordering::SeqCst);
    database.migrate().await?;
    assert!(
        query::<Turso>("SELECT name FROM sqlite_schema WHERE name='private_history'")
            .fetch_all(database.reader())
            .await?
            .is_empty()
    );
    assert!(
        query::<Turso>("SELECT change_id FROM turso_cdc WHERE table_name='private_history'")
            .fetch_all(database.reader())
            .await?
            .is_empty()
    );
    for row in query::<Turso>("SELECT statements FROM _fleet_sync_schema_steps")
        .fetch_all(database.reader())
        .await?
    {
        assert!(
            !row.try_get::<String, _>("statements")?
                .contains("private_history")
        );
    }
    write_and_check(&database, "c").await?;
    database.close().await;
    Ok(())
}
