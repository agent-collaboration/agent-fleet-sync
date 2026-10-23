#![allow(clippy::panic_in_result_fn)]

use durable_steps::Uuid;
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::{PeerId, TableName};
use fleet_sync_writer::{
    Migration, OpenError, ReplicatedDatabase, ReplicatedSchema, SchemaForm, WriterOptions,
};
use runtime_primitives::SystemClock;
use sqlx_core::{
    migrate::MigrationType,
    query::query,
    row::Row,
    sql_str::{AssertSqlSafe, SqlSafeStr},
};
use sqlx_turso::Turso;
use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const PARENT_KEY: &str = "'019eef00-0000-7000-8000-000000000001'";
const MISSING_KEY: &str = "'019eef00-0000-7000-8000-000000000009'";
static TABLES: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
    LazyLock::new(|| {
        ["parents", "children"]
            .into_iter()
            .map(TableName::new)
            .collect()
    });
static BUNDLES: LazyLock<Vec<Vec<Migration>>> = LazyLock::new(|| {
    [1, 2, 98].into_iter().map(|width| {
        let parent_names = (0..width).map(|column| format!("p{column}")).collect::<Vec<_>>();
        let child_names = (0..width).map(|column| format!("c{column}")).collect::<Vec<_>>();
        let parent_definition = parent_names.iter().map(|name| format!("{name} TEXT NOT NULL"))
            .collect::<Vec<_>>().join(",");
        let child_definition = child_names.iter().map(|name| format!("{name} TEXT"))
            .collect::<Vec<_>>().join(",");
        let parent_key = parent_names.join(",");
        let child_key = child_names.join(",");
        let valid_values = vec![PARENT_KEY; width].join(",");
        let orphan_values = (0..width).map(|column| {
            if column + 1 == width { MISSING_KEY } else { PARENT_KEY }
        }).collect::<Vec<_>>().join(",");
        [
            format!("CREATE TABLE parents({parent_definition},PRIMARY KEY({parent_key})); \
                CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,{child_definition},FOREIGN KEY({child_key}) REFERENCES parents({parent_key})); \
                CREATE INDEX partial_children ON children(c0) WHERE c0 IS NOT NULL"),
            format!("INSERT INTO parents VALUES({valid_values}); \
                INSERT INTO children VALUES('019eef00-0000-7000-8000-000000000002',{valid_values})"),
            format!("INSERT INTO children VALUES('019eef00-0000-7000-8000-000000000003',{orphan_values})"),
        ].into_iter().zip(1_i64..).map(|(statement, version)| {
            Migration::new(version, "foreign key integrity".into(), MigrationType::Simple,
                AssertSqlSafe(statement).into_sql_str(), false)
        }).collect()
    }).collect()
});

#[derive(Clone)]
struct ForeignKeySchema {
    migrations: &'static [Migration],
    tables: &'static [TableName],
    offered_count: Arc<AtomicUsize>,
}

impl ReplicatedSchema for ForeignKeySchema {
    fn migrations(&self) -> &'static [Migration] {
        self.migrations
            .get(..self.offered_count.load(Ordering::SeqCst))
            .unwrap_or_default()
    }

    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}

fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(600),
        renew_interval: Duration::from_secs(30),
    }
}

async fn open_migrate_and_reject_orphan(bundle: usize) -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let peer = PeerId::new(Uuid::from_u128(0x019eef00000070008000000000000002))?;
    let schema = ForeignKeySchema {
        migrations: BUNDLES.get(bundle).ok_or("missing migration bundle")?,
        tables: TABLES.as_ref().map_err(Clone::clone)?,
        offered_count: Arc::new(AtomicUsize::new(1)),
    };
    let options = WriterOptions {
        allow_provisional_writes: true,
    };
    let database =
        ReplicatedDatabase::create(&path, schema.clone(), peer, policy(), options).await?;
    let database_id = database.database();
    database.close().await;

    schema.offered_count.store(2, Ordering::SeqCst);
    let database = ReplicatedDatabase::open_with(
        &path,
        database_id,
        schema.clone(),
        LeaseHolder::provisional(database_id, peer, policy()),
        Arc::new(SystemClock),
        options,
    )
    .await?;
    assert_eq!(
        query::<Turso>("SELECT id FROM children")
            .fetch_all(database.reader())
            .await?
            .len(),
        1
    );

    schema.offered_count.store(3, Ordering::SeqCst);
    assert!(matches!(database.migrate().await,
        Err(OpenError::SchemaUnsupported { form: SchemaForm::ForeignKeyOrphan { table, parent } })
            if table.as_str() == "children" && parent.as_str() == "parents"
    ));
    assert_eq!(
        query::<Turso>("SELECT id FROM children")
            .fetch_all(database.reader())
            .await?
            .len(),
        1
    );
    for history in ["_sqlx_migrations", "_fleet_sync_schema_steps"] {
        let row = query::<Turso>(
            AssertSqlSafe(format!("SELECT MAX(version) AS version FROM {history}")).into_sql_str(),
        )
        .fetch_one(database.reader())
        .await?;
        assert_eq!(row.try_get::<i64, _>("version")?, 2);
    }
    database.close().await;

    schema.offered_count.store(2, Ordering::SeqCst);
    let reopened = ReplicatedDatabase::open_with(
        &path,
        database_id,
        schema,
        LeaseHolder::provisional(database_id, peer, policy()),
        Arc::new(SystemClock),
        options,
    )
    .await?;
    let row = query::<Turso>("SELECT id FROM children")
        .fetch_one(reopened.reader())
        .await?;
    assert_eq!(
        row.try_get::<String, _>("id")?,
        "019eef00-0000-7000-8000-000000000002"
    );
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn single_key_open_migrate_rejects_orphans_without_committing_them() -> TestResult {
    open_migrate_and_reject_orphan(0).await
}

#[tokio::test]
async fn composite_key_open_migrate_rejects_orphans_without_committing_them() -> TestResult {
    open_migrate_and_reject_orphan(1).await
}

#[tokio::test]
async fn widest_key_open_migrate_rejects_orphans_without_committing_them() -> TestResult {
    open_migrate_and_reject_orphan(2).await
}
