#![allow(clippy::panic_in_result_fn)]
use crate::{
    writer_bootstrap_fixture::{NoCalls, TestHost, policy},
    *,
};
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_protocol::*;
use fleet_sync_receiver::{
    AppliedPiece, HookError, ReceiverRules, SnapshotInstall, SnapshotInstallRules,
};
use fleet_sync_writer::{Migration, ReplicatedDatabase, ReplicatedSchema, WriterOptions};
use runtime_primitives::SystemClock;
use sqlx_core::{
    migrate::MigrationType,
    query::query,
    row::Row,
    sql_str::{AssertSqlSafe, SqlSafeStr},
};
use std::sync::{Arc, LazyLock};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const TABLE: &str = "CREATE TABLE records (id TEXT NOT NULL PRIMARY KEY, body TEXT)";
const INDEX: &str = "CREATE INDEX records_body ON records (body)";
const VIEW: &str = "CREATE VIEW records_view AS SELECT id, body FROM records";
static TABLES: LazyLock<Result<Vec<TableName>, TextValueError>> =
    LazyLock::new(|| Ok(vec![TableName::new("records")?]));
static MIGRATIONS: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    let mut migrations = vec![Migration::new(1, "catalog".into(), MigrationType::Simple,
        AssertSqlSafe(format!("{TABLE}; {INDEX}; {VIEW}; INSERT INTO records VALUES('019eef00-0000-7000-8000-000000000001','catalog payload')")).into_sql_str(), false)];
    for version in 2..=13 {
        migrations.push(Migration::new(version, "retired schema".into(), MigrationType::Simple,
            AssertSqlSafe(format!("CREATE TABLE discarded_{version}(id TEXT NOT NULL PRIMARY KEY,payload TEXT DEFAULT '{}'); DROP TABLE discarded_{version}", "x".repeat(32 * 1024))).into_sql_str(), false));
    }
    migrations
});
static LOCAL: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    vec![Migration::new(
    1, "writer local".into(), MigrationType::Simple,
    "CREATE TABLE writer_local(value TEXT); CREATE INDEX writer_local_value ON writer_local(value); INSERT INTO writer_local VALUES('writer private')".into_sql_str(), false,
)]
});
static HOOK: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    vec![Migration::new(
    1, "copy local".into(), MigrationType::Simple,
    "CREATE TABLE hook_local(value TEXT); CREATE INDEX hook_local_value ON hook_local(value); INSERT INTO hook_local VALUES('hook private')".into_sql_str(), false,
)]
});
struct Schema {
    tables: &'static [TableName],
}
impl ReplicatedSchema for Schema {
    fn migrations(&self) -> &'static [Migration] {
        &MIGRATIONS
    }
    fn local_migrations(&self) -> &'static [Migration] {
        &LOCAL
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}
struct Hook;
impl<S: TransactionScope> ReceiverRules<S> for Hook {
    type Protocol = NoConsumer;
    type FollowUp = ();
    fn local_migrations(&self) -> &'static [Migration] {
        &HOOK
    }
    async fn on_applied(&self, _: &mut S, _: &AppliedPiece<'_, ()>) -> Result<Vec<()>, HookError> {
        Ok(Vec::new())
    }
}

async fn copy(
    objects: &ObjectStore<TestHost>,
    name: &str,
) -> TestResult<Arc<TokioObject<TestHost>>> {
    let target = objects
        .object(
            ObjectKey::new(name)?,
            ObjectOptions {
                foreign_keys: false,
            },
            Arc::new(TokioHost::new(Arc::new(NoCalls), SystemClock)),
        )
        .await?;
    target
        .transaction(|scope| {
            Box::pin(async move {
                let mut migrator = sqlx::migrate!("../fleet-sync-receiver/migrations");
                migrator.dangerous_set_table_name("_fleet_sync_copy_migrations");
                migrator
                    .run_direct(None, scope.executor(), false)
                    .await
                    .map_err(StoreError::backend)?;
                fleet_sync_receiver::initialize_hook(scope, &Hook)
                    .await
                    .map_err(StoreError::backend)?;
                Ok(())
            })
        })
        .await?;
    Ok(target)
}

async fn seed_pages(
    store: &StagingStore,
    target: &TokioObject<TestHost>,
    status: &CopyStatus,
    pages: &[SnapshotPage],
    manifest: &SnapshotManifest,
) -> TestResult {
    let install = SnapshotInstall {
        install_id: InstallId::new(durable_steps::Uuid::now_v7())?,
        database: status.database,
        position: status.acked,
        schema: status.schema.clone(),
    };
    store.prepare(&install, status).await?;
    let mut cursor = None;
    for (index, page) in pages.iter().enumerate() {
        store
            .stage_page(&install, u32::try_from(index)?, cursor.as_ref(), page)
            .await?;
        cursor = page.next.clone();
    }
    assert!(cursor.is_none());
    store.verify(&install, manifest).await?;
    store.publish(&install, target).await?;
    target
        .transaction(move |scope| {
            Box::pin(async move {
                SnapshotInstallRules(scope)
                    .mark_ready(&install, &Hook)
                    .await
                    .map_err(StoreError::backend)?;
                Ok(())
            })
        })
        .await?;
    Ok(())
}

async fn copy_pages(
    copy: &TokioObject<TestHost>,
) -> TestResult<(CopyStatus, Vec<SnapshotPage>, SnapshotManifest)> {
    Ok(copy
        .transaction(|scope| {
            Box::pin(async move {
                let CopyStatusAnswer::Status(status) = fleet_sync_receiver::copy_status(scope)
                    .await
                    .map_err(StoreError::backend)?
                else {
                    return Err(StoreError::backend("copy is not readable"));
                };
                let mut cursor = None;
                let mut pages = Vec::new();
                loop {
                    let SnapshotRead::Page(page) =
                        fleet_sync_receiver::snapshot_page(scope, cursor)
                            .await
                            .map_err(StoreError::backend)?
                    else {
                        return Err(StoreError::backend(
                            "snapshot moved inside owned transaction",
                        ));
                    };
                    cursor = page.next.clone();
                    pages.push(*page);
                    if cursor.is_none() {
                        break;
                    }
                }
                let SnapshotManifestAnswer::Manifest(manifest) =
                    fleet_sync_receiver::snapshot_manifest(scope, status.acked)
                        .await
                        .map_err(StoreError::backend)?
                else {
                    return Err(StoreError::backend(
                        "manifest moved inside owned transaction",
                    ));
                };
                Ok((status, pages, manifest))
            })
        })
        .await?)
}

fn assert_current_header(pages: &[SnapshotPage]) -> TestResult {
    for page in pages {
        assert_eq!(page.schema_steps, [TABLE, INDEX, VIEW]);
        assert!(serde_json::to_vec(&page.schema_steps)?.len() < 1024);
        assert_eq!(
            page.table, "records",
            "a view or private table became a row source"
        );
    }
    Ok(())
}

#[tokio::test]
async fn writer_and_copy_catalogs_seed_the_same_fingerprint_without_private_objects() -> TestResult
{
    let root = tempfile::tempdir()?;
    let holder = PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000002,
    ))?;
    let source = ReplicatedDatabase::create(
        &root.path().join("writer.db"),
        Schema {
            tables: TABLES.as_ref().map_err(Clone::clone)?,
        },
        holder,
        policy(),
        WriterOptions::default(),
    )
    .await?;
    let history: i64 = query::<sqlx_turso::Turso>(
        "SELECT SUM(length(statements)) AS bytes FROM _fleet_sync_schema_steps",
    )
    .fetch_one(source.reader())
    .await?
    .try_get("bytes")?;
    assert!(
        history > 256 * 1024,
        "fixture did not retain enough retired history"
    );
    let private: i64 = query::<sqlx_turso::Turso>("SELECT count(*) AS objects FROM sqlite_schema WHERE name IN ('writer_local','writer_local_value','_fleet_sync_writer')")
        .fetch_one(source.reader()).await?.try_get("objects")?;
    assert_eq!(private, 3, "writer private owners must really exist");
    let mut snapshot = source.snapshot().await?;
    let mut pages = Vec::new();
    let mut cursor = None;
    loop {
        let page = snapshot.page(cursor).await?;
        cursor = page.next.clone();
        pages.push(page);
        if cursor.is_none() {
            break;
        }
    }
    assert_current_header(&pages)?;
    let manifest = snapshot.manifest().await?;
    let status = CopyStatus {
        database: source.database(),
        role: CopyRole::Authoritative,
        fence: snapshot.position().epoch,
        holder: Some(holder),
        start: snapshot.position(),
        acked: snapshot.position(),
        schema: snapshot.schema().clone(),
    };
    snapshot.close().await?;
    let objects = ObjectStore::open(root.path().join("objects")).await?;
    let store = StagingStore::open(root.path().join("staging")).await?;
    let relay = copy(&objects, "relay").await?;
    seed_pages(&store, &relay, &status, &pages, &manifest).await?;
    let private = relay.transaction(|scope| Box::pin(async move {
        query::<sqlx_turso::Turso>("SELECT count(*) AS objects FROM sqlite_schema WHERE name IN ('hook_local','hook_local_value','_fleet_sync_copy')")
            .fetch_one(scope.executor()).await?.try_get::<i64, _>("objects").map_err(StoreError::backend)
    })).await?;
    assert_eq!(private, 3, "hook private owners must really exist");
    let (copy_status, from_copy, copy_manifest) = copy_pages(&relay).await?;
    assert_current_header(&from_copy)?;
    assert_eq!(copy_manifest, manifest);
    assert_eq!(from_copy, pages);
    let writer_seed = copy(&objects, "writer-seed").await?;
    let copy_seed = copy(&objects, "copy-seed").await?;
    seed_pages(&store, &writer_seed, &status, &pages, &manifest).await?;
    seed_pages(&store, &copy_seed, &copy_status, &from_copy, &copy_manifest).await?;
    let (writer_status, writer_pages, writer_manifest) = copy_pages(&writer_seed).await?;
    let (copy_status, copy_pages, copy_manifest) = copy_pages(&copy_seed).await?;
    let expected = schema_fingerprint(&[
        SchemaEntry {
            kind: SchemaObjectKind::Table,
            name: "records".into(),
            sql: TABLE.into(),
        },
        SchemaEntry {
            kind: SchemaObjectKind::Index,
            name: "records_body".into(),
            sql: INDEX.into(),
        },
        SchemaEntry {
            kind: SchemaObjectKind::View,
            name: "records_view".into(),
            sql: VIEW.into(),
        },
    ])?;
    assert_eq!(writer_status.schema.fingerprint, expected);
    assert_eq!(copy_status.schema.fingerprint, expected);
    assert_eq!(writer_manifest, copy_manifest);
    assert_eq!(writer_pages, copy_pages);
    assert_current_header(&writer_pages)?;
    assert_eq!(
        writer_pages
            .iter()
            .flat_map(|page| &page.rows)
            .collect::<Vec<_>>(),
        vec![&vec![
            SqlValue::Text("019eef00-0000-7000-8000-000000000001".into()),
            SqlValue::Text("catalog payload".into())
        ]]
    );
    writer_seed.close().await?;
    copy_seed.close().await?;
    relay.close().await?;
    source.close().await;
    Ok(())
}
