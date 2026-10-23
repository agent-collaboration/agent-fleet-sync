//! Disposable follower generations use real files and the guarded SQL surface.
#![allow(clippy::panic_in_result_fn)]
use crate::{staging_store_tests::snapshot, *};
use durable_steps::{AtomicStore, CallFailure, NotSentReason, StoreError};
use fleet_sync_protocol::*;
use fleet_sync_receiver::SnapshotInstall;
use runtime_primitives::SystemClock;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    row::Row,
};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn assert_generation_removed(path: &Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut file = path.as_os_str().to_owned();
        file.push(suffix);
        assert!(
            !Path::new(&file).exists(),
            "released generation left {suffix:?}"
        );
    }
}
#[derive(Default)]
struct NoCalls {
    calls: AtomicUsize,
}
impl CallRouter for NoCalls {
    async fn dispatch(&self, _: CallEnvelope) -> Result<serde_json::Value, CallFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(CallFailure::NotSent {
            reason: NotSentReason::Overloaded,
        })
    }
}
type ReadHost = TokioHost<NoCalls>;

struct Fixture {
    objects: ObjectStore<ReadHost>,
    coordinator: Arc<InstallCoordinator<ReadHost>>,
    host: Arc<ReadHost>,
    router: Arc<NoCalls>,
}
impl Fixture {
    async fn open(root: &Path) -> TestResult<Self> {
        let objects = ObjectStore::open(root.join("generations")).await?;
        let router = Arc::new(NoCalls::default());
        let host = Arc::new(TokioHost::new(router.clone(), SystemClock));
        let control = objects
            .object(
                ObjectKey::new("control")?,
                ObjectOptions::default(),
                host.clone(),
            )
            .await?;
        let staging = Arc::new(StagingStore::open(root.join("staging")).await?);
        let coordinator = Arc::new(InstallCoordinator::open(control, staging).await?);
        Ok(Self {
            objects,
            coordinator,
            host,
            router,
        })
    }

    async fn install(
        &self,
        name: &str,
        sequence: u64,
        value: &str,
        verified: bool,
    ) -> TestResult<(Arc<TokioObject<ReadHost>>, SnapshotInstall)> {
        let target = self
            .objects
            .object(
                ObjectKey::new(name)?,
                ObjectOptions {
                    foreign_keys: false,
                },
                self.host.clone(),
            )
            .await?;
        target
            .transaction(|scope| {
                Box::pin(async move {
                    use durable_steps::storage::TransactionScope;
                    let mut migrator = sqlx::migrate!("../fleet-sync-receiver/migrations");
                    migrator.dangerous_set_table_name("_fleet_sync_copy_migrations");
                    migrator
                        .run_direct(None, scope.executor(), false)
                        .await
                        .map_err(StoreError::backend)
                })
            })
            .await?;
        let (mut install, mut source, mut page, mut manifest) = snapshot()?;
        install.position.sequence = CommitSequence::new(sequence);
        source.acked = install.position;
        page.position = install.position;
        manifest.position = install.position;
        *page
            .rows
            .first_mut()
            .ok_or("missing row")?
            .get_mut(1)
            .ok_or("missing value")? = SqlValue::Text(value.into());
        let mut digest = TableDigestBuilder::new();
        digest.push_row(page.rows.first().ok_or("missing row")?)?;
        manifest.tables.first_mut().ok_or("missing table")?.digest = digest.finish();
        self.coordinator
            .start(
                InstallRequest {
                    attempt: InstallAttemptId::new(install.install_id.as_uuid())?,
                    target: target.key().clone(),
                    database: install.database,
                    source: ReceiverId::new(durable_steps::Uuid::from_u128(
                        0x019eef0000007000800000000000000a,
                    ))?,
                },
                &target,
            )
            .await?;
        self.coordinator.pin(install.clone()).await?;
        self.coordinator
            .prepare(install.install_id, &source)
            .await?;
        self.coordinator
            .fetch_page(install.install_id, 0, None, move || async {
                Ok(SnapshotRead::Page(Box::new(page)))
            })
            .await?;
        if verified {
            self.coordinator
                .verify_from(install.install_id, move || async {
                    Ok(SnapshotManifestAnswer::Manifest(manifest))
                })
                .await?;
            self.coordinator
                .publish(install.install_id, &target)
                .await?;
            self.coordinator
                .mark_read_copy_ready(install.install_id, &target)
                .await?;
        }
        Ok((target, install))
    }
}

#[tokio::test]
async fn verified_read_copy_has_typed_metadata_guarded_reads_and_no_writer_state() -> TestResult {
    let root = tempfile::tempdir()?;
    let fixture = Fixture::open(root.path()).await?;
    let (target, install) = fixture
        .install("generation-a", 20, "verified read copy", true)
        .await?;
    let path = target.path().to_path_buf();
    target.transaction(|scope| Box::pin(async move {
        use durable_steps::storage::TransactionScope;
        let count: i64 = sqlx_core::query::query::<sqlx_turso::Turso>(
            "SELECT count(*) AS count FROM sqlite_schema WHERE name IN ('_fleet_sync_writer','_fleet_sync_writer_engine_history','_fleet_sync_capture_bookkeeping','_sqlx_migrations','_fleet_sync_local_migrations')"
        ).fetch_one(scope.executor()).await?.try_get("count")?;
        assert_eq!(count, 0, "read-copy install created writer or migration/capture state");
        let has_capture: i64 = sqlx_core::query::query::<sqlx_turso::Turso>(
            "SELECT count(*) AS count FROM sqlite_schema WHERE name='turso_cdc'"
        ).fetch_one(scope.executor()).await?.try_get("count")?;
        if has_capture != 0 {
            let changes: i64 = sqlx_core::query::query::<sqlx_turso::Turso>(
                "SELECT count(*) AS count FROM turso_cdc"
            ).fetch_one(scope.executor()).await?.try_get("count")?;
            assert_eq!(changes, 0, "read-copy install captured a change");
        }
        Ok(())
    })).await?;
    let copy = fixture
        .coordinator
        .read_copy(install.install_id, &target)
        .await?;
    assert_eq!(copy.installed().database, install.database);
    assert_eq!(copy.installed().position.sequence.value(), 20);
    assert_eq!(copy.installed().schema, install.schema);
    assert_eq!(
        copy.reader()
            .fetch_one("SELECT value FROM records")
            .await?
            .try_get::<String, _>("value")?,
        "verified read copy"
    );
    for sql in [
        "INSERT INTO records VALUES('x','wrong')",
        "BEGIN",
        "PRAGMA user_version=1",
    ] {
        assert!(
            copy.reader().execute(sql).await.is_err(),
            "unguarded statement {sql}"
        );
    }
    assert!(
        target
            .transaction(|_| Box::pin(async { Ok(()) }))
            .await
            .is_err()
    );
    assert!(matches!(
        fleet_sync_writer::ReadCopy::open_verified(&path, &install).await,
        Err(fleet_sync_writer::ReadCopyError::Open(
            fleet_sync_writer::OpenError::FileInUse
        ))
    ));
    copy.close().await?;
    assert_generation_removed(&path);
    assert_eq!(
        fixture.router.calls.load(Ordering::SeqCst),
        0,
        "read-only install invoked a writer authority call"
    );
    fixture.objects.close().await?;
    Ok(())
}

#[tokio::test]
async fn old_read_pin_outlives_its_owner_and_a_newly_exposed_generation() -> TestResult {
    let root = tempfile::tempdir()?;
    let fixture = Fixture::open(root.path()).await?;
    let (old_target, old_install) = fixture
        .install("generation-old", 20, "old generation", true)
        .await?;
    let old_path = old_target.path().to_path_buf();
    let old = fixture
        .coordinator
        .read_copy(old_install.install_id, &old_target)
        .await?;
    let mut pin = old.reader().begin_read().await?;
    assert_eq!(
        (&mut pin)
            .fetch_one("SELECT value FROM records")
            .await?
            .try_get::<String, _>("value")?,
        "old generation"
    );
    drop(old);
    assert!(old_path.exists(), "owner drop released a pinned file");
    let (new_target, new_install) = fixture
        .install("generation-new", 30, "new generation", true)
        .await?;
    let new = fixture
        .coordinator
        .read_copy(new_install.install_id, &new_target)
        .await?;
    assert_eq!(new.installed().position.sequence.value(), 30);
    assert_eq!(
        new.reader()
            .fetch_one("SELECT value FROM records")
            .await?
            .try_get::<String, _>("value")?,
        "new generation"
    );
    assert_eq!(
        (&mut pin)
            .fetch_one("SELECT value FROM records")
            .await?
            .try_get::<String, _>("value")?,
        "old generation"
    );
    assert!(old_path.exists());
    pin.close().await?;
    // Poll an actual file-lifetime outcome; elapsed time alone is never success.
    tokio::time::timeout(Duration::from_secs(5), async {
        while old_path.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_generation_removed(&old_path);
    let new_path = new_target.path().to_path_buf();
    new.close().await?;
    assert_generation_removed(&new_path);
    fixture.objects.close().await?;
    Ok(())
}

#[tokio::test]
async fn partial_install_after_store_reopen_and_wrong_schema_cannot_be_exposed() -> TestResult {
    let root = tempfile::tempdir()?;
    let fixture = Fixture::open(root.path()).await?;
    let (target, install) = fixture
        .install("generation-partial", 20, "partial rows", false)
        .await?;
    assert!(matches!(
        fixture
            .coordinator
            .read_copy(install.install_id, &target)
            .await,
        Err(StagingError::Rules(
            fleet_sync_receiver::SnapshotInstallError::NotVerified
        ))
    ));
    target.transaction(|_| Box::pin(async { Ok(()) })).await?;
    let staging_path = fixture.coordinator.staging.path(&install);
    fixture.objects.close().await?;
    drop(fixture);
    let staging = StagingStore::open(root.path().join("staging")).await?;
    assert!(staging.page_receipt(&install, 0).await?.is_some());
    assert!(matches!(
        fleet_sync_writer::ReadCopy::open_verified(&staging_path, &install).await,
        Err(fleet_sync_writer::ReadCopyError::Install(
            fleet_sync_receiver::SnapshotInstallError::NotVerified
        ))
    ));
    assert!(staging_path.exists());
    drop(target);

    let fixture = Fixture::open(&root.path().join("complete")).await?;
    let (target, install) = fixture
        .install("generation-complete", 30, "complete", true)
        .await?;
    target.close_for_read_copy().await?;
    let mut wrong = install.clone();
    wrong.schema.version += 1;
    assert!(matches!(
        fleet_sync_writer::ReadCopy::open_verified(target.path(), &wrong).await,
        Err(fleet_sync_writer::ReadCopyError::Install(
            fleet_sync_receiver::SnapshotInstallError::IdentityConflict
        ))
    ));
    assert!(target.path().exists());
    let mut connection = sqlx_turso::TursoConnectOptions::new()
        .filename(target.path())
        .create_if_missing(false)
        .connect()
        .await?;
    connection
        .execute("CREATE INDEX unexpected_catalog ON records(value)")
        .await?;
    connection.close().await?;
    assert!(matches!(
        fleet_sync_writer::ReadCopy::open_verified(target.path(), &install).await,
        Err(fleet_sync_writer::ReadCopyError::Install(
            fleet_sync_receiver::SnapshotInstallError::SchemaMismatch
        ))
    ));
    assert!(target.path().exists());
    let mut connection = sqlx_turso::TursoConnectOptions::new()
        .filename(target.path())
        .create_if_missing(false)
        .connect()
        .await?;
    connection.execute("DROP INDEX unexpected_catalog").await?;
    connection.close().await?;
    let valid = fleet_sync_writer::ReadCopy::open_verified(target.path(), &install).await?;
    valid.close().await?;
    fixture.objects.close().await?;
    Ok(())
}
