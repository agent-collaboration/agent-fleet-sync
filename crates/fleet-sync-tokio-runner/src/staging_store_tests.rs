#![allow(clippy::panic_in_result_fn)]
use crate::*;
use durable_steps::{
    AtomicStore, CallFailure, NotSentReason, StoreError, storage::TransactionScope,
};
use fleet_sync_protocol::*;
use fleet_sync_receiver::{SnapshotInstall, SnapshotInstallError, SnapshotInstallRules};
use runtime_primitives::SystemClock;
use sqlx_core::{executor::Executor, query::query, row::Row};
use std::sync::Arc;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct NoCalls;
impl CallRouter for NoCalls {
    async fn dispatch(&self, _: CallEnvelope) -> Result<serde_json::Value, CallFailure> {
        Err(CallFailure::NotSent {
            reason: NotSentReason::Overloaded,
        })
    }
}
pub(super) fn snapshot() -> TestResult<(SnapshotInstall, CopyStatus, SnapshotPage, SnapshotManifest)>
{
    let database = DatabaseId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000001,
    ))?;
    let holder = PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000002,
    ))?;
    let epoch = Epoch::new(1)?;
    let position = ReplicationPosition::new(epoch, CommitSequence::new(20));
    let sql = "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,value TEXT)";
    let schema = SchemaIdentity::new(
        1,
        schema_fingerprint(&[SchemaEntry {
            kind: SchemaObjectKind::Table,
            name: "records".into(),
            sql: sql.into(),
        }])?,
    )?;
    let rows = vec![vec![
        SqlValue::Text("019eef00-0000-7000-8000-000000000002".into()),
        SqlValue::Text("verified snapshot".into()),
    ]];
    let mut digest = TableDigestBuilder::new();
    digest.push_row(rows.first().ok_or("no fixture row")?)?;
    Ok((
        SnapshotInstall {
            install_id: fleet_sync_protocol::InstallId::new(durable_steps::Uuid::now_v7())?,
            database,
            position,
            schema: schema.clone(),
        },
        CopyStatus {
            database,
            role: CopyRole::Authoritative,
            fence: epoch,
            holder: Some(holder),
            start: ReplicationPosition::new(epoch, CommitSequence::new(0)),
            acked: position,
            schema: schema.clone(),
        },
        SnapshotPage {
            database,
            position,
            schema: schema.clone(),
            schema_steps: vec![sql.into()],
            table: "records".into(),
            rows,
            next: None,
        },
        SnapshotManifest {
            database,
            position,
            schema,
            tables: vec![TableManifest {
                table: TableName::new("records")?,
                row_count: 1,
                digest: digest.finish(),
            }],
        },
    ))
}

#[tokio::test]
async fn verified_receipt_replays_without_fetching_a_later_moved_manifest() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (install, source, page, manifest) = snapshot()?;
    let store = StagingStore::open(directory.path()).await?;
    store.prepare(&install, &source).await?;
    store.stage_page(&install, 0, None, &page).await?;
    assert_eq!(
        store
            .verify_from(&install, move || async {
                Ok(SnapshotManifestAnswer::Manifest(manifest))
            })
            .await?,
        StagedVerification::Verified
    );
    drop(store);
    let reopened = StagingStore::open(directory.path()).await?;
    assert_eq!(
        reopened
            .verify_from(&install, || async {
                Err(StoreError::backend(
                    "verified receipt must answer without fetching source again",
                )
                .into())
            })
            .await?,
        StagedVerification::Verified
    );
    Ok(())
}

#[tokio::test]
async fn publication_exposes_only_verified_rows_and_replays_after_staging_file_was_renamed()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let objects = ObjectStore::open(directory.path().join("objects")).await?;
    let target = objects
        .object(
            ObjectKey::new("copy")?,
            ObjectOptions {
                foreign_keys: false,
            },
            Arc::new(TokioHost::new(Arc::new(NoCalls), SystemClock)),
        )
        .await?;
    target.transaction(|scope|Box::pin(async move {
        let mut migrator = sqlx::migrate!("../fleet-sync-receiver/migrations");
        migrator.dangerous_set_table_name("_fleet_sync_copy_migrations");
        migrator.run_direct(None, scope.executor(), false).await.map_err(StoreError::backend)?;
        scope.executor().execute("CREATE TABLE old_local(value TEXT); INSERT INTO old_local VALUES('target local')").await.map_err(StoreError::backend)?;
        Ok(())
    })).await?;
    let store = StagingStore::open(directory.path().join("staging")).await?;
    let (mut old_install, mut old_status, mut old_page, mut old_manifest) = snapshot()?;
    let old_position = ReplicationPosition::new(old_install.position.epoch, CommitSequence::new(5));
    old_install.position = old_position;
    old_status.acked = old_position;
    old_page.position = old_position;
    old_page.rows = vec![vec![
        SqlValue::Text("019eef00-0000-7000-8000-000000000001".into()),
        SqlValue::Text("since deleted".into()),
    ]];
    old_manifest.position = old_position;
    let mut old_digest = TableDigestBuilder::new();
    old_digest.push_row(old_page.rows.first().ok_or("missing old row")?)?;
    old_manifest
        .tables
        .first_mut()
        .ok_or("missing old table")?
        .digest = old_digest.finish();
    store.prepare(&old_install, &old_status).await?;
    store.stage_page(&old_install, 0, None, &old_page).await?;
    store.verify(&old_install, &old_manifest).await?;
    store.publish(&old_install, &target).await?;
    let (install, status, page, manifest) = snapshot()?;
    store.prepare(&install, &status).await?;
    store.stage_page(&install, 0, None, &page).await?;
    assert!(matches!(
        store.publish(&install, &target).await,
        Err(StagingError::Rules(SnapshotInstallError::NotVerified))
    ));
    assert_eq!(
        target
            .transaction(|scope| Box::pin(async move {
                let row = query::<sqlx_turso::Turso>("SELECT value FROM old_local")
                    .fetch_one(scope.executor())
                    .await?;
                row.try_get::<String, _>("value")
                    .map_err(StoreError::backend)
            }))
            .await?,
        "target local"
    );
    let still_old = target
        .transaction(|scope| {
            Box::pin(async move {
                fleet_sync_receiver::snapshot_page(scope, None)
                    .await
                    .map_err(StoreError::backend)
            })
        })
        .await?;
    assert!(matches!(still_old,SnapshotRead::Page(ref result) if result.rows == old_page.rows));
    store.verify(&install, &manifest).await?;
    store.publish(&install, &target).await?;
    drop(store);
    let store = StagingStore::open(directory.path().join("staging")).await?;
    store.publish(&install, &target).await?;
    let observed = target
        .transaction(move |scope| {
            Box::pin(async move {
                let marker = SnapshotInstallRules(scope)
                    .state()
                    .await
                    .map_err(StoreError::backend)?
                    .ok_or_else(|| StoreError::backend("missing marker"))?;
                let page = fleet_sync_receiver::snapshot_page(scope, None)
                    .await
                    .map_err(StoreError::backend)?;
                let local = query::<sqlx_turso::Turso>("SELECT value FROM old_local")
                    .fetch_one(scope.executor())
                    .await?
                    .try_get::<String, _>("value")?;
                Ok((marker, page, local))
            })
        })
        .await?;
    assert_eq!(observed.0.install, install);
    assert!(observed.0.verified && !observed.0.ready);
    assert!(matches!(observed.1,SnapshotRead::Page(ref result) if result.rows==page.rows));
    assert_eq!(observed.2, "target local");
    objects.close().await?;
    Ok(())
}

#[tokio::test]
async fn committed_page_replays_without_refetch_and_moved_source_does_not_write_a_receipt()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let store = StagingStore::open(directory.path()).await?;
    let (install, status, page, _) = snapshot()?;
    store.prepare(&install, &status).await?;
    let first = store
        .fetch_page(&install, 0, None, || async {
            Ok(SnapshotRead::Page(Box::new(page)))
        })
        .await?;
    assert!(matches!(first, StagedPage::Receipt(ref receipt) if receipt.rows==1));
    drop(store);
    let store = StagingStore::open(directory.path()).await?;
    let replay = store
        .fetch_page(&install, 0, None, || async {
            Err(StoreError::backend("receipt replay must not fetch a moved source").into())
        })
        .await?;
    assert_eq!(replay, first);
    let (other_install, other_status, _, _) = snapshot()?;
    store.prepare(&other_install, &other_status).await?;
    let moved = ReplicationPosition::new(other_install.position.epoch, CommitSequence::new(30));
    assert_eq!(
        store
            .fetch_page(&other_install, 0, None, || async {
                Ok(SnapshotRead::Moved { position: moved })
            })
            .await?,
        StagedPage::Moved { position: moved }
    );
    assert!(store.page_receipt(&other_install, 0).await?.is_none());
    Ok(())
}
