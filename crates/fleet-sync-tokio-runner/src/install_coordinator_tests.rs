#![allow(clippy::panic_in_result_fn)]
use crate::*;
use durable_steps::{
    AtomicStore, CallFailure, NotSentReason, StoreError, Uuid, storage::TransactionScope,
};
use fleet_sync_protocol::*;
use fleet_sync_receiver::{SnapshotInstall, SnapshotInstallRules};
use runtime_primitives::SystemClock;
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
type TestHost = TokioHost<NoCalls>;
struct Fixture {
    objects: ObjectStore<TestHost>,
    target: Arc<TokioObject<TestHost>>,
    coordinator: Arc<InstallCoordinator<TestHost>>,
}
impl Fixture {
    async fn open(root: &std::path::Path) -> TestResult<Self> {
        let objects = ObjectStore::open(root.join("objects")).await?;
        let host = Arc::new(TokioHost::new(Arc::new(NoCalls), SystemClock));
        let target = objects
            .object(
                ObjectKey::new("copy")?,
                ObjectOptions {
                    foreign_keys: false,
                },
                host.clone(),
            )
            .await?;
        target
            .transaction(|scope| {
                Box::pin(async move {
                    let mut migrations = sqlx::migrate!("../fleet-sync-receiver/migrations");
                    migrations.dangerous_set_table_name("_fleet_sync_copy_migrations");
                    migrations
                        .run_direct(None, scope.executor(), false)
                        .await
                        .map_err(StoreError::backend)
                })
            })
            .await?;
        let control = objects
            .object(ObjectKey::new("control")?, ObjectOptions::default(), host)
            .await?;
        let staging = Arc::new(StagingStore::open(root.join("staging")).await?);
        let coordinator = Arc::new(InstallCoordinator::open(control, staging).await?);
        Ok(Self {
            objects,
            target,
            coordinator,
        })
    }
    async fn stage(&self, sequence: u64) -> TestResult<(SnapshotInstall, InstallHandle<TestHost>)> {
        let (mut install, mut source, mut page, mut manifest) =
            crate::staging_store_tests::snapshot()?;
        let position =
            ReplicationPosition::new(install.position.epoch, CommitSequence::new(sequence));
        install.position = position;
        source.acked = position;
        page.position = position;
        manifest.position = position;
        let request = InstallRequest {
            attempt: install.install_id,
            target: self.target.key().clone(),
            database: install.database,
            source: ReceiverId::new(Uuid::from_u128(10))?,
        };
        let handle = self.coordinator.start(request, &self.target).await?;
        self.coordinator.pin(install.clone()).await?;
        self.coordinator.staging.prepare(&install, &source).await?;
        self.coordinator
            .staging
            .stage_page(&install, 0, None, &page)
            .await?;
        self.coordinator.staging.verify(&install, &manifest).await?;
        Ok((install, handle))
    }
    async fn live(&self) -> TestResult<SnapshotInstall> {
        Ok(self
            .target
            .transaction(|scope| {
                Box::pin(async move {
                    SnapshotInstallRules(scope)
                        .state()
                        .await
                        .map_err(StoreError::backend)?
                        .map(|marker| marker.install)
                        .ok_or_else(|| StoreError::backend("live marker missing"))
                })
            })
            .await?)
    }
}

#[tokio::test]
async fn newer_publication_refuses_the_older_install_and_preserves_its_canonical_outcome_after_restart()
-> TestResult {
    let root = tempfile::tempdir()?;
    let fixture = Fixture::open(root.path()).await?;
    let (older, handle) = fixture.stage(20).await?;
    let (newer, _) = fixture.stage(30).await?;
    assert!(matches!(
        fixture
            .coordinator
            .publish(newer.install_id, &fixture.target)
            .await?,
        InstallFileAnswer::Published { .. }
    ));
    assert_eq!(
        fixture
            .coordinator
            .publish(older.install_id, &fixture.target)
            .await?,
        InstallFileAnswer::Superseded {
            by: newer.install_id
        }
    );
    assert!(
        matches!(handle.status().await?.status, InstallStatus::Superseded { by, .. } if by == newer.install_id)
    );
    assert_eq!(fixture.live().await?, newer);
    fixture.objects.close().await?;
    drop(fixture);
    let restarted = Fixture::open(root.path()).await?;
    assert_eq!(
        restarted
            .coordinator
            .publish(older.install_id, &restarted.target)
            .await?,
        InstallFileAnswer::Superseded {
            by: newer.install_id
        }
    );
    assert_eq!(restarted.live().await?, newer);
    restarted.objects.close().await?;
    Ok(())
}

#[tokio::test]
async fn published_receipt_survives_a_later_marker_and_a_crash_before_the_control_receipt()
-> TestResult {
    for lost_control_receipt in [false, true] {
        let root = tempfile::tempdir()?;
        let fixture = Fixture::open(root.path()).await?;
        let (older, _) = fixture.stage(20).await?;
        if lost_control_receipt {
            // Crash boundary: the file rename committed, but the control call
            // answer has not. Only the live marker proves publication here.
            fixture
                .coordinator
                .staging
                .publish(&older, &fixture.target)
                .await?;
        } else {
            fixture
                .coordinator
                .publish(older.install_id, &fixture.target)
                .await?;
        }
        fixture.objects.close().await?;
        drop(fixture);
        let restarted = Fixture::open(root.path()).await?;
        let (newer, _) = restarted.stage(30).await?;
        restarted
            .coordinator
            .publish(newer.install_id, &restarted.target)
            .await?;
        assert_eq!(
            restarted
                .coordinator
                .publish(older.install_id, &restarted.target)
                .await?,
            InstallFileAnswer::Published {
                snapshot: older.clone()
            }
        );
        assert_eq!(
            restarted
                .coordinator
                .mark_ready_with(older.install_id, &restarted.target, |_, _| Box::pin(
                    async {
                        Err(StoreError::backend("superseded ready callback must not run").into())
                    }
                ))
                .await?,
            InstallFileAnswer::Superseded {
                by: newer.install_id
            }
        );
        assert_eq!(restarted.live().await?, newer);
        let old_record = restarted.coordinator.record(older.install_id).await?;
        let new_record = restarted.coordinator.record(newer.install_id).await?;
        assert!(old_record.generation < new_record.generation);
        restarted.objects.close().await?;
    }
    Ok(())
}
