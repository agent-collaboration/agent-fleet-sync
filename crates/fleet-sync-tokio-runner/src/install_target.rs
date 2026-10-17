use crate::{ObjectKey, StagingError, StagingStore, TokioObject};
use durable_steps::{BoxFuture, Host, StoreError};
use fleet_sync_receiver::{SnapshotInstall, SnapshotInstallRules, SnapshotInstallState};
use fleet_sync_writer::{ReplicatedDatabase, ReplicatedSchema};
use std::{path::Path, sync::Arc};

/// The control journal orders installs; each target owns its file and receipt.
pub trait SnapshotTarget: Send + Sync {
    fn key(&self) -> &ObjectKey;
    fn path(&self) -> &Path;
    fn installed_snapshot(&self)
    -> BoxFuture<'_, Result<Option<SnapshotInstallState>, StoreError>>;
    fn publish_snapshot<'file>(
        &'file self,
        staging: &'file StagingStore,
        install: &'file SnapshotInstall,
    ) -> BoxFuture<'file, Result<(), StagingError>>;
}

pub struct WriterInstallTarget<S: ReplicatedSchema> {
    key: ObjectKey,
    database: Arc<ReplicatedDatabase<S>>,
}
impl<S: ReplicatedSchema> WriterInstallTarget<S> {
    pub fn new(key: ObjectKey, database: Arc<ReplicatedDatabase<S>>) -> Self {
        Self { key, database }
    }
    pub fn database(&self) -> &Arc<ReplicatedDatabase<S>> {
        &self.database
    }
}
impl<S: ReplicatedSchema> SnapshotTarget for WriterInstallTarget<S> {
    fn key(&self) -> &ObjectKey {
        &self.key
    }
    fn path(&self) -> &Path {
        self.database.path()
    }
    fn installed_snapshot(
        &self,
    ) -> BoxFuture<'_, Result<Option<SnapshotInstallState>, StoreError>> {
        Box::pin(async {
            self.database
                .installed_snapshot()
                .await
                .map_err(StoreError::backend)
        })
    }
    fn publish_snapshot<'file>(
        &'file self,
        staging: &'file StagingStore,
        install: &'file SnapshotInstall,
    ) -> BoxFuture<'file, Result<(), StagingError>> {
        Box::pin(staging.publish_writer(install, self.database.as_ref()))
    }
}
impl<H: Host> SnapshotTarget for TokioObject<H> {
    fn key(&self) -> &ObjectKey {
        self.key()
    }
    fn path(&self) -> &Path {
        self.path()
    }
    fn installed_snapshot(
        &self,
    ) -> BoxFuture<'_, Result<Option<SnapshotInstallState>, StoreError>> {
        Box::pin(self.rule_transaction(|scope| {
            Box::pin(async move {
                SnapshotInstallRules(scope)
                    .state()
                    .await
                    .map_err(StoreError::backend)
            })
        }))
    }
    fn publish_snapshot<'file>(
        &'file self,
        staging: &'file StagingStore,
        install: &'file SnapshotInstall,
    ) -> BoxFuture<'file, Result<(), StagingError>> {
        Box::pin(staging.publish(install, self))
    }
}
impl<T: SnapshotTarget + ?Sized> SnapshotTarget for Arc<T> {
    fn key(&self) -> &ObjectKey {
        self.as_ref().key()
    }
    fn path(&self) -> &Path {
        self.as_ref().path()
    }
    fn installed_snapshot(
        &self,
    ) -> BoxFuture<'_, Result<Option<SnapshotInstallState>, StoreError>> {
        self.as_ref().installed_snapshot()
    }
    fn publish_snapshot<'file>(
        &'file self,
        staging: &'file StagingStore,
        install: &'file SnapshotInstall,
    ) -> BoxFuture<'file, Result<(), StagingError>> {
        self.as_ref().publish_snapshot(staging, install)
    }
}
