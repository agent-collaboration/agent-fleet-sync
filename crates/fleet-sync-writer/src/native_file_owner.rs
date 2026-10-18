use crate::{
    bootstrap_publish_probe::BootstrapPublishProbe,
    file_admission::FileAdmission,
    native_file::{PublishError, checkpoint_for_publish, ensure_empty_wal},
    native_pool::{NativePool, PoolSlot},
    replicated_database::{CaptureMode, open_pool},
    writer_file_record::{WriterFileRecord, read_file_record},
};
use durable_steps::{BoxFuture, StoreError};
use fleet_sync_receiver::{SnapshotInstall, SnapshotInstallState};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    pool::Pool,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

enum OwnerState {
    Open(Arc<FileAdmission>),
    Publishing {
        receipt: Box<PublicationReceipt>,
        admission: Arc<FileAdmission>,
    },
    Closing(Arc<FileAdmission>),
    Closed,
}
struct PublicationReceipt {
    database: fleet_sync_protocol::DatabaseId,
    peer: fleet_sync_protocol::PeerId,
    epoch: fleet_sync_protocol::Epoch,
    previous: Option<SnapshotInstallState>,
    expected: Option<SnapshotInstall>,
}
pub(crate) struct NativeFileOwner {
    path: PathBuf,
    state: Mutex<OwnerState>,
    writer: Arc<PoolSlot>,
    maintenance: Arc<PoolSlot>,
    readers: Arc<PoolSlot>,
    probe: BootstrapPublishProbe,
}
impl NativeFileOwner {
    pub fn new(
        path: &Path,
        writer: Pool<Turso>,
        maintenance: Pool<Turso>,
        readers: Pool<Turso>,
        probe: BootstrapPublishProbe,
        admission: Arc<FileAdmission>,
    ) -> Arc<Self> {
        Arc::new(Self {
            path: path.into(),
            state: Mutex::new(OwnerState::Open(admission)),
            writer: Arc::new(PoolSlot::new(writer)),
            maintenance: Arc::new(PoolSlot::new(maintenance)),
            readers: Arc::new(PoolSlot::new(readers)),
            probe,
        })
    }
    pub fn writer(self: &Arc<Self>) -> NativePool {
        NativePool::owned(self.writer.clone(), self.clone())
    }
    pub fn maintenance(self: &Arc<Self>) -> NativePool {
        NativePool::owned(self.maintenance.clone(), self.clone())
    }
    pub fn readers(self: &Arc<Self>) -> NativePool {
        NativePool::owned(self.readers.clone(), self.clone())
    }
    pub async fn recover(&self) -> Result<(), PublishError> {
        self.recover_locked(&mut *self.state.lock().await).await
    }
    pub async fn close(&self) {
        let mut owner = self.state.lock().await;
        let admission = match &*owner {
            OwnerState::Open(admission)
            | OwnerState::Publishing { admission, .. }
            | OwnerState::Closing(admission) => admission.clone(),
            OwnerState::Closed => return,
        };
        // A cancelled close stays terminal while retaining its physical claim
        // until a retry finishes draining or the final owner is dropped.
        *owner = OwnerState::Closing(admission);
        self.close_pools().await;
        *owner = OwnerState::Closed;
    }
    async fn close_pools(&self) {
        self.writer.close().await;
        self.readers.close().await;
        self.maintenance.close().await;
    }
    pub async fn publish<T, F>(
        &self,
        record: &WriterFileRecord,
        expected: Option<SnapshotInstall>,
        publish: F,
    ) -> Result<T, PublishError>
    where
        T: Send + 'static,
        F: for<'path> FnOnce(&'path Path) -> BoxFuture<'path, Result<T, PublishError>> + Send,
    {
        let mut owner = self.state.lock().await;
        self.recover_locked(&mut owner).await?;
        let admission = match &*owner {
            OwnerState::Open(admission) => admission.clone(),
            _ => return Err(PublishError::Closed),
        };
        *owner = OwnerState::Publishing {
            receipt: Box::new(PublicationReceipt {
                database: record.database,
                peer: record.peer,
                epoch: record.epoch,
                previous: record.bootstrap.clone(),
                expected,
            }),
            admission,
        };
        let result = async {
            self.writer.close().await;
            self.readers.close().await;
            let mut connection = self
                .maintenance
                .current()
                .map_err(StoreError::backend)?
                .acquire()
                .await
                .map_err(StoreError::backend)?;
            checkpoint_for_publish(&mut connection).await?;
            drop(connection);
            self.maintenance.close().await;
            ensure_empty_wal(&self.path).await?;
            publish(&self.path).await
        }
        .await;
        self.recover_locked(&mut owner).await?;
        result
    }
    async fn recover_locked(&self, owner: &mut OwnerState) -> Result<(), PublishError> {
        let (receipt, admission) = match owner {
            OwnerState::Open(_) => return Ok(()),
            OwnerState::Closed | OwnerState::Closing(_) => return Err(PublishError::Closed),
            OwnerState::Publishing { receipt, admission } => (receipt, admission),
        };
        self.close_pools().await;
        // Publication can replace the inode. Decide admission for that actual
        // generation before any writable recovery connection reaches the SDK.
        let current_admission = FileAdmission::writer(&self.path, false).await?;
        *admission = current_admission.clone();
        let mut connection = TursoConnectOptions::new()
            .filename(&self.path)
            .create_if_missing(false)
            .foreign_keys(false)
            .connect()
            .await
            .map_err(StoreError::backend)?;
        let found = async {
            connection
                .execute("PRAGMA capture_data_changes_conn('off')")
                .await
                .map_err(StoreError::backend)?;
            read_file_record(&mut connection)
                .await
                .map_err(StoreError::backend)
        }
        .await;
        let closed = connection.close().await.map_err(StoreError::backend);
        let found = found?.ok_or(PublishError::InvalidWriterReceipt)?;
        closed?;
        let same_identity = found.database == receipt.database
            && found.peer == receipt.peer
            && found.epoch == receipt.epoch;
        let previous = found.bootstrap == receipt.previous;
        let published = receipt.expected.as_ref().is_some_and(|expected| {
            found
                .bootstrap
                .as_ref()
                .is_some_and(|marker| marker.install == *expected && marker.verified)
        });
        if !same_identity || (!previous && !published) {
            return Err(PublishError::InvalidWriterReceipt);
        }
        self.probe.check_reopen()?;
        let maintenance = open_pool(
            &self.path,
            CaptureMode::Maintenance,
            current_admission.clone(),
        )
        .await
        .map_err(StoreError::backend)?;
        let writer =
            match open_pool(&self.path, CaptureMode::Writer, current_admission.clone()).await {
                Ok(pool) => pool,
                Err(error) => {
                    maintenance.close().await;
                    return Err(StoreError::backend(error).into());
                }
            };
        let readers =
            match open_pool(&self.path, CaptureMode::Reader, current_admission.clone()).await {
                Ok(pool) => pool,
                Err(error) => {
                    writer.close().await;
                    maintenance.close().await;
                    return Err(StoreError::backend(error).into());
                }
            };
        self.maintenance
            .reopen(maintenance)
            .map_err(StoreError::backend)?;
        self.writer.reopen(writer).map_err(StoreError::backend)?;
        self.readers.reopen(readers).map_err(StoreError::backend)?;
        *owner = OwnerState::Open(current_admission);
        Ok(())
    }
}
