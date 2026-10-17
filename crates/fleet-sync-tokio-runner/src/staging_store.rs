use crate::{LocalStateCarryError, PublishError, native_carry_schema::FileScope};
use durable_steps::{BoxFuture, StoreError, Uuid};
use fleet_sync_protocol::{
    CopyStatus, ReplicationPosition, SnapshotCursor, SnapshotManifest, SnapshotPage, SnapshotRead,
};
use fleet_sync_receiver::{
    InstallPageReceipt, SnapshotInstall, SnapshotInstallError, SnapshotInstallRules,
};
use serde::{Deserialize, Serialize};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    migrate::Migrator,
};
use sqlx_turso::{TursoConnectOptions, TursoConnection};
use std::{
    collections::BTreeMap,
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Debug, thiserror::Error)]
pub enum StagingError {
    #[error(transparent)]
    Storage(#[from] StoreError),
    #[error(transparent)]
    Rules(#[from] SnapshotInstallError),
    #[error(transparent)]
    Publish(#[from] PublishError),
    #[error(transparent)]
    Carry(#[from] LocalStateCarryError),
    #[error("staging I/O failed: {message}")]
    Io { message: String },
}
impl StagingError {
    pub(crate) fn io(error: impl std::fmt::Display) -> Self {
        Self::Io {
            message: error.to_string(),
        }
    }
}
impl From<sqlx_core::Error> for StagingError {
    fn from(error: sqlx_core::Error) -> Self {
        Self::Storage(StoreError::backend(error))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StagedPage {
    Receipt(InstallPageReceipt),
    Moved { position: ReplicationPosition },
}

/// Owns private staging files. A page call holds only this install's lock while
/// fetching; no SQL transaction spans the network request. Receipts and rows
/// commit together, and only the receipt is returned to the control journal.
pub struct StagingStore {
    directory: PathBuf,
    files: Mutex<BTreeMap<Uuid, Arc<Mutex<()>>>>,
}
static COPY_MIGRATIONS: Migrator = sqlx::migrate!("../fleet-sync-receiver/migrations");
impl StagingStore {
    pub async fn open(directory: impl AsRef<Path>) -> Result<Self, StagingError> {
        tokio::fs::create_dir_all(directory.as_ref())
            .await
            .map_err(StagingError::io)?;
        Ok(Self {
            directory: directory.as_ref().into(),
            files: Mutex::new(BTreeMap::new()),
        })
    }
    pub(crate) fn path(&self, install: &SnapshotInstall) -> PathBuf {
        self.directory
            .join(format!("install-{}.db", install.install_id))
    }
    pub(crate) async fn lock(
        &self,
        install: &SnapshotInstall,
    ) -> Result<OwnedMutexGuard<()>, StagingError> {
        if install.install_id.is_nil() {
            return Err(SnapshotInstallError::IdentityConflict.into());
        }
        let owner = self
            .files
            .lock()
            .await
            .entry(install.install_id)
            .or_default()
            .clone();
        Ok(owner.lock_owned().await)
    }
    pub async fn prepare(
        &self,
        install: &SnapshotInstall,
        source: &CopyStatus,
    ) -> Result<(), StagingError> {
        let _owner = self.lock(install).await?;
        let mut connection = open_stage(&self.path(install), true).await?;
        let result = async {
            let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
            let mut migrator =
                Migrator::with_migrations(COPY_MIGRATIONS.iter().cloned().collect::<Vec<_>>());
            migrator.dangerous_set_table_name("_fleet_sync_copy_migrations");
            migrator
                .run_direct(None, &mut *transaction, false)
                .await
                .map_err(StoreError::backend)?;
            SnapshotInstallRules(&mut FileScope(&mut transaction))
                .prepare(install, source)
                .await?;
            transaction.commit().await?;
            Ok::<_, StagingError>(())
        }
        .await;
        let closed = connection.close().await;
        result?;
        closed?;
        Ok(())
    }
    pub async fn page_receipt(
        &self,
        install: &SnapshotInstall,
        page: u32,
    ) -> Result<Option<InstallPageReceipt>, StagingError> {
        let _owner = self.lock(install).await?;
        let install = install.clone();
        transact_path(&self.path(&install), move |scope| {
            Box::pin(async move {
                Ok(SnapshotInstallRules(scope)
                    .page_receipt(&install, page)
                    .await?)
            })
        })
        .await
    }
    pub async fn stage_page(
        &self,
        install: &SnapshotInstall,
        page: u32,
        cursor: Option<&SnapshotCursor>,
        contents: &SnapshotPage,
    ) -> Result<InstallPageReceipt, StagingError> {
        let _owner = self.lock(install).await?;
        let (install, cursor, contents) = (install.clone(), cursor.cloned(), contents.clone());
        transact_path(&self.path(&install), move |scope| {
            Box::pin(async move {
                Ok(SnapshotInstallRules(scope)
                    .stage_page(&install, page, cursor.as_ref(), &contents)
                    .await?)
            })
        })
        .await
    }
    pub async fn fetch_page<F, Fetch>(
        &self,
        install: &SnapshotInstall,
        page: u32,
        cursor: Option<&SnapshotCursor>,
        fetch: F,
    ) -> Result<StagedPage, StagingError>
    where
        F: FnOnce() -> Fetch + Send,
        Fetch: Future<Output = Result<SnapshotRead, StagingError>> + Send,
    {
        let _owner = self.lock(install).await?;
        let path = self.path(install);
        let saved_install = install.clone();
        let saved = transact_path(&path, move |scope| {
            Box::pin(async move {
                Ok(SnapshotInstallRules(scope)
                    .page_receipt(&saved_install, page)
                    .await?)
            })
        })
        .await?;
        if let Some(saved) = saved {
            return Ok(StagedPage::Receipt(saved));
        }
        let contents = match fetch().await? {
            SnapshotRead::Moved { position } => return Ok(StagedPage::Moved { position }),
            SnapshotRead::Page(contents) if contents.position != install.position => {
                return Ok(StagedPage::Moved {
                    position: contents.position,
                });
            }
            SnapshotRead::Page(contents) => contents,
        };
        let (install, cursor) = (install.clone(), cursor.cloned());
        let receipt = transact_path(&path, move |scope| {
            Box::pin(async move {
                Ok(SnapshotInstallRules(scope)
                    .stage_page(&install, page, cursor.as_ref(), &contents)
                    .await?)
            })
        })
        .await?;
        Ok(StagedPage::Receipt(receipt))
    }
    pub async fn verify(
        &self,
        install: &SnapshotInstall,
        manifest: &SnapshotManifest,
    ) -> Result<(), StagingError> {
        let _owner = self.lock(install).await?;
        let (install, manifest) = (install.clone(), manifest.clone());
        transact_path(&self.path(&install), move |scope| {
            Box::pin(async move {
                Ok(SnapshotInstallRules(scope)
                    .verify(&install, &manifest)
                    .await?)
            })
        })
        .await
    }
}
pub(crate) async fn open_stage(path: &Path, create: bool) -> Result<TursoConnection, StagingError> {
    Ok(TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(create)
        .foreign_keys(false)
        .connect()
        .await?)
}
pub(crate) async fn transact_path<T, F>(path: &Path, run: F) -> Result<T, StagingError>
where
    T: Send,
    F: for<'scope, 'transaction> FnOnce(
            &'scope mut FileScope<'transaction>,
        ) -> BoxFuture<'scope, Result<T, StagingError>>
        + Send,
{
    let mut connection = open_stage(path, false).await?;
    let result = async {
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        match run(&mut FileScope(&mut transaction)).await {
            Ok(value) => {
                transaction.commit().await?;
                Ok(value)
            }
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }
    .await;
    let closed = connection.close().await;
    let result = result?;
    closed?;
    Ok(result)
}
