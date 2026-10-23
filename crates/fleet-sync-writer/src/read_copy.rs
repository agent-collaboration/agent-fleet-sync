//! Guarded access to one verified immutable follower file, without a writer.
use crate::{OpenError, ReadPool, file_admission::FileAdmission, writer_scope::WriterScope};
use fleet_sync_protocol::CopyRole;
use fleet_sync_receiver::{
    CopyInspection, InstalledSnapshot, SnapshotInstall, SnapshotInstallError, SnapshotInstallRules,
};
use sqlx_core::pool::PoolOptions;
use sqlx_turso::{Turso, TursoConnectOptions};
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum ReadCopyError {
    #[error(transparent)]
    Open(#[from] OpenError),
    #[error(transparent)]
    Install(#[from] SnapshotInstallError),
    #[error("read-copy cleanup failed: {message}")]
    Cleanup { message: String },
}
impl From<sqlx_core::Error> for ReadCopyError {
    fn from(error: sqlx_core::Error) -> Self {
        Self::Open(error.into())
    }
}

/// One disposable immutable file, whose reads remain valid across newer copies.
pub struct ReadCopy {
    reader: ReadPool,
    installed: InstalledSnapshot,
    released: tokio::sync::watch::Receiver<crate::read_copy_file::ReleaseResult>,
}
impl ReadCopy {
    /// Transfer exclusive ownership of a closed, checkpointed follower file.
    ///
    /// The file must have a verified install matching `expected`. No other writable
    /// handle may remain or replace its path. Success consumes this disposable
    /// file: it is removed after the handle and every pinned read finish. Keep the
    /// Tokio runtime alive until cleanup completes; `close` waits for that result.
    /// Validation failure leaves the caller's file intact.
    pub async fn open_verified(
        path: &Path,
        expected: &SnapshotInstall,
    ) -> Result<Self, ReadCopyError> {
        let admission = FileAdmission::read_copy(path).await?;
        let pool_admission = admission.clone();
        let pool = PoolOptions::<Turso>::new()
            .min_connections(1)
            .max_connections(4)
            .after_connect(move |_, _| {
                let admission = pool_admission.clone();
                Box::pin(async move {
                    let _admission = admission;
                    Ok(())
                })
            })
            .connect_with(
                TursoConnectOptions::new()
                    .filename(path)
                    .create_if_missing(false)
                    .read_only(true),
            )
            .await?;
        let validated = async {
            let mut connection = pool.acquire().await?;
            let mut scope = WriterScope(&mut connection);
            let state = SnapshotInstallRules(&mut scope)
                .state()
                .await?
                .ok_or(SnapshotInstallError::NotPrepared)?;
            if state.install != *expected {
                return Err(SnapshotInstallError::IdentityConflict.into());
            }
            if !state.verified {
                return Err(SnapshotInstallError::NotVerified.into());
            }
            match fleet_sync_receiver::inspect_copy(&mut scope).await {
                CopyInspection::Live {
                    status,
                    fingerprint,
                    ..
                } if status.role == CopyRole::Follower
                    && status.database == expected.database
                    && status.acked == expected.position
                    && status.schema == expected.schema
                    && fingerprint == expected.schema.fingerprint =>
                {
                    Ok::<_, ReadCopyError>(())
                }
                _ => Err(SnapshotInstallError::SchemaMismatch.into()),
            }
        }
        .await;
        if let Err(error) = validated {
            pool.close().await;
            return Err(error);
        }
        let (file, released) =
            crate::read_copy_file::ReadCopyFile::new(path.into(), pool.clone(), admission);
        let mut reader = ReadPool::from_native_pool(crate::native_pool::NativePool::passive(pool));
        reader.file = Some(file);
        Ok(Self {
            reader,
            installed: InstalledSnapshot {
                database: expected.database,
                position: expected.position,
                schema: expected.schema.clone(),
            },
            released,
        })
    }

    pub fn reader(&self) -> &ReadPool {
        &self.reader
    }
    pub fn installed(&self) -> &InstalledSnapshot {
        &self.installed
    }

    /// Release this handle and wait for its final pin, pool close and file cleanup.
    pub async fn close(self) -> Result<(), ReadCopyError> {
        let mut released = self.released.clone();
        drop(self);
        loop {
            if let Some(result) = released.borrow().clone() {
                return result.map_err(|message| ReadCopyError::Cleanup { message });
            }
            released
                .changed()
                .await
                .map_err(|error| ReadCopyError::Cleanup {
                    message: error.to_string(),
                })?;
        }
    }
}
