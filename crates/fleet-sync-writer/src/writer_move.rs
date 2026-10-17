use crate::{
    OpenError, ReplicatedDatabase, ReplicatedSchema,
    native_file::{PublishError, checkpoint_for_publish, ensure_empty_wal},
    writer_authority::AuthorityObservation,
    writer_file_record::{WriterFileRecord, read_file_record},
    writer_move_probe::{MoveAsidePhase, MoveAsideProbe},
};
use fleet_sync_protocol::DatabaseId;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
};
use std::{fs::Metadata, path::Path};

#[derive(Debug, thiserror::Error)]
pub enum MoveAsideError {
    #[error("only a durably rejected registration can move aside")]
    NotRejected,
    #[error("move destination is occupied by another file")]
    DestinationOccupied,
    #[error("move paths must be distinct regular files")]
    InvalidPaths,
    #[error("neither the rejected source nor its destination exists")]
    SourceMissing,
    #[error("moved file has another database identity or no rejection marker")]
    InvalidIdentity,
    #[error(transparent)]
    Publish(#[from] PublishError),
    #[error(transparent)]
    Writer(#[from] OpenError),
    #[error("move-aside I/O failed: {message}")]
    Io { message: String },
}
impl From<std::io::Error> for MoveAsideError {
    fn from(error: std::io::Error) -> Self {
        Self::Io {
            message: error.to_string(),
        }
    }
}
impl From<sqlx_core::Error> for MoveAsideError {
    fn from(error: sqlx_core::Error) -> Self {
        Self::Writer(error.into())
    }
}

impl<S: ReplicatedSchema> ReplicatedDatabase<S> {
    #[cfg(feature = "test-support")]
    pub fn hold_move_aside(&self, phase: MoveAsidePhase) {
        self.move_probe.arm(phase);
    }
    #[cfg(feature = "test-support")]
    pub async fn wait_move_aside(&self) -> Result<MoveAsidePhase, OpenError> {
        self.move_probe.wait().await
    }
    #[cfg(feature = "test-support")]
    pub fn release_move_aside(&self) {
        self.move_probe.release();
    }

    /// The consumer stops and joins its writer runtime before calling this.
    /// Moving a rejected file is terminal; retry by its identities after a crash.
    pub async fn move_aside(&self, destination: &Path) -> Result<(), MoveAsideError> {
        check_paths(&self.path, destination).await?;
        let _engine = self.engine_work.lock().await;
        let _control = self.control.lock().await;
        let _readers = self
            .readers
            .readers
            .clone()
            .try_write_owned()
            .map_err(|_| PublishError::ActiveReaders)?;
        if !self.maintenance.is_closed() {
            let mut connection = self.maintenance.acquire().await?;
            let record = read_file_record(&mut connection)
                .await?
                .ok_or(MoveAsideError::InvalidIdentity)?;
            require_rejected(&record, self.database)?;
        }
        // Closed is set before draining starts. Cancellation never reopens this
        // terminal owner, and retry_move_aside never creates a replacement file.
        self.native_owner.close().await;
        move_file(self.database, &self.path, destination, &self.move_probe).await
    }

    /// Complete a stopped consumer's move without creating either path.
    pub async fn retry_move_aside(
        database: DatabaseId,
        source: &Path,
        destination: &Path,
    ) -> Result<(), MoveAsideError> {
        move_file(database, source, destination, &MoveAsideProbe::default()).await
    }
}

fn require_rejected(record: &WriterFileRecord, database: DatabaseId) -> Result<(), MoveAsideError> {
    if record.database != database {
        return Err(MoveAsideError::InvalidIdentity);
    }
    if !matches!(
        record.authority,
        Some(AuthorityObservation::RegistrationRejected { .. })
    ) {
        return Err(MoveAsideError::NotRejected);
    }
    Ok(())
}
async fn metadata(path: &Path) -> Result<Option<Metadata>, MoveAsideError> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.is_file() => Ok(Some(metadata)),
        Ok(_) => Err(MoveAsideError::InvalidPaths),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}
fn same_file(first: &Metadata, second: &Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        (first.dev(), first.ino()) == (second.dev(), second.ino())
    }
    #[cfg(not(unix))]
    {
        let _ = (first, second);
        false
    }
}
async fn check_paths(
    source: &Path,
    destination: &Path,
) -> Result<(Option<Metadata>, Option<Metadata>), MoveAsideError> {
    if source == destination {
        return Err(MoveAsideError::InvalidPaths);
    }
    let source_metadata = metadata(source).await?;
    let destination_metadata = metadata(destination).await?;
    if let (Some(source_metadata), Some(destination_metadata)) =
        (&source_metadata, &destination_metadata)
    {
        if tokio::fs::canonicalize(source).await? == tokio::fs::canonicalize(destination).await? {
            return Err(MoveAsideError::InvalidPaths);
        }
        if !same_file(source_metadata, destination_metadata) {
            return Err(MoveAsideError::DestinationOccupied);
        }
    }
    Ok((source_metadata, destination_metadata))
}
async fn verify_closed(path: &Path, database: DatabaseId) -> Result<(), MoveAsideError> {
    let mut connection = sqlx_turso::TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .foreign_keys(false)
        .connect()
        .await
        .map_err(OpenError::from)?;
    let result: Result<(), MoveAsideError> = async {
        connection
            .execute("PRAGMA capture_data_changes_conn('off')")
            .await
            .map_err(OpenError::from)?;
        let record = read_file_record(&mut connection)
            .await?
            .ok_or(MoveAsideError::InvalidIdentity)?;
        require_rejected(&record, database)?;
        // Opening a terminal file for identity verification can create Turso's
        // WAL header. Truncate it through the same checked native checkpoint.
        checkpoint_for_publish(&mut connection).await?;
        Ok(())
    }
    .await;
    let closed = connection.close().await.map_err(OpenError::from);
    result?;
    closed?;
    ensure_empty_wal(path).await?;
    Ok(())
}
async fn sync_directory(path: &Path) -> Result<(), MoveAsideError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    tokio::fs::File::open(parent).await?.sync_all().await?;
    Ok(())
}
async fn move_file(
    database: DatabaseId,
    source: &Path,
    destination: &Path,
    probe: &MoveAsideProbe,
) -> Result<(), MoveAsideError> {
    let (source_metadata, destination_metadata) = check_paths(source, destination).await?;
    match (source_metadata, destination_metadata) {
        (None, None) => return Err(MoveAsideError::SourceMissing),
        (None, Some(_)) => {
            verify_closed(destination, database).await?;
            sync_directory(destination).await?;
            sync_directory(source).await?;
            return Ok(());
        }
        (Some(_), Some(_)) => {
            // The existing names are the same inode, left by an interrupted
            // link/unlink. Both sidecars must already be free of committed WAL.
            verify_closed(destination, database).await?;
            ensure_empty_wal(source).await?;
        }
        (Some(_), None) => {
            verify_closed(source, database).await?;
            probe.at(MoveAsidePhase::BeforeLink).await;
            match tokio::fs::hard_link(source, destination).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    check_paths(source, destination).await?;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    sync_directory(destination).await?;
    probe.at(MoveAsidePhase::AfterLink).await;
    tokio::fs::remove_file(source).await?;
    probe.at(MoveAsidePhase::AfterUnlink).await;
    sync_directory(source).await?;
    sync_directory(destination).await?;
    Ok(())
}
