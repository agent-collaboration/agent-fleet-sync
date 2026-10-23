use crate::{
    OpenError, RetainedAuthority, RetainedFileOrigin, file_admission::FileAdmission,
    writer_file_record::read_file_record,
};
use fleet_sync_receiver::SnapshotInstallState;
use sqlx_core::connection::{ConnectOptions, Connection};
use sqlx_turso::TursoConnectOptions;
use std::path::Path;

/// Validated writer metadata, including a bootstrap that has no local history yet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeFileInspection {
    pub origin: RetainedFileOrigin,
    pub bootstrap: Option<SnapshotInstallState>,
    /// Persisted loss of authority. Absence does not establish a current lease.
    pub authority: Option<RetainedAuthority>,
}

/// Inspect an existing managed file without migrations, recovery or a SQL reader.
///
/// The caller must exclude writers in other processes and writable handles opened
/// outside this library. This validates the stored writer record, not the full
/// retained-file format or the availability of consumer tables and local history.
pub async fn inspect_native_file(path: &Path) -> Result<NativeFileInspection, OpenError> {
    let _admission = FileAdmission::retained(path).await?;
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .read_only(true)
        .connect()
        .await?;
    let record = read_file_record(&mut connection).await;
    // Close before releasing the file admission, including on corrupt metadata.
    let closed = connection.close().await;
    let record = record?.ok_or_else(|| OpenError::corrupt("writer.record"))?;
    closed?;
    Ok(NativeFileInspection {
        origin: RetainedFileOrigin {
            database: record.database,
            epoch: record.epoch,
            start: record.start,
            kind: record.origin,
        },
        authority: record
            .authority
            .as_ref()
            .and_then(|authority| authority.retained_authority(record.epoch, record.peer)),
        bootstrap: record.bootstrap,
    })
}
