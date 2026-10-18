use crate::{ReceiverError, StoreError};
use fleet_sync_protocol::InstallId;
use fleet_sync_protocol::{
    DatabaseId, HookRefusal, PageDigest, ReplicationPosition, SchemaIdentity, SnapshotCursor,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotInstall {
    pub install_id: InstallId,
    pub database: DatabaseId,
    pub position: ReplicationPosition,
    pub schema: SchemaIdentity,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InstallPageReceipt {
    pub install_id: InstallId,
    pub page: u32,
    pub digest: PageDigest,
    pub rows: u64,
    pub next: Option<SnapshotCursor>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotInstallState {
    pub install: SnapshotInstall,
    pub verified: bool,
    pub ready: bool,
}
pub enum SnapshotReady<F> {
    Ready {
        installed: crate::InstalledSnapshot,
        follow_ups: Vec<F>,
    },
    AlreadyReady {
        installed: crate::InstalledSnapshot,
    },
}
#[derive(Debug, thiserror::Error)]
pub enum SnapshotInstallError {
    #[error(transparent)]
    Storage(#[from] StoreError),
    #[error(transparent)]
    Rules(#[from] ReceiverError),
    #[error("snapshot install identity conflicts with the stored marker")]
    IdentityConflict,
    #[error("snapshot install is not prepared")]
    NotPrepared,
    #[error("snapshot page {page} has conflicting contents")]
    PageConflict { page: u32 },
    #[error("snapshot pages are incomplete")]
    Incomplete,
    #[error("snapshot schema differs from the declared identity")]
    SchemaMismatch,
    #[error("snapshot manifest differs from the installed rows")]
    ManifestMismatch,
    #[error("snapshot is not verified")]
    NotVerified,
    #[error("snapshot position {offered:?} precedes the retained fence end {minimum:?}")]
    BelowFence {
        offered: ReplicationPosition,
        minimum: ReplicationPosition,
    },
    #[error("snapshot fence conflicts with the target holder")]
    FenceConflict,
    #[error("an authoritative copy cannot be replaced by a follower snapshot")]
    AuthoritativeCopy,
    #[error("a retired copy cannot be replaced")]
    Retired,
    #[error("snapshot hook refused: {refusal:?}")]
    Hook { refusal: HookRefusal },
}
impl From<sqlx_core::Error> for SnapshotInstallError {
    fn from(error: sqlx_core::Error) -> Self {
        Self::Storage(crate::sql_transport::sql_error(error))
    }
}
