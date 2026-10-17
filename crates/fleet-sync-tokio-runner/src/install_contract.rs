use crate::ObjectKey;
use durable_steps::{OperationFailure, StoreError, Uuid};
use fleet_sync_protocol::{DatabaseId, ReceiverId};
use fleet_sync_receiver::SnapshotInstall;
use serde::{Deserialize, Serialize};

/// Stable identities only. Snapshot rows remain in the staging file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InstallRequest {
    pub attempt: Uuid,
    pub target: ObjectKey,
    pub database: DatabaseId,
    pub source: ReceiverId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum InstallProgress {
    Reserved,
    Pinned { snapshot: SnapshotInstall },
    Published { snapshot: SnapshotInstall },
}

/// The canonical install outcome. An engine row records execution diagnostics;
/// callers obtain install outcomes from this record, including after restart.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum InstallStatus {
    Running {
        progress: InstallProgress,
    },
    Ready {
        snapshot: SnapshotInstall,
    },
    Superseded {
        by: Uuid,
        progress: InstallProgress,
    },
    Failed {
        reason: OperationFailure,
        progress: InstallProgress,
    },
}
impl InstallStatus {
    pub(crate) fn published(&self) -> Option<&SnapshotInstall> {
        match self {
            Self::Ready { snapshot }
            | Self::Running {
                progress: InstallProgress::Published { snapshot },
            }
            | Self::Superseded {
                progress: InstallProgress::Published { snapshot },
                ..
            }
            | Self::Failed {
                progress: InstallProgress::Published { snapshot },
                ..
            } => Some(snapshot),
            _ => None,
        }
    }
    pub(crate) fn snapshot(&self) -> Option<&SnapshotInstall> {
        match self {
            Self::Ready { snapshot }
            | Self::Running {
                progress:
                    InstallProgress::Pinned { snapshot } | InstallProgress::Published { snapshot },
            }
            | Self::Superseded {
                progress:
                    InstallProgress::Pinned { snapshot } | InstallProgress::Published { snapshot },
                ..
            }
            | Self::Failed {
                progress:
                    InstallProgress::Pinned { snapshot } | InstallProgress::Published { snapshot },
                ..
            } => Some(snapshot),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InstallRecord {
    pub request: InstallRequest,
    pub install_id: Uuid,
    pub generation: u64,
    pub status: InstallStatus,
}
impl InstallRecord {
    pub(crate) fn validate(&self) -> Result<(), StoreError> {
        if self.request.attempt.is_nil() || self.install_id.is_nil() || self.generation == 0 {
            return Err(corrupt("invalid install identity or generation"));
        }
        if let Some(snapshot) = self.status.snapshot()
            && (snapshot.install_id != self.install_id
                || snapshot.database != self.request.database)
        {
            return Err(corrupt("snapshot identity differs from its install record"));
        }
        if matches!(self.status, InstallStatus::Superseded { by, .. } if by.is_nil() || by == self.install_id)
        {
            return Err(corrupt("invalid superseding install identity"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum InstallFileAnswer {
    Published { snapshot: SnapshotInstall },
    Ready { snapshot: SnapshotInstall },
    Superseded { by: Uuid },
    Failed { reason: OperationFailure },
}

pub(crate) fn corrupt(message: impl Into<String>) -> StoreError {
    StoreError::Corrupt {
        field: "snapshot_install".into(),
        message: message.into(),
    }
}
