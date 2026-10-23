use crate::StoreError;
use fleet_sync_protocol::{CopyStatus, DatabaseId, SyncTime};
use serde::{Deserialize, Serialize};

pub(crate) fn validate_copy_status(status: &CopyStatus) -> Result<(), StoreError> {
    if status.fence < status.acked.epoch
        || status.start > status.acked
        || (status.holder.is_none() && status.fence.value() != 1)
        || status.schema.fingerprint.is_empty()
    {
        return Err(StoreError::Corrupt {
            field: "copy state".into(),
        });
    }
    Ok(())
}

/// Live state and the minimal permanent marker have different retention rules.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "copy",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum CopyRecord {
    Starting {
        status: CopyStatus,
        install_id: fleet_sync_protocol::InstallId,
    },
    Seeding {
        status: CopyStatus,
        expected_position: Option<fleet_sync_protocol::ReplicationPosition>,
    },
    Following(CopyStatus),
    Retired {
        database: DatabaseId,
        retired_at: SyncTime,
    },
}

impl CopyRecord {
    pub(crate) fn database(&self) -> DatabaseId {
        match self {
            Self::Starting { status, .. }
            | Self::Seeding { status, .. }
            | Self::Following(status) => status.database,
            Self::Retired { database, .. } => *database,
        }
    }
    pub(crate) fn validate(&self) -> Result<(), StoreError> {
        match self {
            Self::Starting { status, .. }
                if status.role != fleet_sync_protocol::CopyRole::Follower =>
            {
                Err(StoreError::Corrupt {
                    field: "starting copy marker".into(),
                })
            }
            Self::Starting { status, .. }
            | Self::Seeding { status, .. }
            | Self::Following(status) => validate_copy_status(status),
            Self::Retired { database, .. } if database.as_uuid().is_nil() => {
                Err(StoreError::Corrupt {
                    field: "retired database".into(),
                })
            }
            Self::Retired { .. } => Ok(()),
        }
    }
}
