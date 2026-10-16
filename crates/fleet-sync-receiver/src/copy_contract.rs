use crate::StoreError;
use fleet_sync_protocol::CopyStatus;
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

/// The retired marker retains the fence and position after user records are deleted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "copy",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum CopyRecord {
    Seeding {
        status: CopyStatus,
        expected_position: Option<fleet_sync_protocol::ReplicationPosition>,
    },
    Following(CopyStatus),
    Retired(CopyStatus),
}

impl CopyRecord {
    pub(crate) fn status(&self) -> &CopyStatus {
        match self {
            Self::Seeding { status, .. } | Self::Following(status) | Self::Retired(status) => {
                status
            }
        }
    }
}
