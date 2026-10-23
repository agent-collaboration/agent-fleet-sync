use crate::{
    DatabaseId, Epoch, PeerId, ReplicationPosition, RequestId, SchemaIdentity, SyncTime, TransferId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LeaseState {
    Held,
    Expired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeaseStatus {
    pub database: DatabaseId,
    pub epoch: Epoch,
    pub holder: PeerId,
    pub state: LeaseState,
    pub expires_at: SyncTime,
    pub remaining_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RenewRequest {
    pub attempt: RequestId,
    pub database: DatabaseId,
    pub epoch: Epoch,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RenewResult {
    pub epoch: Epoch,
    pub expires_at: SyncTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum LeaseRefusal {
    StaleEpoch {
        current_epoch: Epoch,
        epoch_ended_at: ReplicationPosition,
    },
    NotWriter {
        current: Option<Epoch>,
    },
    NoAuthoritativeCopy,
    EpochUnprovable,
    NotRegistered,
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum RenewAnswer {
    Renewed {
        recorded: RenewResult,
        current: LeaseStatus,
        transfer_in_progress: Option<TransferId>,
        replayed: bool,
    },
    Refused(LeaseRefusal),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TakeoverPreview {
    pub token: crate::PreviewToken,
    pub fence: Epoch,
    pub acked: ReplicationPosition,
    pub holder: PeerId,
    pub last_seen: Option<SyncTime>,
}

/// The caller's reason, preserved verbatim in the holder-change record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct HolderChangeReason(String);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("holder-change reason must contain 1 to 16384 UTF-8 bytes")]
pub struct HolderChangeReasonError;

impl HolderChangeReason {
    pub fn new(value: impl Into<String>) -> Result<Self, HolderChangeReasonError> {
        let value = value.into();
        if value.is_empty() || value.len() > 16 * 1024 {
            return Err(HolderChangeReasonError);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for HolderChangeReason {
    type Error = HolderChangeReasonError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<HolderChangeReason> for String {
    fn from(value: HolderChangeReason) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum HolderChangeRequest {
    Move {
        attempt: TransferId,
        database: DatabaseId,
        from_epoch: Epoch,
        to: PeerId,
        reason: HolderChangeReason,
    },
    Takeover {
        attempt: TransferId,
        database: DatabaseId,
        preview: TakeoverPreview,
        to: PeerId,
        reason: HolderChangeReason,
    },
    Claim {
        attempt: TransferId,
        database: DatabaseId,
        claimant: PeerId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum HolderChangeRefusal {
    HolderStale {
        last_renewed_at: SyncTime,
    },
    GateRefused {
        reason: crate::GateReason,
    },
    TransferInProgress {
        transfer: TransferId,
    },
    PreviewStale,
    MigrationPending {
        migration: u64,
        state: crate::MigrationState,
    },
    SchemaTooNew {
        store: SchemaIdentity,
        supported: Option<u64>,
    },
    AlreadyHeld {
        holder: PeerId,
    },
    NotFound,
    Unavailable,
    EpochUnprovable,
    Superseded {
        by: TransferId,
    },
    HolderUnreachable {
        last_seen: Option<SyncTime>,
    },
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum HolderChangeAnswer {
    Cancelled {
        by: crate::ActorRef,
    },
    Granted {
        epoch: Epoch,
        expires_at: SyncTime,
        start: ReplicationPosition,
    },
    Pending {
        transfer: TransferId,
    },
    Refused(HolderChangeRefusal),
    Failed {
        reason: crate::FailureCode,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum LeaseRequest {
    Register(crate::Registration),
    Renew(RenewRequest),
    Claim {
        attempt: RequestId,
        database: DatabaseId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all_fields = "camelCase",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum LeaseAnswer {
    Register(crate::RegistrationAnswer),
    Renew(RenewAnswer),
    Claim(HolderChangeAnswer),
}
