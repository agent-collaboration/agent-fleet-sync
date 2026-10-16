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
    pub holder: PeerId,
    pub epoch: Epoch,
    pub acked: ReplicationPosition,
    pub schema: SchemaIdentity,
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
    },
    Takeover {
        attempt: TransferId,
        database: DatabaseId,
        preview: TakeoverPreview,
        to: PeerId,
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
    TransferInProgress {
        transfer: TransferId,
    },
    PreviewStale,
    MigrationPending,
    SchemaTooNew {
        store: SchemaIdentity,
        supported: Option<u64>,
    },
    AlreadyHeld,
    NotFound,
    Unavailable,
    HolderUnreachable,
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
    Granted {
        epoch: Epoch,
        expires_at: SyncTime,
        start: ReplicationPosition,
    },
    Pending {
        transfer: TransferId,
    },
    Refused(HolderChangeRefusal),
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
    Renew(RenewRequest),
    HolderChange(HolderChangeRequest),
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
    Renew(RenewAnswer),
    HolderChange(HolderChangeAnswer),
}
