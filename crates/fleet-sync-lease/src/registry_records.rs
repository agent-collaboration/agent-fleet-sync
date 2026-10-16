use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DatabaseLifecycle {
    Registered { origin: Origin },
    Unheld { import_digest: ImportDigest },
    Retiring { attempt: RequestId },
    Retired { at: SyncTime },
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DatabaseRecord {
    pub database: DatabaseId,
    pub lifecycle: DatabaseLifecycle,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegistryLease {
    pub database: DatabaseId,
    pub holder: PeerId,
    pub epoch: Epoch,
    pub expires_at: SyncTime,
    pub last_renewed_at: SyncTime,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EpochHistory {
    pub database: DatabaseId,
    pub epoch: Epoch,
    pub holder: PeerId,
    pub granted_at: SyncTime,
    pub ended_at: Option<ReplicationPosition>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RenewalAttempt {
    pub request: RenewRequest,
    pub holder: PeerId,
    pub answer: RenewAnswer,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TransferIntent {
    pub request: HolderChangeRequest,
    pub new_epoch: Option<Epoch>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransferRecord {
    pub intent: TransferIntent,
    pub outcome: Option<HolderChangeAnswer>,
}
impl TransferIntent {
    pub(crate) fn database(&self) -> DatabaseId {
        match self.request {
            HolderChangeRequest::Move { database, .. }
            | HolderChangeRequest::Takeover { database, .. }
            | HolderChangeRequest::Claim { database, .. } => database,
        }
    }
    pub(crate) fn attempt(&self) -> TransferId {
        match self.request {
            HolderChangeRequest::Move { attempt, .. }
            | HolderChangeRequest::Takeover { attempt, .. }
            | HolderChangeRequest::Claim { attempt, .. } => attempt,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "request",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum LifecycleIntent {
    Register(Registration),
    Import(Import),
    Retire(Retirement),
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "answer",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum LifecycleOutcome {
    Registration(RegistrationAnswer),
    Import(ImportAnswer),
    Retirement(RetirementAnswer),
    Failed { reason: FailureCode },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleAttempt {
    pub intent: LifecycleIntent,
    pub outcome: Option<LifecycleOutcome>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreviewRecord {
    pub database: DatabaseId,
    pub preview: TakeoverPreview,
}
impl LifecycleIntent {
    pub(crate) fn database(&self) -> DatabaseId {
        match self {
            Self::Register(request) => request.database,
            Self::Import(request) => request.database,
            Self::Retire(request) => request.database,
        }
    }
    pub(crate) fn attempt(&self) -> RequestId {
        match self {
            Self::Register(request) => request.attempt,
            Self::Import(request) => request.attempt,
            Self::Retire(request) => request.attempt,
        }
    }
}
