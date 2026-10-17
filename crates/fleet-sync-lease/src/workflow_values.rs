use durable_steps::storage::CallReach;
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "phase",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum HolderChangePhase {
    Requested,
    Releasing,
    Released { final_position: ReplicationPosition },
    Reserved,
    Fencing,
    Cancelling,
    Ended,
}

impl HolderChangePhase {
    pub(crate) fn cancellable(&self) -> bool {
        matches!(
            self,
            Self::Requested
                | Self::Releasing
                | Self::Released { .. }
                | Self::Reserved
                | Self::Cancelling
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PhaseEntry {
    pub phase: HolderChangePhase,
    pub entered_at: SyncTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CancelReason(String);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("cancel reason must contain 1 to 512 bytes")]
pub struct CancelReasonError;

impl CancelReason {
    pub fn new(value: impl Into<String>) -> Result<Self, CancelReasonError> {
        let value = value.into();
        if value.is_empty() || value.len() > 512 {
            return Err(CancelReasonError);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for CancelReason {
    type Error = CancelReasonError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<CancelReason> for String {
    fn from(value: CancelReason) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CancelHolderChange {
    pub attempt: TransferId,
    pub by: ActorRef,
    pub reason: CancelReason,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CancelRecord {
    pub by: ActorRef,
    pub reason: CancelReason,
    pub at: SyncTime,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum CancelAnswer {
    Accepted {
        recorded: CancelRecord,
        phase: HolderChangePhase,
    },
    TooLate {
        phase: HolderChangePhase,
    },
    Ended(HolderChangeAnswer),
    NotFound,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum WaitingOn {
    Holder(PeerId),
    Copy(ReceiverId),
    Gate,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Waiting {
    pub on: WaitingOn,
    pub since: SyncTime,
    pub last_seen: Option<SyncTime>,
    pub reach: CallReach,
    pub attempt: u32,
    pub next_attempt_at: SyncTime,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HolderChangeView {
    pub request: HolderChangeRequest,
    pub requested_by: ActorRef,
    pub phase: HolderChangePhase,
    pub history: Vec<PhaseEntry>,
    pub waiting: Option<Waiting>,
    pub settlement_owed: bool,
    pub follow_up_failure: Option<FailureCode>,
    pub cancel: Option<CancelRecord>,
    pub outcome: Option<HolderChangeAnswer>,
}
