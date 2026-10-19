use crate::{DeliverySeq, ItemDigest, ItemId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DeliveryRefusal {
    ProcessingFailed { kind: DeliveryFailureKind },
    TooLarge { field: DeliverySizeField },
    MailboxFull,
    ItemConflict { existing: ItemDigest },
    RecipientUnknown,
    RecipientRevoked,
    DroppedByPolicy,
    CancelSenderMismatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeliveryFailureKind {
    CustodyMissing,
    CustodyUnresolvable,
    CustodyCorrupt,
    DigestMismatch,
    VersionMismatch,
    Nondeterministic,
    InvalidPayload,
    ExecutionFailed,
}
impl DeliveryFailureKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CustodyMissing => "custodyMissing",
            Self::CustodyUnresolvable => "custodyUnresolvable",
            Self::CustodyCorrupt => "custodyCorrupt",
            Self::DigestMismatch => "digestMismatch",
            Self::VersionMismatch => "versionMismatch",
            Self::Nondeterministic => "nondeterministic",
            Self::InvalidPayload => "invalidPayload",
            Self::ExecutionFailed => "executionFailed",
        }
    }
    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "custodyMissing" => Some(Self::CustodyMissing),
            "custodyUnresolvable" => Some(Self::CustodyUnresolvable),
            "custodyCorrupt" => Some(Self::CustodyCorrupt),
            "digestMismatch" => Some(Self::DigestMismatch),
            "versionMismatch" => Some(Self::VersionMismatch),
            "nondeterministic" => Some(Self::Nondeterministic),
            "invalidPayload" => Some(Self::InvalidPayload),
            "executionFailed" => Some(Self::ExecutionFailed),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DeliveryStatus<A> {
    Queued,
    Sent { seq: DeliverySeq },
    AtRecipient,
    Held,
    Delivered { acceptance: A },
    Uncertain,
    Expired,
    Cancelled,
    Refused { reason: DeliveryRefusal },
}
impl<A> DeliveryStatus<A> {
    pub const fn is_terminal(&self) -> bool {
        self.rank() == 4
    }
    pub const fn rank(&self) -> u8 {
        match self {
            Self::Queued => 0,
            Self::Sent { .. } => 1,
            Self::AtRecipient => 2,
            Self::Held => 3,
            Self::Delivered { .. }
            | Self::Uncertain
            | Self::Expired
            | Self::Cancelled
            | Self::Refused { .. } => 4,
        }
    }
    pub fn may_advance_to(&self, newer: &Self) -> bool {
        newer.rank() > self.rank()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TooLateAt {
    Delivered,
    Uncertain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum CancelResult {
    Pending,
    Prevented,
    TooLate { at: TooLateAt },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum RecipientOutcome<A> {
    Held,
    Delivered { acceptance: A },
    Uncertain,
    Expired,
    Prevented { cancel: ItemId },
    TooLate { cancel: ItemId, at: TooLateAt },
    Dropped,
    Refused { reason: DeliveryRefusal },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeliverySizeField {
    Sender,
    Address,
    Acceptance,
    StatusRoute,
}
