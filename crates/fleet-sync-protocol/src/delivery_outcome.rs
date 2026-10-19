use crate::{DeliverySeq, ItemDigest, ItemId};
use serde::{Deserialize, Serialize};

pub const MAX_RECIPIENT_REFUSAL_CODE_BYTES: usize = 64;

/// Consumer-owned refusal identity. Delivery carries the code without assigning
/// meaning; local descriptive detail belongs to the consumer's projection.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RecipientRefusalCode(String);

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("recipient refusal code must contain 1 to 64 ASCII letters or digits")]
pub struct RecipientRefusalCodeError;

impl RecipientRefusalCode {
    pub fn new(value: impl Into<String>) -> Result<Self, RecipientRefusalCodeError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_RECIPIENT_REFUSAL_CODE_BYTES
            || !value.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            return Err(RecipientRefusalCodeError);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for RecipientRefusalCode {
    type Error = RecipientRefusalCodeError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<RecipientRefusalCode> for String {
    fn from(value: RecipientRefusalCode) -> Self {
        value.0
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
pub enum DeliveryRefusal {
    RecipientRefused { code: RecipientRefusalCode },
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
