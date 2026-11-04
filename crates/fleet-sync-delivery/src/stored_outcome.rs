use crate::{DeliveryError, custody_record::*};
use fleet_sync_protocol::*;
use serde::de::DeserializeOwned;

pub(crate) fn required(
    value: Option<String>,
    field: &'static str,
) -> Result<String, DeliveryError> {
    value.ok_or(DeliveryError::Corrupt { field })
}

pub(crate) fn stored_enum<T: DeserializeOwned>(
    value: Option<String>,
    field: &'static str,
) -> Result<T, DeliveryError> {
    decode(Some(encode(&required(value, field)?)?), field)
}

pub(crate) fn optional_number(
    value: Option<String>,
    class: Option<String>,
    field: &'static str,
) -> Result<Option<u64>, DeliveryError> {
    if class.as_deref() == Some("null") && value.is_none() {
        return Ok(None);
    }
    integer(value, class, field).map(Some)
}

pub(crate) fn stored_time(
    value: Option<String>,
    class: Option<String>,
    field: &'static str,
) -> Result<Option<SyncTime>, DeliveryError> {
    if class.as_deref() == Some("null") && value.is_none() {
        return Ok(None);
    }
    if class.as_deref() != Some("integer") {
        return Err(DeliveryError::Corrupt { field });
    }
    required(value, field)?
        .parse::<i64>()
        .map(SyncTime::from_unix_millis)
        .map(Some)
        .map_err(|_| DeliveryError::Corrupt { field })
}

pub(crate) fn stored_bool(
    value: Option<String>,
    class: Option<String>,
    field: &'static str,
) -> Result<bool, DeliveryError> {
    match integer(value, class, field)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(DeliveryError::Corrupt { field }),
    }
}

pub(crate) struct RefusalFields {
    pub kind: Option<String>,
    pub code: Option<String>,
    pub failure: Option<String>,
    pub size_field: Option<String>,
    pub digest: Option<String>,
    pub item_id: Option<String>,
}
impl RefusalFields {
    pub fn decode(self) -> Result<DeliveryRefusal, DeliveryError> {
        let field = "stored refusal";
        Ok(match self.kind.as_deref() {
            Some("recipientRefused") => DeliveryRefusal::RecipientRefused {
                code: stored_enum(self.code, field)?,
            },
            Some("processingFailed") => DeliveryRefusal::ProcessingFailed {
                kind: stored_enum(self.failure, field)?,
            },
            Some("tooLarge") => DeliveryRefusal::TooLarge {
                field: stored_enum(self.size_field, field)?,
            },
            Some("itemConflict") => DeliveryRefusal::ItemConflict {
                existing: stored_enum(self.digest, field)?,
            },
            Some("cancelAlreadyRequested") => DeliveryRefusal::CancelAlreadyRequested {
                existing: DeliveryItemRef {
                    item: stored_enum(self.item_id, field)?,
                    digest: stored_enum(self.digest, field)?,
                },
            },
            Some("mailboxFull") => DeliveryRefusal::MailboxFull,
            Some("recipientUnknown") => DeliveryRefusal::RecipientUnknown,
            Some("recipientRevoked") => DeliveryRefusal::RecipientRevoked,
            Some("senderNotEnrolled") => DeliveryRefusal::SenderNotEnrolled,
            Some("droppedByPolicy") => DeliveryRefusal::DroppedByPolicy,
            Some("cancelSenderMismatch") => DeliveryRefusal::CancelSenderMismatch,
            Some("notThisMachine") => DeliveryRefusal::NotThisMachine,
            Some("notRecipient") => DeliveryRefusal::NotRecipient,
            Some("notSubmitter") => DeliveryRefusal::NotSubmitter,
            Some("tooOld") => DeliveryRefusal::TooOld,
            _ => return Err(DeliveryError::Corrupt { field }),
        })
    }
}

pub(crate) fn stored_cancel(
    kind: Option<String>,
    at: Option<String>,
) -> Result<Option<CancelResult>, DeliveryError> {
    Ok(match kind.as_deref() {
        None => None,
        Some("pending") => Some(CancelResult::Pending),
        Some("prevented") => Some(CancelResult::Prevented),
        Some("tooLate") => Some(CancelResult::TooLate {
            at: stored_enum(at, "cancel too late")?,
        }),
        _ => {
            return Err(DeliveryError::Corrupt {
                field: "cancel outcome",
            });
        }
    })
}

pub(crate) fn stored_status<A: DeserializeOwned>(
    kind: Option<String>,
    _sequence: Option<u64>,
    acceptance: Option<String>,
    refusal: RefusalFields,
) -> Result<DeliveryStatus<A>, DeliveryError> {
    let field = "stored status";
    Ok(match kind.as_deref() {
        Some("queued") => DeliveryStatus::Queued,
        Some("sent") => DeliveryStatus::Sent,
        Some("atRecipient") => DeliveryStatus::AtRecipient,
        Some("held") => DeliveryStatus::Held,
        Some("delivered") => DeliveryStatus::Delivered {
            acceptance: decode(acceptance, field)?,
        },
        Some("uncertain") => DeliveryStatus::Uncertain,
        Some("expired") => DeliveryStatus::Expired,
        Some("expiredAtUpgrade") => DeliveryStatus::ExpiredAtUpgrade,
        Some("cancelled") => DeliveryStatus::Cancelled,
        Some("refused") => DeliveryStatus::Refused {
            reason: refusal.decode()?,
        },
        _ => return Err(DeliveryError::Corrupt { field }),
    })
}
