use crate::{DeliveryError, DeliveryLimits};
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<String, DeliveryError> {
    serde_json::to_string(value).map_err(|error| DeliveryError::Encoding(error.to_string()))
}
pub(crate) fn decode<T: DeserializeOwned>(
    value: Option<String>,
    field: &'static str,
) -> Result<T, DeliveryError> {
    serde_json::from_str(&value.ok_or(DeliveryError::Corrupt { field })?)
        .map_err(|_| DeliveryError::Corrupt { field })
}
pub(crate) fn copy_wire<T: Serialize + DeserializeOwned>(value: &T) -> Result<T, DeliveryError> {
    decode(Some(encode(value)?), "copied wire value")
}
pub(crate) fn sql_error(error: sqlx_core::error::Error) -> DeliveryError {
    DeliveryError::Storage(error.to_string())
}
pub(crate) fn item_key(item: ItemId) -> String {
    item.as_uuid().to_string()
}
pub(crate) fn stream_key(stream: DeliveryStream) -> String {
    match stream {
        DeliveryStream::Items => "items",
        DeliveryStream::Controls => "controls",
    }
    .into()
}
pub(crate) fn integer(
    value: Option<String>,
    class: Option<String>,
    field: &'static str,
) -> Result<u64, DeliveryError> {
    if class.as_deref() != Some("integer") {
        return Err(DeliveryError::Corrupt { field });
    }
    value
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value <= i64::MAX as u64)
        .ok_or(DeliveryError::Corrupt { field })
}
pub(crate) fn next_sequence(value: DeliverySeq) -> Result<DeliverySeq, DeliveryError> {
    value
        .next()
        .filter(|next| next.value() <= i64::MAX as u64)
        .ok_or(DeliveryError::SequenceExhausted)
}
pub(crate) fn next_version(value: StatusVersion) -> Result<StatusVersion, DeliveryError> {
    value
        .next()
        .filter(|next| next.value() <= i64::MAX as u64)
        .ok_or(DeliveryError::SequenceExhausted)
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields,
    bound = ""
)]
pub(crate) enum MailRecord<D: DeliveryTypes> {
    Live(LiveMail<D>),
    Compacted(CompactedMail<D::Acceptance>),
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, bound = "")]
pub(crate) struct LiveMail<D: DeliveryTypes> {
    pub id: ItemId,
    pub digest: ItemDigest,
    pub origin: PeerId,
    pub status_target: CurrentStatusTarget<D::StatusRoute>,

    pub stream: DeliveryStream,
    pub seq: DeliverySeq,
    pub payload: Option<DeliveryEnvelope<D>>,
    pub status: DeliveryStatus<D::Acceptance>,
    pub version: StatusVersion,
    pub last_report_seq: ReportSeq,
    pub accepted_at: SyncTime,
    pub fetched: bool,
    pub released_at: Option<SyncTime>,
    pub cancel: Option<CancelResult>,
    pub cancel_target: Option<ItemId>,
    pub settlement_bytes: u32,
    pub capacity_slot: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CurrentStatusTarget<R> {
    pub peer: PeerId,
    pub route: R,
    pub rank: Option<u64>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CompactedMail<A> {
    pub id: ItemId,
    pub digest: ItemDigest,
    pub status: DeliveryStatus<A>,
    pub version: StatusVersion,
    pub cancel: Option<CancelResult>,
}
impl<D: DeliveryTypes> MailRecord<D> {
    pub fn id(&self) -> ItemId {
        match self {
            Self::Live(row) => row.id,
            Self::Compacted(row) => row.id,
        }
    }
    pub fn digest(&self) -> &ItemDigest {
        match self {
            Self::Live(row) => &row.digest,
            Self::Compacted(row) => &row.digest,
        }
    }
    pub fn status(&self) -> &DeliveryStatus<D::Acceptance> {
        match self {
            Self::Live(row) => &row.status,
            Self::Compacted(row) => &row.status,
        }
    }
    pub fn version(&self) -> StatusVersion {
        match self {
            Self::Live(row) => row.version,
            Self::Compacted(row) => row.version,
        }
    }
    pub fn cancel(&self) -> Option<CancelResult> {
        match self {
            Self::Live(row) => row.cancel,
            Self::Compacted(row) => row.cancel,
        }
    }
    pub fn validate(&self, owner: PeerId) -> Result<(), DeliveryError> {
        if self.version().value() == 0 || self.version().value() > i64::MAX as u64 {
            return Err(DeliveryError::Corrupt {
                field: "mail status version",
            });
        }
        match self {
            Self::Live(row) => {
                if row.seq.value() == 0
                    || row.last_report_seq.value() > i64::MAX as u64
                    || row.seq.value() > i64::MAX as u64
                    || row.status.rank() == 0
                    || row.released_at.is_some() && row.payload.is_some()
                    || row.released_at.is_none()
                        && row.payload.is_none()
                        && !row.status.is_terminal()
                    || (row.stream == DeliveryStream::Controls) != (row.cancel_target.is_some())
                    || (row.cancel_target.is_some()) != (row.cancel.is_some())
                {
                    return Err(DeliveryError::Corrupt {
                        field: "mail custody state",
                    });
                }
                if let Some(payload) = &row.payload
                    && (payload.id() != row.id
                        || payload.digest() != &row.digest
                        || payload.stream() != row.stream
                        || payload.recipient().peer != owner
                        || row.cancel_target
                            != match payload {
                                DeliveryEnvelope::Item(_) => None,
                                DeliveryEnvelope::Cancel(cancel) => Some(cancel.target()),
                            })
                {
                    return Err(DeliveryError::Corrupt {
                        field: "mail custody payload",
                    });
                }
                if let DeliveryStatus::Sent { seq } = row.status
                    && seq != row.seq
                {
                    return Err(DeliveryError::Corrupt {
                        field: "mail sent sequence",
                    });
                }
            }
            Self::Compacted(row) => {
                if !row.status.is_terminal() {
                    return Err(DeliveryError::Corrupt {
                        field: "compacted final status",
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MailState {
    pub peer: PeerId,
    pub items: DeliverySeq,
    pub controls: DeliverySeq,
    pub received_items: DeliverySeq,
    pub received_controls: DeliverySeq,
    pub closed: bool,
}
impl MailState {
    pub fn new(peer: PeerId) -> Self {
        Self {
            peer,
            items: DeliverySeq::new(0),
            controls: DeliverySeq::new(0),
            received_items: DeliverySeq::new(0),
            received_controls: DeliverySeq::new(0),
            closed: false,
        }
    }
    pub fn allocate(&mut self, stream: DeliveryStream) -> Result<DeliverySeq, DeliveryError> {
        let head = match stream {
            DeliveryStream::Items => &mut self.items,
            DeliveryStream::Controls => &mut self.controls,
        };
        *head = next_sequence(*head)?;
        Ok(*head)
    }
    pub fn head(&self, stream: DeliveryStream) -> DeliverySeq {
        match stream {
            DeliveryStream::Items => self.items,
            DeliveryStream::Controls => self.controls,
        }
    }
}

pub(crate) fn payload_bytes<D: DeliveryTypes>(
    payload: &Option<DeliveryEnvelope<D>>,
) -> Result<u64, DeliveryError> {
    match payload {
        Some(payload) => {
            u64::try_from(encode(payload)?.len()).map_err(|_| DeliveryError::SequenceExhausted)
        }
        None => Ok(0),
    }
}
pub(crate) fn fits_mailbox(
    limits: DeliveryLimits,
    items: u64,
    bytes: u64,
    new_payload: u64,
) -> bool {
    items < limits.mailbox_items
        && new_payload
            .checked_add(u64::from(limits.settlement_allowance))
            .and_then(|size| bytes.checked_add(size))
            .is_some_and(|total| total <= limits.mailbox_payload_bytes)
}

pub(crate) fn delivery_header<D: DeliveryTypes>(
    id: ItemId,
    sender: &D::Sender,
    recipient: &Recipient<D::Address>,
    valid_until: Option<SyncTime>,
    digest: &ItemDigest,
) -> Result<DeliveryHeader<D>, DeliveryError> {
    Ok(DeliveryHeader {
        id,
        sender: copy_wire(sender)?,
        recipient: copy_wire(recipient)?,
        valid_until,
        digest: digest.clone(),
    })
}
