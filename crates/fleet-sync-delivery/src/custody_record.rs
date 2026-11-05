use crate::DeliveryError;
use fleet_sync_protocol::*;
use serde::{Serialize, de::DeserializeOwned};

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
pub(crate) fn created_at(item: ItemId) -> SyncTime {
    // UUIDv7's first 48 bits are an unsigned Unix millisecond timestamp.
    let milliseconds = item.as_uuid().as_u128() >> 80;
    SyncTime::from_unix_millis(milliseconds as i64)
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
pub(crate) fn next_version(value: StatusVersion) -> Result<StatusVersion, DeliveryError> {
    value
        .next()
        .filter(|next| next.value() <= i64::MAX as u64)
        .ok_or(DeliveryError::SequenceExhausted)
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

pub(crate) fn stored_row_id(
    id: &Option<String>,
    table: &'static str,
    key: String,
) -> Result<(), DeliveryError> {
    if id
        .as_deref()
        .is_none_or(|value| !runtime_primitives::canonical_uuid_v7(value))
    {
        return Err(DeliveryError::CorruptRow {
            table,
            key,
            field: "id",
        });
    }
    Ok(())
}
