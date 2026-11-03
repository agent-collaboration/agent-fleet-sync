use crate::DeliveryError;
use fleet_sync_protocol::{DatabaseId, PeerId, Submitter};

pub(crate) enum SourceRetention {
    Item,
    Negative,
    Legacy,
}

pub(crate) struct CheckedSender<D: fleet_sync_protocol::DeliveryTypes> {
    pub submitter: Option<Submitter>,
    pub consumer: Option<D::Sender>,
}

pub(crate) fn checked_sender<D: fleet_sync_protocol::DeliveryTypes>(
    kind: Option<String>,
    id: Option<String>,
    consumer: Option<&str>,
    retention: SourceRetention,
    table: &'static str,
    key: &str,
) -> Result<CheckedSender<D>, DeliveryError> {
    let corrupt = |field| DeliveryError::CorruptRow {
        table,
        key: key.to_owned(),
        field,
    };
    let submitter = from_columns(kind, id).map_err(|_| corrupt("submitter"))?;
    if matches!(retention, SourceRetention::Legacy) {
        if submitter.is_some() || consumer.is_some() {
            return Err(corrupt("legacy sender"));
        }
        return Ok(CheckedSender {
            submitter: None,
            consumer: None,
        });
    }
    let Some(raw) = consumer else {
        if matches!(retention, SourceRetention::Negative) {
            return Ok(CheckedSender {
                submitter,
                consumer: None,
            });
        }
        return Err(corrupt("consumer_sender"));
    };
    let consumer: D::Sender = serde_json::from_str(raw).map_err(|_| corrupt("consumer_sender"))?;
    if Some(D::submitter(&consumer)) != submitter {
        return Err(corrupt("submitter"));
    }
    Ok(CheckedSender {
        submitter,
        consumer: Some(consumer),
    })
}

pub(crate) const fn kind(owner: Submitter) -> &'static str {
    match owner {
        Submitter::Machine(_) => "machine",
        Submitter::Project(_) => "project",
    }
}

pub(crate) fn id(owner: Submitter) -> String {
    owner.as_uuid().to_string()
}

pub(crate) fn from_columns(
    kind: Option<String>,
    id: Option<String>,
) -> Result<Option<Submitter>, DeliveryError> {
    let corrupt = || DeliveryError::Corrupt {
        field: "submitter identity",
    };
    match (kind.as_deref(), id) {
        (None, None) => Ok(None),
        (Some("machine"), Some(id)) => PeerId::try_from(id)
            .map(Submitter::Machine)
            .map(Some)
            .map_err(|_| corrupt()),
        (Some("project"), Some(id)) => DatabaseId::try_from(id)
            .map(Submitter::Project)
            .map(Some)
            .map_err(|_| corrupt()),
        _ => Err(corrupt()),
    }
}

#[cfg(test)]
#[path = "submitter_test.rs"]
mod tests;
