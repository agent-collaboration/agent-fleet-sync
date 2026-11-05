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

#[expect(
    clippy::manual_async_fn,
    reason = "explicit Send preserves higher-ranked borrowed transaction futures"
)]
pub(crate) fn checked_sender<'a, S, D, B>(
    scope: &'a mut S,
    binding: &'a B,
    kind: Option<String>,
    id: Option<String>,
    consumer: Option<&'a str>,
    retention: SourceRetention,
    row: crate::CustodyRow,
) -> impl std::future::Future<Output = Result<CheckedSender<D>, DeliveryError>> + Send + 'a
where
    S: durable_steps::storage::TransactionScope,
    D: fleet_sync_protocol::DeliveryTypes,
    B: crate::SenderBinding<S, D>,
{
    async move {
        let corrupt = |field| DeliveryError::CorruptRow {
            table: row.table(),
            key: row.item().as_uuid().to_string(),
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
        if submitter.is_none() {
            return Err(corrupt("submitter"));
        }
        let consumer: D::Sender =
            serde_json::from_str(raw).map_err(|_| corrupt("consumer_sender"))?;
        let verified =
            crate::sender_binding::verify_sender::<S, D, B>(scope, binding, &consumer, row).await?;
        if Some(verified.owner()) != submitter {
            return Err(corrupt("submitter"));
        }
        Ok(CheckedSender {
            submitter,
            consumer: Some(consumer),
        })
    }
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
