//! Read-only committed-state observations for process verification, excluded from normal builds.
use crate::status_row::PublicationRow;
use crate::{DeliveryError, checked_inspection_queries as queries};
use crate::{
    DeliveryTypes, checked_queries,
    custody_record::{encode, sql_error},
};
use durable_steps::storage::TransactionScope;
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

#[derive(Serialize, Deserialize)]
pub struct DeliveryObservation {
    pub category: String,
    pub identity: String,
    pub metadata: Option<String>,
    pub content: Option<String>,
    pub payload_bytes: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EventObservationKey {
    submitter: crate::Submitter,
    version: crate::StatusVersion,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PublicationObservationKey {
    target_kind: String,
    submitter_kind: String,
    submitter_id: String,
    route: String,
}
/// Call inside the owner's existing transaction; this executes only a SELECT.
pub async fn snapshot<S, D: DeliveryTypes, B>(
    scope: &mut S,
    role: crate::DeliveryHostRole,
    binding: &B,
) -> Result<Vec<DeliveryObservation>, DeliveryError>
where
    S: TransactionScope,
    B: crate::SenderBinding<S, D>,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    let rows = match role {
        crate::DeliveryHostRole::Machine => queries::read_delivery_snapshot(scope).await,
        crate::DeliveryHostRole::Relay => queries::read_relay_delivery_snapshot(scope).await,
    }
    .map_err(sql_error)?;
    let mut observations = Vec::with_capacity(rows.len());
    for row in rows {
        let mut observation = DeliveryObservation {
            category: row.category.ok_or(DeliveryError::Corrupt {
                field: "inspection category",
            })?,
            identity: row.identity.ok_or(DeliveryError::Corrupt {
                field: "inspection identity",
            })?,
            metadata: row.metadata,
            content: row.content,
            payload_bytes: row.payload_bytes,
        };
        observation.content = match observation.category.as_str() {
            "outbox" => {
                let row = checked_queries::read_outbox_item(scope, observation.identity.clone())
                    .await
                    .map_err(sql_error)?
                    .ok_or(DeliveryError::CustodyMissing)?;
                Some(encode(
                    &crate::outbox_storage::decode_header::<S, D, B>(scope, binding, &row).await?,
                )?)
            }
            "inbox" => {
                let row = checked_queries::read_inbox_item(scope, observation.identity.clone())
                    .await
                    .map_err(sql_error)?
                    .ok_or(DeliveryError::CustodyMissing)?;
                Some(encode(
                    &crate::inbox_row::decode_header::<S, D, B>(scope, binding, &row).await?,
                )?)
            }
            "report" => {
                let sequence = observation.metadata.clone().ok_or(DeliveryError::Corrupt {
                    field: "inspection report sequence",
                })?;
                let row =
                    checked_queries::read_report(scope, observation.identity.clone(), sequence)
                        .await
                        .map_err(sql_error)?
                        .ok_or(DeliveryError::CustodyMissing)?;
                Some(encode(&crate::report_row::decode_report::<D>(&row)?)?)
            }
            "publication" => {
                let key: PublicationObservationKey = crate::custody_record::decode(
                    observation.metadata.clone(),
                    "inspection publication target",
                )?;
                let target: crate::PublicationTarget<D::StatusRoute> =
                    crate::status_row::stored_publication_target(
                        Some(key.target_kind),
                        Some(key.submitter_kind),
                        Some(key.submitter_id),
                        Some(key.route),
                    )?;
                let target = crate::status_stream::canonical_encode(&target)?;
                observation.metadata = Some(target.clone());
                let row =
                    checked_queries::read_publication(scope, observation.identity.clone(), target)
                        .await
                        .map_err(sql_error)?
                        .ok_or(DeliveryError::CustodyMissing)?;
                Some(encode(&row.publication::<D::Acceptance>()?)?)
            }
            "statusEvent" => {
                let key: EventObservationKey = crate::custody_record::decode(
                    observation.metadata.clone(),
                    "inspection event key",
                )?;
                let row = checked_queries::read_status_revision(
                    scope,
                    observation.identity.clone(),
                    key.version.value().to_string(),
                    crate::submitter::kind(key.submitter).to_owned(),
                    crate::submitter::id(key.submitter),
                )
                .await
                .map_err(sql_error)?
                .ok_or(DeliveryError::CustodyMissing)?;
                let publication = row.publication::<D::Acceptance>()?;
                let cursor = crate::StatusCursor::new(crate::custody_record::integer(
                    row.cursor,
                    row.cursor_class,
                    "inspection cursor",
                )?);
                observation.metadata = Some(cursor.value().to_string());
                Some(encode(&crate::StatusEvent {
                    cursor,
                    item: publication.item,
                    status: publication.status,
                    version: publication.version,
                    cancel: publication.cancel,
                })?)
            }
            _ => observation.content,
        };
        observations.push(observation);
    }
    Ok(observations)
}
