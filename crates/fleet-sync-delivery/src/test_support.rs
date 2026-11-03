//! Read-only committed-state observations for process verification, excluded from normal builds.
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
/// Call inside the owner's existing transaction; this executes only a SELECT.
pub async fn snapshot<S, D: DeliveryTypes>(
    scope: &mut S,
) -> Result<Vec<DeliveryObservation>, DeliveryError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    let rows = queries::read_delivery_snapshot(scope)
        .await
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
                Some(encode(&crate::outbox_storage::decode_header::<
                    D::Acceptance,
                >(&row)?)?)
            }
            "inbox" => {
                let row = checked_queries::read_inbox_item(scope, observation.identity.clone())
                    .await
                    .map_err(sql_error)?
                    .ok_or(DeliveryError::CustodyMissing)?;
                Some(encode(&crate::inbox_row::decode_header::<D>(&row)?)?)
            }
            _ => observation.content,
        };
        observations.push(observation);
    }
    Ok(observations)
}
