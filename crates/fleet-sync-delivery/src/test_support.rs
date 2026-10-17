//! Read-only committed-state observations for process verification, excluded from normal builds.
use crate::{DeliveryError, checked_inspection_queries as queries};
use durable_steps::storage::TransactionScope;
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode, types::Type,
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
pub async fn snapshot<S>(scope: &mut S) -> Result<Vec<DeliveryObservation>, DeliveryError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    queries::read_delivery_snapshot(scope)
        .await
        .map_err(crate::custody_record::sql_error)?
        .into_iter()
        .map(|row| {
            Ok(DeliveryObservation {
                category: row.category.ok_or(DeliveryError::Corrupt {
                    field: "inspection category",
                })?,
                identity: row.identity.ok_or(DeliveryError::Corrupt {
                    field: "inspection identity",
                })?,
                metadata: row.metadata,
                content: row.content,
                payload_bytes: row.payload_bytes,
            })
        })
        .collect()
}
