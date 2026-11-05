use crate::{DeliveryError, SyncTime, checked_queries as queries, custody_record::*};
use durable_steps::storage::TransactionScope;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

#[expect(
    clippy::manual_async_fn,
    reason = "explicit Send preserves borrowed transaction futures"
)]
pub(crate) fn completed<'scope, S>(
    scope: &'scope mut S,
    name: &'static str,
) -> impl std::future::Future<Output = Result<bool, DeliveryError>> + Send + 'scope
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    async move {
        let Some(row) = queries::read_delivery_backfill(scope, name.to_owned())
            .await
            .map_err(sql_error)?
        else {
            return Ok(false);
        };
        stored_row_id(&row.id, "_fleet_sync_delivery_backfills", name.to_owned())?;
        if row.name.as_deref() != Some(name) {
            return Err(DeliveryError::Corrupt {
                field: "delivery backfill name",
            });
        }
        Ok(true)
    }
}

pub(crate) async fn complete<S>(
    scope: &mut S,
    name: &'static str,
    now: SyncTime,
) -> Result<(), DeliveryError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
{
    queries::complete_delivery_backfill(scope, name.to_owned(), now.unix_millis().to_string())
        .await
        .map_err(sql_error)
}
