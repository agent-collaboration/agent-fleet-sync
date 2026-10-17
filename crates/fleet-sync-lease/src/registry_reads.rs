use crate::{LeaseError, registry_storage::RegistryStorage};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::DatabaseId;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// List registry-owned database identities except those permanently retired.
pub async fn database_ids<S>(scope: &mut S) -> Result<Vec<DatabaseId>, LeaseError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    RegistryStorage(scope).database_ids().await
}
