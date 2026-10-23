use crate::{LeaseError, registry_storage::RegistryStorage};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{DatabaseId, Epoch, PeerId, ReplicationPosition, SyncTime};
use serde::{Deserialize, Serialize};
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

/// A validated historical grant. `ended_at` is the final replication position,
/// rather than a wall-clock timestamp.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EpochGrant {
    pub epoch: Epoch,
    pub holder: PeerId,
    pub granted_at: SyncTime,
    pub ended_at: Option<ReplicationPosition>,
}

/// Read the holder granted this exact epoch without consulting the current lease.
pub async fn epoch_holder<S>(
    scope: &mut S,
    database: DatabaseId,
    epoch: Epoch,
) -> Result<Option<EpochGrant>, LeaseError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    Ok(RegistryStorage(scope)
        .history(database, epoch)
        .await?
        .map(|record| EpochGrant {
            epoch: record.epoch,
            holder: record.holder,
            granted_at: record.granted_at,
            ended_at: record.ended_at,
        }))
}
