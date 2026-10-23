use crate::{DatabaseLifecycle, LeaseError, registry_storage::RegistryStorage};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{DatabaseId, Epoch, PeerId, SyncTime};
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::num::NonZeroU64;

/// A positive state change counter within an actual registry epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RegistryRevision {
    pub epoch: Epoch,
    pub change: NonZeroU64,
}

/// Stored current grant facts; expiry is not evaluated against a reader's clock.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RegistryGrant {
    pub epoch: Epoch,
    pub holder: PeerId,
    pub expires_at: SyncTime,
}

/// One committed state revision. Publication and incarnation belong to the host.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RegistrySnapshot {
    pub database: DatabaseId,
    pub revision: RegistryRevision,
    pub changed_at: SyncTime,
    pub lifecycle: DatabaseLifecycle,
    pub grant: Option<RegistryGrant>,
}

/// Read lifecycle, stored grant and revision together in the caller's transaction.
pub async fn registry_snapshot<S>(
    scope: &mut S,
    database: DatabaseId,
) -> Result<Option<RegistrySnapshot>, LeaseError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let mut storage = RegistryStorage(scope);
    let Some(record) = storage.database(database).await? else {
        return Ok(None);
    };
    let lease = storage.lease(database).await?;
    record.validate_grant(lease.as_ref())?;
    Ok(Some(RegistrySnapshot {
        database,
        revision: record.revision,
        changed_at: record.changed_at,
        lifecycle: record.lifecycle,
        grant: lease.map(|lease| RegistryGrant {
            epoch: lease.epoch,
            holder: lease.holder,
            expires_at: lease.expires_at,
        }),
    }))
}

/// Enumerate every stored database for publication repair, including Retired.
pub async fn registry_snapshot_ids<S>(scope: &mut S) -> Result<Vec<DatabaseId>, LeaseError>
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
        .database_records()
        .await?
        .into_iter()
        .map(|record| record.database)
        .collect())
}
