use crate::{LeaseError, RegistrySnapshot};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::DatabaseId;
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::future::Future;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeaseChange {
    pub before: Option<RegistrySnapshot>,
    pub after: RegistrySnapshot,
}

/// Project consistent lease snapshots in the lease transaction. An error rolls
/// back that transaction, including the hook's own writes.
pub trait LeaseChangeHook<S: TransactionScope>: Send + Sync + 'static {
    fn on_lease_change(
        &self,
        scope: &mut S,
        change: &LeaseChange,
    ) -> impl Future<Output = Result<(), LeaseError>> + Send;
}

/// Hosts with no lease projection choose this explicitly at composition.
pub struct NoLeaseChangeHook;
impl<S: TransactionScope> LeaseChangeHook<S> for NoLeaseChangeHook {
    async fn on_lease_change(
        &self,
        _scope: &mut S,
        _change: &LeaseChange,
    ) -> Result<(), LeaseError> {
        Ok(())
    }
}

pub(crate) async fn project_lease_change<S, H>(
    scope: &mut S,
    database: DatabaseId,
    before: Option<RegistrySnapshot>,
    hook: &H,
) -> Result<(), LeaseError>
where
    S: TransactionScope,
    H: LeaseChangeHook<S>,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let after = crate::registry_snapshot(scope, database).await?;
    if before == after {
        return Ok(());
    }
    let after = after.ok_or_else(|| LeaseError::Corrupt {
        field: "lease change removed its registry snapshot".into(),
    })?;
    hook.on_lease_change(scope, &LeaseChange { before, after })
        .await
}

#[cfg(test)]
#[path = "lease_change_hook_test.rs"]
mod tests;
