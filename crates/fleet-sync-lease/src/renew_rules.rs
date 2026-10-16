use crate::{
    LeaseError, LeasePolicy,
    registry_records::{DatabaseLifecycle, RegistryLease, RenewalAttempt},
    registry_storage::RegistryStorage,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub(crate) fn current_lease(lease: &RegistryLease, now: SyncTime) -> LeaseStatus {
    let remaining = lease
        .expires_at
        .unix_millis()
        .saturating_sub(now.unix_millis());
    LeaseStatus {
        database: lease.database,
        epoch: lease.epoch,
        holder: lease.holder,
        state: if remaining > 0 {
            LeaseState::Held
        } else {
            LeaseState::Expired
        },
        expires_at: lease.expires_at,
        remaining_ms: remaining.max(0).unsigned_abs(),
    }
}

/// Return the saved renewal decision with current authority; replay never extends the lease.
pub async fn renew<S>(
    scope: &mut S,
    request: &RenewRequest,
    holder: &PeerId,
    now: SyncTime,
    policy: &LeasePolicy,
) -> Result<RenewAnswer, LeaseError>
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
    let database = storage.database(request.database).await?;
    if database.as_ref().is_some_and(|database| {
        matches!(
            database.lifecycle,
            DatabaseLifecycle::Retired { .. } | DatabaseLifecycle::Retiring { .. }
        )
    }) {
        return Ok(RenewAnswer::Refused(LeaseRefusal::Retired));
    }
    let lease = storage.lease(request.database).await?;
    let transfer = storage
        .active_transfer(request.database)
        .await?
        .map(|transfer| transfer.intent.attempt());
    if let Some(previous) = storage.renewal(request.database, request.attempt).await? {
        if previous.request != *request || previous.holder != *holder {
            return Err(LeaseError::InvalidInput {
                field: "renewal identity has different contents".into(),
            });
        }
        return match previous.answer {
            RenewAnswer::Renewed { recorded, .. } => {
                let current = lease.ok_or_else(|| LeaseError::Corrupt {
                    field: "successful renewal has no current lease".into(),
                })?;
                Ok(RenewAnswer::Renewed {
                    recorded,
                    current: current_lease(&current, now),
                    transfer_in_progress: transfer,
                    replayed: true,
                })
            }
            refused => Ok(refused),
        };
    }
    let answer = if database.is_none() {
        RenewAnswer::Refused(LeaseRefusal::NotRegistered)
    } else if let Some(mut lease) = lease {
        if lease.holder == *holder && lease.epoch == request.epoch {
            if policy.lease_duration.is_zero() {
                return Err(LeaseError::InvalidInput {
                    field: "lease duration must be positive".into(),
                });
            }
            lease.expires_at = now.after(policy.lease_duration);
            lease.last_renewed_at = now;
            storage.save_lease(&lease).await?;
            RenewAnswer::Renewed {
                recorded: RenewResult {
                    epoch: lease.epoch,
                    expires_at: lease.expires_at,
                },
                current: current_lease(&lease, now),
                transfer_in_progress: transfer,
                replayed: false,
            }
        } else if let Some(history) = storage.history(request.database, request.epoch).await?
            && history.holder == *holder
            && history.ended_at.is_some()
        {
            let ended = history.ended_at.ok_or_else(|| LeaseError::Corrupt {
                field: "epoch end".into(),
            })?;
            RenewAnswer::Refused(LeaseRefusal::StaleEpoch {
                current_epoch: lease.epoch,
                epoch_ended_at: ended,
            })
        } else {
            RenewAnswer::Refused(LeaseRefusal::NotWriter {
                current: Some(lease.epoch),
            })
        }
    } else {
        RenewAnswer::Refused(LeaseRefusal::NotWriter { current: None })
    };
    storage
        .save_renewal(&RenewalAttempt {
            request: request.clone(),
            holder: *holder,
            answer: answer.clone(),
        })
        .await?;
    Ok(answer)
}

/// Read a database's current lease without renewing it. Retirement is checked first.
pub async fn lease_status<S>(
    scope: &mut S,
    database: DatabaseId,
    now: SyncTime,
) -> Result<Option<LeaseStatus>, LeaseError>
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
    if storage.database(database).await?.is_some_and(|record| {
        matches!(
            record.lifecycle,
            DatabaseLifecycle::Retired { .. } | DatabaseLifecycle::Retiring { .. }
        )
    }) {
        return Err(LeaseError::Retired);
    }
    Ok(storage
        .lease(database)
        .await?
        .as_ref()
        .map(|lease| current_lease(lease, now)))
}

/// Host entry points read lifecycle before serving any cached durable-operation answer.
pub async fn database_lifecycle<S>(
    scope: &mut S,
    database: DatabaseId,
) -> Result<Option<DatabaseLifecycle>, LeaseError>
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
        .database(database)
        .await?
        .map(|record| record.lifecycle))
}
