use crate::{
    LeaseError, RegistryConfig,
    change_checks::{attempt, database, target},
    operation_support::{step, step_error},
    registry_records::*,
    registry_storage::RegistryStorage,
};
use durable_steps::{AtomicStore, OperationContext, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub(crate) async fn outcome<C, D>(
    ctx: &mut C,
    request: &HolderChangeRequest,
    name: &str,
    phase: Option<crate::HolderChangePhase>,
) -> Result<Option<HolderChangeAnswer>, LeaseError>
where
    C: OperationContext,
    D: Database,
    for<'t> <C::Store as AtomicStore>::Scope<'t>: TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    let id = attempt(request);
    let database = database(request);
    let now = ctx.now();
    Ok(ctx
        .local(step(name)?, move |scope| {
            Box::pin(async move {
                if let Some(answer) =
                    crate::transfer_cancellation::process_cancel::<C::Store, D>(scope, id, now)
                        .await
                        .map_err(step_error)?
                {
                    return Ok(Some(answer));
                }
                let mut storage = RegistryStorage(scope);
                let mut record =
                    storage
                        .transfer(id)
                        .await
                        .map_err(step_error)?
                        .ok_or_else(|| {
                            step_error(LeaseError::Corrupt {
                                field: "transfer record".into(),
                            })
                        })?;
                if storage
                    .database(database)
                    .await
                    .map_err(step_error)?
                    .is_some_and(|record| {
                        matches!(record.lifecycle, DatabaseLifecycle::Retired { .. })
                    })
                {
                    let answer = HolderChangeAnswer::Refused(HolderChangeRefusal::Retired);
                    if record.outcome.is_none() {
                        record.outcome = Some(answer.clone());
                        record
                            .enter(crate::HolderChangePhase::Ended, now)
                            .map_err(step_error)?;
                        storage.save_transfer(&record).await.map_err(step_error)?;
                    }
                    return Ok(Some(answer));
                }
                if record.outcome.is_none()
                    && let Some(phase) = phase
                {
                    record.enter(phase, now).map_err(step_error)?;
                    storage.save_transfer(&record).await.map_err(step_error)?;
                }
                Ok(record.outcome)
            })
        })
        .await?)
}

pub(crate) async fn refuse<C, D>(
    ctx: &mut C,
    request: &HolderChangeRequest,
    name: &str,
    reason: HolderChangeRefusal,
) -> Result<HolderChangeAnswer, LeaseError>
where
    C: OperationContext,
    D: Database,
    for<'t> <C::Store as AtomicStore>::Scope<'t>: TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    let id = attempt(request);
    let now = ctx.now();
    Ok(ctx
        .local(step(name)?, move |scope| {
            Box::pin(async move {
                if let Some(answer) =
                    crate::transfer_cancellation::process_cancel::<C::Store, D>(scope, id, now)
                        .await
                        .map_err(step_error)?
                {
                    return Ok(answer);
                }
                let mut storage = RegistryStorage(scope);
                let mut record =
                    storage
                        .transfer(id)
                        .await
                        .map_err(step_error)?
                        .ok_or_else(|| {
                            step_error(LeaseError::Corrupt {
                                field: "transfer record".into(),
                            })
                        })?;
                if let Some(answer) = record.outcome {
                    return Ok(answer);
                }
                let answer = if storage
                    .database(record.intent.database())
                    .await
                    .map_err(step_error)?
                    .is_some_and(|record| {
                        matches!(record.lifecycle, DatabaseLifecycle::Retired { .. })
                    }) {
                    HolderChangeAnswer::Refused(HolderChangeRefusal::Retired)
                } else {
                    HolderChangeAnswer::Refused(reason)
                };
                record.outcome = Some(answer.clone());
                record
                    .enter(crate::HolderChangePhase::Ended, now)
                    .map_err(step_error)?;
                storage.save_transfer(&record).await.map_err(step_error)?;
                Ok(answer)
            })
        })
        .await?)
}

/// Grant and its immutable transfer outcome share one transaction with both history entries.
pub(crate) async fn grant<C, D>(
    ctx: &mut C,
    request: &HolderChangeRequest,
    fenced: FenceResult,
    expect: FenceExpectation,
    config: &RegistryConfig,
) -> Result<HolderChangeAnswer, LeaseError>
where
    C: OperationContext,
    D: Database,
    for<'t> <C::Store as AtomicStore>::Scope<'t>: TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    let request = request.clone();
    let duration = config.lease.lease_duration;
    let now = ctx.now();
    Ok(ctx
        .local(step("grant")?, move |scope| {
            Box::pin(async move {
                if let Some(answer) = crate::transfer_cancellation::process_cancel::<C::Store, D>(
                    scope,
                    attempt(&request),
                    now,
                )
                .await
                .map_err(step_error)?
                {
                    return Ok(answer);
                }
                let mut storage = RegistryStorage(scope);
                let database = database(&request);
                let mut transfer = storage
                    .transfer(attempt(&request))
                    .await
                    .map_err(step_error)?
                    .ok_or_else(|| {
                        step_error(LeaseError::Corrupt {
                            field: "grant transfer".into(),
                        })
                    })?;
                if let Some(answer) = transfer.outcome {
                    return Ok(answer);
                }
                let record = storage
                    .database(database)
                    .await
                    .map_err(step_error)?
                    .ok_or_else(|| step_error(LeaseError::NotRegistered))?;
                let existing = storage.lease(database).await.map_err(step_error)?;
                let reserved = transfer.intent.new_epoch.ok_or_else(|| {
                    step_error(LeaseError::Corrupt {
                        field: "reserved grant epoch".into(),
                    })
                })?;
                let FenceResult::Fenced {
                    old_epoch,
                    new_epoch,
                    start,
                } = fenced
                else {
                    return Err(step_error(LeaseError::InvalidInput {
                        field: "grant requires successful fence".into(),
                    }));
                };
                if reserved != new_epoch || old_epoch != expect.epoch || start != expect.acked {
                    return Err(step_error(LeaseError::Corrupt {
                        field: "grant epoch differs from reserved epoch".into(),
                    }));
                }
                let refusal = match record.lifecycle {
                    DatabaseLifecycle::Retired { .. } | DatabaseLifecycle::Retiring { .. } => {
                        Some(HolderChangeRefusal::Retired)
                    }
                    _ if storage.floor(database).await.map_err(step_error)? != Some(reserved) => {
                        Some(HolderChangeRefusal::PreviewStale)
                    }
                    _ => match &request {
                        HolderChangeRequest::Claim { .. }
                            if existing.is_some()
                                || !matches!(
                                    record.lifecycle,
                                    DatabaseLifecycle::Unheld { .. }
                                ) =>
                        {
                            let holder = existing
                                .as_ref()
                                .ok_or_else(|| {
                                    step_error(LeaseError::Corrupt {
                                        field: "registered database has no holder".into(),
                                    })
                                })?
                                .holder;
                            Some(HolderChangeRefusal::AlreadyHeld { holder })
                        }
                        HolderChangeRequest::Move { from_epoch, .. }
                            if existing
                                .as_ref()
                                .is_none_or(|lease| lease.epoch != *from_epoch) =>
                        {
                            Some(HolderChangeRefusal::PreviewStale)
                        }
                        HolderChangeRequest::Takeover { preview, .. }
                            if existing
                                .as_ref()
                                .is_none_or(|lease| lease.holder != preview.holder)
                                || start != preview.acked =>
                        {
                            Some(HolderChangeRefusal::PreviewStale)
                        }
                        _ => None,
                    },
                };
                let answer = if let Some(refusal) = refusal {
                    HolderChangeAnswer::Refused(refusal)
                } else {
                    if duration.is_zero() {
                        return Err(step_error(LeaseError::InvalidInput {
                            field: "lease duration must be positive".into(),
                        }));
                    }
                    if let Some(previous) = existing {
                        let mut history = storage
                            .history(database, previous.epoch)
                            .await
                            .map_err(step_error)?
                            .ok_or_else(|| {
                                step_error(LeaseError::Corrupt {
                                    field: "previous grant history".into(),
                                })
                            })?;
                        history.ended_at = Some(start);
                        storage.save_history(&history).await.map_err(step_error)?;
                    }
                    let lease = RegistryLease {
                        database,
                        holder: target(&request),
                        epoch: reserved,
                        last_renewed_at: now,
                        expires_at: now.after(duration),
                    };
                    storage.save_lease(&lease).await.map_err(step_error)?;
                    storage
                        .save_history(&EpochHistory {
                            database,
                            epoch: reserved,
                            holder: lease.holder,
                            granted_at: now,
                            ended_at: None,
                        })
                        .await
                        .map_err(step_error)?;
                    if matches!(record.lifecycle, DatabaseLifecycle::Unheld { .. }) {
                        storage
                            .save_database(&DatabaseRecord {
                                database,
                                lifecycle: DatabaseLifecycle::Registered {
                                    origin: Origin::Seeded { position: start },
                                },
                            })
                            .await
                            .map_err(step_error)?;
                    }
                    HolderChangeAnswer::Granted {
                        epoch: reserved,
                        expires_at: lease.expires_at,
                        start,
                    }
                };
                transfer.outcome = Some(answer.clone());
                transfer
                    .enter(crate::HolderChangePhase::Ended, now)
                    .map_err(step_error)?;
                storage.save_transfer(&transfer).await.map_err(step_error)?;
                Ok(answer)
            })
        })
        .await?)
}

/// Read the domain's final answer, including a committed grant whose engine later failed.
pub async fn holder_change_answer<S>(
    scope: &mut S,
    request: &HolderChangeRequest,
) -> Result<Option<HolderChangeAnswer>, LeaseError>
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
    if storage
        .database(database(request))
        .await?
        .is_some_and(|record| matches!(record.lifecycle, DatabaseLifecycle::Retired { .. }))
    {
        return Ok(Some(HolderChangeAnswer::Refused(
            HolderChangeRefusal::Retired,
        )));
    }
    let Some(record) = storage.transfer(attempt(request)).await? else {
        return Ok(None);
    };
    if record.intent.request != *request {
        return Err(LeaseError::InvalidInput {
            field: "holder-change identity has different contents".into(),
        });
    }
    Ok(record.outcome.or(Some(HolderChangeAnswer::Pending {
        transfer: attempt(request),
    })))
}
