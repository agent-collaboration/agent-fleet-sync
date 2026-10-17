use crate::{
    LeaseError, RegistryConfig,
    operation_support::{step, step_error},
    registry_records::*,
    registry_storage::RegistryStorage,
};
use durable_steps::{
    AtomicStore, OperationContext, OperationFailure, RequestKey, StepError,
    storage::{JournalStorage, TransactionScope},
};
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub(crate) fn database(request: &HolderChangeRequest) -> DatabaseId {
    match *request {
        HolderChangeRequest::Move { database, .. }
        | HolderChangeRequest::Takeover { database, .. }
        | HolderChangeRequest::Claim { database, .. } => database,
    }
}
pub(crate) fn attempt(request: &HolderChangeRequest) -> TransferId {
    match *request {
        HolderChangeRequest::Move { attempt, .. }
        | HolderChangeRequest::Takeover { attempt, .. }
        | HolderChangeRequest::Claim { attempt, .. } => attempt,
    }
}
pub(crate) fn target(request: &HolderChangeRequest) -> PeerId {
    match *request {
        HolderChangeRequest::Move { to, .. } | HolderChangeRequest::Takeover { to, .. } => to,
        HolderChangeRequest::Claim { claimant, .. } => claimant,
    }
}
pub fn holder_change_request_key(id: TransferId) -> Result<RequestKey, LeaseError> {
    RequestKey::new(id.as_uuid().to_string()).map_err(|error| LeaseError::InvalidInput {
        field: error.to_string(),
    })
}

#[derive(Serialize, Deserialize)]
pub(crate) enum CheckedChange {
    Continue {
        intent: TransferIntent,
        holder: Option<PeerId>,
    },
    Answer(HolderChangeAnswer),
}

pub(crate) async fn check<C, D>(
    ctx: &mut C,
    request: &HolderChangeRequest,
    config: &RegistryConfig,
) -> Result<CheckedChange, LeaseError>
where
    C: OperationContext,
    D: Database,
    for<'t> <C::Store as AtomicStore>::Scope<'t>: TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    let operation = *ctx.id();
    let offered = request.clone();
    let now = ctx.now();
    let renewal_bound = config.move_needs_renewal_within;
    let checked = ctx
        .local(step("check")?, move |scope| {
            Box::pin(async move {
                let journal = scope
                    .operation(operation)
                    .await?
                    .ok_or(durable_steps::StoreError::MissingOperation { id: operation })?;
                if journal.request_key
                    != holder_change_request_key(attempt(&offered)).map_err(step_error)?
                {
                    return Err(StepError::Failed(OperationFailure::InvalidPayload {
                        message: "holder-change request key must be its attempt ID".into(),
                    }));
                }
                let id = attempt(&offered);
                let admission = RegistryStorage(scope)
                    .admission(id)
                    .await
                    .map_err(step_error)?
                    .ok_or_else(|| {
                        step_error(LeaseError::Corrupt {
                            field: "holder change must use registry admission".into(),
                        })
                    })?;
                if admission.operation != operation || admission.request != offered {
                    return Err(step_error(LeaseError::Corrupt {
                        field: "holder-change admission identity".into(),
                    }));
                }
                if let Some(answer) =
                    crate::transfer_cancellation::process_cancel::<C::Store, D>(scope, id, now)
                        .await
                        .map_err(step_error)?
                {
                    return Ok(CheckedChange::Answer(answer));
                }
                let mut storage = RegistryStorage(scope);
                let database = database(&offered);
                let id = attempt(&offered);
                let record = storage.database(database).await.map_err(step_error)?;
                if record.as_ref().is_some_and(|record| {
                    matches!(record.lifecycle, DatabaseLifecycle::Retired { .. })
                }) {
                    if storage.transfer(id).await.map_err(step_error)?.is_none() {
                        return refuse(&mut storage, offered, HolderChangeRefusal::Retired, now)
                            .await;
                    }
                    return Ok(CheckedChange::Answer(HolderChangeAnswer::Refused(
                        HolderChangeRefusal::Retired,
                    )));
                }
                if let Some(existing) = storage.transfer(id).await.map_err(step_error)? {
                    if existing.intent.request != offered {
                        return Err(step_error(LeaseError::InvalidInput {
                            field: "holder-change identity has different contents".into(),
                        }));
                    }
                    if let Some(outcome) = existing.outcome {
                        return Ok(CheckedChange::Answer(outcome));
                    }
                    let holder = storage
                        .lease(database)
                        .await
                        .map_err(step_error)?
                        .map(|lease| lease.holder);
                    return Ok(CheckedChange::Continue {
                        intent: existing.intent,
                        holder,
                    });
                }
                let Some(record) = record else {
                    return refuse(&mut storage, offered, HolderChangeRefusal::NotFound, now).await;
                };
                if let DatabaseLifecycle::Retiring { attempt } = record.lifecycle {
                    let transfer = TransferId::new(attempt.as_uuid()).map_err(|error| {
                        step_error(LeaseError::InvalidInput {
                            field: error.to_string(),
                        })
                    })?;
                    return refuse(
                        &mut storage,
                        offered,
                        HolderChangeRefusal::TransferInProgress { transfer },
                        now,
                    )
                    .await;
                }
                if let Some(lifecycle) = storage
                    .active_lifecycle(database)
                    .await
                    .map_err(step_error)?
                {
                    let transfer =
                        TransferId::new(lifecycle.intent.attempt().as_uuid()).map_err(|error| {
                            step_error(LeaseError::InvalidInput {
                                field: error.to_string(),
                            })
                        })?;
                    return refuse(
                        &mut storage,
                        offered,
                        HolderChangeRefusal::TransferInProgress { transfer },
                        now,
                    )
                    .await;
                }
                let lease = storage.lease(database).await.map_err(step_error)?;
                match &offered {
                    HolderChangeRequest::Move { from_epoch, to, .. } => {
                        let Some(current) = lease.as_ref() else {
                            return refuse(
                                &mut storage,
                                offered,
                                HolderChangeRefusal::NotFound,
                                now,
                            )
                            .await;
                        };
                        if *to == current.holder {
                            return refuse(
                                &mut storage,
                                offered,
                                HolderChangeRefusal::AlreadyHeld,
                                now,
                            )
                            .await;
                        }
                        if *from_epoch != current.epoch {
                            return refuse(
                                &mut storage,
                                offered,
                                HolderChangeRefusal::PreviewStale,
                                now,
                            )
                            .await;
                        }
                        if renewal_bound
                            .is_some_and(|bound| now > current.last_renewed_at.after(bound))
                        {
                            let last_renewed_at = current.last_renewed_at;
                            return refuse(
                                &mut storage,
                                offered,
                                HolderChangeRefusal::HolderStale { last_renewed_at },
                                now,
                            )
                            .await;
                        }
                    }
                    HolderChangeRequest::Takeover { preview, to, .. } => {
                        let Some(current) = lease.as_ref() else {
                            return refuse(
                                &mut storage,
                                offered,
                                HolderChangeRefusal::NotFound,
                                now,
                            )
                            .await;
                        };
                        if *to == current.holder {
                            return refuse(
                                &mut storage,
                                offered,
                                HolderChangeRefusal::AlreadyHeld,
                                now,
                            )
                            .await;
                        }
                        let saved = storage.preview(&preview.token).await.map_err(step_error)?;
                        if current.holder != preview.holder
                            || saved.is_none_or(|saved| {
                                saved.database != database || saved.preview != *preview
                            })
                        {
                            return refuse(
                                &mut storage,
                                offered,
                                HolderChangeRefusal::PreviewStale,
                                now,
                            )
                            .await;
                        }
                    }
                    HolderChangeRequest::Claim { .. } => {
                        if lease.is_some()
                            || !matches!(record.lifecycle, DatabaseLifecycle::Unheld { .. })
                        {
                            return refuse(
                                &mut storage,
                                offered,
                                HolderChangeRefusal::AlreadyHeld,
                                now,
                            )
                            .await;
                        }
                    }
                }
                let Some(floor) = storage.floor(database).await.map_err(step_error)? else {
                    return refuse(
                        &mut storage,
                        offered,
                        HolderChangeRefusal::EpochUnprovable,
                        now,
                    )
                    .await;
                };
                let floor = match &offered {
                    HolderChangeRequest::Takeover { preview, .. } => floor.max(preview.fence),
                    _ => floor,
                };
                if lease.as_ref().is_some_and(|lease| lease.epoch > floor) {
                    return refuse(
                        &mut storage,
                        offered,
                        HolderChangeRefusal::EpochUnprovable,
                        now,
                    )
                    .await;
                }
                let Some(next) = floor
                    .value()
                    .checked_add(1)
                    .and_then(|value| Epoch::new(value).ok())
                else {
                    return refuse(
                        &mut storage,
                        offered,
                        HolderChangeRefusal::EpochUnprovable,
                        now,
                    )
                    .await;
                };
                if let Some(mut active) = storage
                    .active_transfer(database)
                    .await
                    .map_err(step_error)?
                {
                    if matches!(offered, HolderChangeRequest::Takeover { .. })
                        && matches!(active.intent.request, HolderChangeRequest::Move { .. })
                    {
                        active.outcome = Some(HolderChangeAnswer::Refused(
                            HolderChangeRefusal::Superseded { by: id },
                        ));
                        active
                            .enter(crate::HolderChangePhase::Ended, now)
                            .map_err(step_error)?;
                        storage.save_transfer(&active).await.map_err(step_error)?;
                    } else {
                        return refuse(
                            &mut storage,
                            offered,
                            HolderChangeRefusal::TransferInProgress {
                                transfer: active.intent.attempt(),
                            },
                            now,
                        )
                        .await;
                    }
                }
                storage
                    .save_floor(database, next)
                    .await
                    .map_err(step_error)?;
                let intent = TransferIntent {
                    request: offered,
                    new_epoch: Some(next),
                };
                let mut transfer = TransferRecord::requested(intent.clone(), admission.admitted_at);
                if matches!(intent.request, HolderChangeRequest::Move { .. }) {
                    transfer
                        .enter(crate::HolderChangePhase::Releasing, now)
                        .map_err(step_error)?;
                }
                storage.save_transfer(&transfer).await.map_err(step_error)?;
                Ok(CheckedChange::Continue {
                    intent,
                    holder: lease.map(|lease| lease.holder),
                })
            })
        })
        .await?;
    Ok(checked)
}

async fn refuse<S>(
    storage: &mut RegistryStorage<'_, S>,
    request: HolderChangeRequest,
    reason: HolderChangeRefusal,
    now: SyncTime,
) -> Result<CheckedChange, StepError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let answer = HolderChangeAnswer::Refused(reason);
    let admission = storage
        .admission(attempt(&request))
        .await
        .map_err(step_error)?
        .ok_or_else(|| {
            step_error(LeaseError::Corrupt {
                field: "refusal admission missing".into(),
            })
        })?;
    let mut record = TransferRecord::requested(
        TransferIntent {
            request,
            new_epoch: None,
        },
        admission.admitted_at,
    );
    record.outcome = Some(answer.clone());
    record
        .enter(crate::HolderChangePhase::Ended, now)
        .map_err(step_error)?;
    storage.save_transfer(&record).await.map_err(step_error)?;
    Ok(CheckedChange::Answer(answer))
}
