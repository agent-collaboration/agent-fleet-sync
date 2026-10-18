use crate::{
    CopyCreate, LeaseError, RegistrationAdmission, RegistryConfig,
    operation_support::{Decision, open_lifecycle, saved_failure, step, step_error},
    registry_records::*,
    registry_storage::RegistryStorage,
    renew_rules::current_lease,
};
use durable_steps::{AtomicStore, OperationContext, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// The copy is created before the registry grants its first lease.
pub async fn register<C, D>(
    ctx: &mut C,
    request: &Registration,
    config: &RegistryConfig,
) -> Result<RegistrationAnswer, LeaseError>
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
    let decision = ctx
        .local(step("check")?, move |scope| {
            Box::pin(async move {
                let database = RegistryStorage(scope)
                    .database(offered.database)
                    .await
                    .map_err(step_error)?;
                if database.as_ref().is_some_and(|record| {
                    matches!(
                        record.lifecycle,
                        DatabaseLifecycle::Retired { .. } | DatabaseLifecycle::Retiring { .. }
                    )
                }) {
                    return Ok(Decision::Answer(RegistrationAnswer::Refused(
                        RegistrationRefusal::Retired,
                    )));
                }
                let mut attempt =
                    open_lifecycle(scope, operation, LifecycleIntent::Register(offered.clone()))
                        .await?;
                if let Some(outcome) = attempt.outcome {
                    if let Some(error) = saved_failure(&outcome) {
                        return Err(step_error(error));
                    }
                    return match outcome {
                        LifecycleOutcome::Registration(answer) => Ok(Decision::Answer(answer)),
                        _ => Err(step_error(LeaseError::Corrupt {
                            field: "registration outcome kind".into(),
                        })),
                    };
                }
                let answer = match database {
                    None => return Ok(Decision::Continue),
                    Some(record) => match record.lifecycle {
                        DatabaseLifecycle::Registered { .. } => {
                            let lease = RegistryStorage(scope)
                                .lease(offered.database)
                                .await
                                .map_err(step_error)?;
                            match lease {
                                Some(lease) if lease.holder == offered.writer => {
                                    RegistrationAnswer::Registered {
                                        lease: current_lease(&lease, now),
                                        replayed: true,
                                    }
                                }
                                lease => {
                                    RegistrationAnswer::Refused(RegistrationRefusal::NotWriter {
                                        current: lease.map(|lease| lease.epoch),
                                    })
                                }
                            }
                        }
                        DatabaseLifecycle::Unheld { .. } => {
                            RegistrationAnswer::Refused(RegistrationRefusal::NotWriter {
                                current: None,
                            })
                        }
                        DatabaseLifecycle::Retiring { .. } | DatabaseLifecycle::Retired { .. } => {
                            RegistrationAnswer::Refused(RegistrationRefusal::Retired)
                        }
                    },
                };
                attempt.outcome = Some(LifecycleOutcome::Registration(answer.clone()));
                RegistryStorage(scope)
                    .save_lifecycle(&attempt)
                    .await
                    .map_err(step_error)?;
                Ok(Decision::Answer(answer))
            })
        })
        .await?;
    if let Decision::Answer(answer) = decision {
        return Ok(answer);
    }
    let admission = ctx
        .call(
            step("admit")?,
            RegistrationAdmission,
            fleet_sync_protocol::RegistrationAdmission {
                database: request.database,
                writer: request.writer,
            },
        )
        .await?;
    let refusal = match admission {
        GateAnswer::Admitted => None,
        GateAnswer::Refused { reason } => Some(RegistrationRefusal::DatabaseRejected { reason }),
        GateAnswer::Reserved => {
            return Err(LeaseError::InvalidInput {
                field: "registration gate returned Reserved".into(),
            });
        }
    };
    if let Some(refusal) = refusal {
        return finish_refusal::<C, D>(ctx, request, "refuse_admission", refusal).await;
    }
    let origin = match request.origin {
        Origin::Empty => CopyOrigin::Empty,
        Origin::Seeded { position } => CopyOrigin::AwaitingSeed {
            expected_position: Some(position),
        },
    };
    let created = ctx
        .call(
            step("create_copy")?,
            CopyCreate {
                copy: config.authoritative_receiver,
            },
            CreateCopy {
                database: request.database,
                epoch: Epoch::new(1).map_err(|error| LeaseError::InvalidInput {
                    field: error.to_string(),
                })?,
                holder: Some(request.writer),
                origin,
            },
        )
        .await?;
    match created {
        CreateCopyAnswer::Retired => {
            return finish_refusal::<C, D>(
                ctx,
                request,
                "refuse_retired_copy",
                RegistrationRefusal::Retired,
            )
            .await;
        }
        CreateCopyAnswer::CreationConflict { existing } => {
            return finish_refusal::<C, D>(
                ctx,
                request,
                "refuse_copy_conflict",
                RegistrationRefusal::NotWriter {
                    current: Some(existing.epoch),
                },
            )
            .await;
        }
        CreateCopyAnswer::CopyCreated | CreateCopyAnswer::AlreadyCreated => {}
    }
    let request = request.clone();
    let policy = config.lease;
    let now = ctx.now();
    Ok(ctx
        .local(step("register")?, move |scope| {
            Box::pin(async move {
                let mut storage = RegistryStorage(scope);
                let mut attempt = storage
                    .lifecycle(request.attempt)
                    .await
                    .map_err(step_error)?
                    .ok_or_else(|| {
                        step_error(LeaseError::Corrupt {
                            field: "registration attempt".into(),
                        })
                    })?;
                let answer = match storage
                    .database(request.database)
                    .await
                    .map_err(step_error)?
                {
                    Some(record)
                        if matches!(
                            record.lifecycle,
                            DatabaseLifecycle::Retired { .. } | DatabaseLifecycle::Retiring { .. }
                        ) =>
                    {
                        RegistrationAnswer::Refused(RegistrationRefusal::Retired)
                    }
                    Some(_) => {
                        let lease = storage.lease(request.database).await.map_err(step_error)?;
                        match lease {
                            Some(lease) if lease.holder == request.writer => {
                                RegistrationAnswer::Registered {
                                    lease: current_lease(&lease, now),
                                    replayed: true,
                                }
                            }
                            lease => RegistrationAnswer::Refused(RegistrationRefusal::NotWriter {
                                current: lease.map(|lease| lease.epoch),
                            }),
                        }
                    }
                    None => {
                        if storage
                            .floor(request.database)
                            .await
                            .map_err(step_error)?
                            .is_some()
                        {
                            return Err(step_error(LeaseError::Corrupt {
                                field: "unknown database already has an epoch floor".into(),
                            }));
                        }
                        if policy.lease_duration.is_zero() {
                            return Err(step_error(LeaseError::InvalidInput {
                                field: "lease duration must be positive".into(),
                            }));
                        }
                        let epoch = Epoch::new(1).map_err(|error| {
                            step_error(LeaseError::InvalidInput {
                                field: error.to_string(),
                            })
                        })?;
                        let lease = RegistryLease {
                            database: request.database,
                            epoch,
                            holder: request.writer,
                            last_renewed_at: now,
                            expires_at: now.after(policy.lease_duration),
                        };
                        storage
                            .save_database(&DatabaseRecord {
                                database: request.database,
                                lifecycle: DatabaseLifecycle::Registered {
                                    origin: request.origin.clone(),
                                    imported_from: None,
                                },
                            })
                            .await
                            .map_err(step_error)?;
                        storage
                            .save_floor(request.database, epoch)
                            .await
                            .map_err(step_error)?;
                        storage.save_lease(&lease).await.map_err(step_error)?;
                        storage
                            .save_history(&EpochHistory {
                                database: request.database,
                                epoch,
                                holder: request.writer,
                                granted_at: now,
                                ended_at: None,
                            })
                            .await
                            .map_err(step_error)?;
                        RegistrationAnswer::Registered {
                            lease: current_lease(&lease, now),
                            replayed: false,
                        }
                    }
                };
                attempt.outcome = Some(LifecycleOutcome::Registration(answer.clone()));
                storage.save_lifecycle(&attempt).await.map_err(step_error)?;
                Ok(answer)
            })
        })
        .await?)
}

async fn finish_refusal<C, D>(
    ctx: &mut C,
    request: &Registration,
    name: &str,
    refusal: RegistrationRefusal,
) -> Result<RegistrationAnswer, LeaseError>
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
    Ok(ctx
        .local(step(name)?, move |scope| {
            Box::pin(async move {
                let mut storage = RegistryStorage(scope);
                let mut attempt = storage
                    .lifecycle(request.attempt)
                    .await
                    .map_err(step_error)?
                    .ok_or_else(|| {
                        step_error(LeaseError::Corrupt {
                            field: "registration attempt".into(),
                        })
                    })?;
                let answer = RegistrationAnswer::Refused(
                    if storage
                        .database(request.database)
                        .await
                        .map_err(step_error)?
                        .is_some_and(|record| {
                            matches!(
                                record.lifecycle,
                                DatabaseLifecycle::Retired { .. }
                                    | DatabaseLifecycle::Retiring { .. }
                            )
                        })
                    {
                        RegistrationRefusal::Retired
                    } else {
                        refusal
                    },
                );
                attempt.outcome = Some(LifecycleOutcome::Registration(answer.clone()));
                storage.save_lifecycle(&attempt).await.map_err(step_error)?;
                Ok(answer)
            })
        })
        .await?)
}
