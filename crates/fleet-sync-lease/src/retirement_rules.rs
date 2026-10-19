use crate::{
    CopyRetire, LeaseError, RegistryConfig, RetirementAdmission,
    operation_support::{Decision, open_lifecycle, saved_failure, step, step_error},
    registry_records::*,
    registry_storage::RegistryStorage,
};
use durable_steps::{AtomicStore, OperationContext, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// Seal retirement before deleting the copy. A failed attempt leaves Retiring for a later attempt.
pub async fn retire<C, D>(
    ctx: &mut C,
    request: &Retirement,
    config: &RegistryConfig,
) -> Result<RetirementAnswer, LeaseError>
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
    let decision = ctx
        .local(step("check")?, move |scope| {
            Box::pin(async move {
                let record = RegistryStorage(scope)
                    .database(offered.database)
                    .await
                    .map_err(step_error)?
                    .ok_or_else(|| step_error(LeaseError::NotRegistered))?;
                if let DatabaseLifecycle::Retired { at } = record.lifecycle {
                    return Ok(Decision::Answer(RetirementAnswer::Retired { at }));
                }
                let mut attempt =
                    open_lifecycle(scope, operation, LifecycleIntent::Retire(offered.clone()))
                        .await?;
                if let Some(outcome) = attempt.outcome {
                    if let Some(error) = saved_failure(&outcome) {
                        return Err(step_error(error));
                    }
                    return match outcome {
                        LifecycleOutcome::Retirement(answer) => Ok(Decision::Answer(answer)),
                        _ => Err(step_error(LeaseError::Corrupt {
                            field: "retirement outcome kind".into(),
                        })),
                    };
                }
                let mut storage = RegistryStorage(scope);
                let refusal = if storage
                    .lease(offered.database)
                    .await
                    .map_err(step_error)?
                    .is_some()
                {
                    Some(RetirementRefusal::LeaseHeld)
                } else if storage
                    .active_transfer(offered.database)
                    .await
                    .map_err(step_error)?
                    .is_some()
                {
                    Some(RetirementRefusal::TransferInProgress)
                } else {
                    None
                };
                if let Some(refusal) = refusal {
                    let answer = RetirementAnswer::Refused(refusal);
                    attempt.outcome = Some(LifecycleOutcome::Retirement(answer.clone()));
                    storage.save_lifecycle(&attempt).await.map_err(step_error)?;
                    return Ok(Decision::Answer(answer));
                }
                Ok(Decision::Continue)
            })
        })
        .await?;
    if let Decision::Answer(answer) = decision {
        return Ok(answer);
    }
    let gate = ctx
        .call(
            step("admit")?,
            RetirementAdmission,
            fleet_sync_protocol::RetirementAdmission {
                database: request.database,
            },
        )
        .await?;
    match gate {
        GateAnswer::Refused { reason } => {
            return finish::<C, D>(
                ctx,
                request,
                "refuse_admission",
                Some(RetirementRefusal::GateRefused { reason }),
            )
            .await;
        }
        GateAnswer::Admitted => {}
        GateAnswer::Reserved => {
            return Err(LeaseError::InvalidInput {
                field: "retirement gate returned Reserved".into(),
            });
        }
    }
    let offered = request.clone();
    let retiring_at = ctx.now();
    let decision = ctx
        .local(step("retiring")?, move |scope| {
            Box::pin(async move {
                let mut storage = RegistryStorage(scope);
                let mut record = storage
                    .database(offered.database)
                    .await
                    .map_err(step_error)?
                    .ok_or_else(|| step_error(LeaseError::NotRegistered))?;
                if let DatabaseLifecycle::Retired { at } = record.lifecycle {
                    return Ok(Decision::Answer(RetirementAnswer::Retired { at }));
                }
                let refusal = if storage
                    .lease(offered.database)
                    .await
                    .map_err(step_error)?
                    .is_some()
                {
                    Some(RetirementRefusal::LeaseHeld)
                } else if storage
                    .active_transfer(offered.database)
                    .await
                    .map_err(step_error)?
                    .is_some()
                {
                    Some(RetirementRefusal::TransferInProgress)
                } else {
                    None
                };
                if let Some(refusal) = refusal {
                    let answer = RetirementAnswer::Refused(refusal);
                    let mut attempt = storage
                        .lifecycle(offered.attempt)
                        .await
                        .map_err(step_error)?
                        .ok_or_else(|| {
                            step_error(LeaseError::Corrupt {
                                field: "retirement attempt".into(),
                            })
                        })?;
                    attempt.outcome = Some(LifecycleOutcome::Retirement(answer.clone()));
                    storage.save_lifecycle(&attempt).await.map_err(step_error)?;
                    return Ok(Decision::Answer(answer));
                }
                record
                    .change_lifecycle(
                        DatabaseLifecycle::Retiring {
                            attempt: offered.attempt,
                        },
                        retiring_at,
                    )
                    .map_err(step_error)?;
                storage.save_database(&record).await.map_err(step_error)?;
                Ok(Decision::Continue)
            })
        })
        .await?;
    if let Decision::Answer(answer) = decision {
        return Ok(answer);
    }
    let retired = ctx
        .call(
            step("retire_copy")?,
            CopyRetire {
                copy: config.authoritative_receiver,
            },
            RetireCopy {
                database: request.database,
                expect_fence: request.expect_fence,
                expect_acked: request.expect_acked,
            },
        )
        .await?;
    match retired {
        RetireCopyAnswer::Retired => finish::<C, D>(ctx, request, "retire", None).await,
        RetireCopyAnswer::Mismatch { fence, acked } => {
            finish::<C, D>(
                ctx,
                request,
                "refuse_copy_mismatch",
                Some(RetirementRefusal::CopyMismatch { fence, acked }),
            )
            .await
        }
        RetireCopyAnswer::NotInitialized => {
            finish::<C, D>(
                ctx,
                request,
                "refuse_missing_copy",
                Some(RetirementRefusal::CopyNotInitialized),
            )
            .await
        }
    }
}

async fn finish<C, D>(
    ctx: &mut C,
    request: &Retirement,
    name: &str,
    refusal: Option<RetirementRefusal>,
) -> Result<RetirementAnswer, LeaseError>
where
    C: OperationContext,
    D: Database,
    for<'t> <C::Store as AtomicStore>::Scope<'t>: TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    let offered = request.clone();
    let now = ctx.now();
    Ok(ctx.local(step(name)?,move|scope|Box::pin(async move{
        let mut storage=RegistryStorage(scope);
        let mut attempt=storage.lifecycle(offered.attempt).await.map_err(step_error)?.ok_or_else(||step_error(LeaseError::Corrupt{field:"retirement attempt".into()}))?;
        let mut record=storage.database(offered.database).await.map_err(step_error)?.ok_or_else(||step_error(LeaseError::NotRegistered))?;
        let answer=if let DatabaseLifecycle::Retired{at}=record.lifecycle{RetirementAnswer::Retired{at}}
            else if let Some(refusal)=refusal{RetirementAnswer::Refused(refusal)}else{
                if !matches!(record.lifecycle,DatabaseLifecycle::Retiring{attempt} if attempt==offered.attempt){return Err(step_error(LeaseError::Corrupt{field:"retirement seal".into()}));}
                record.change_lifecycle(DatabaseLifecycle::Retired{at:now},now).map_err(step_error)?;
                storage.save_database(&record).await.map_err(step_error)?;
                RetirementAnswer::Retired{at:now}
            };
        attempt.outcome=Some(LifecycleOutcome::Retirement(answer.clone()));
        storage.save_lifecycle(&attempt).await.map_err(step_error)?;
        Ok(answer)
    })).await?)
}
