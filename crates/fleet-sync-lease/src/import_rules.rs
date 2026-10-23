use crate::{
    CopyCreate, LeaseError, RegistryConfig,
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

fn existing_import(record: DatabaseRecord, request: &Import) -> ImportAnswer {
    match record.lifecycle {
        DatabaseLifecycle::Retired { .. } | DatabaseLifecycle::Retiring { .. } => {
            ImportAnswer::Retired
        }
        DatabaseLifecycle::Unheld { import_digest }
        | DatabaseLifecycle::Registered {
            imported_from: Some(ImportOrigin {
                digest: import_digest,
            }),
            ..
        } => {
            if import_digest == request.digest {
                ImportAnswer::AlreadyImported
            } else {
                ImportAnswer::ImportConflict {
                    existing: Some(import_digest),
                    offered: request.digest.clone(),
                }
            }
        }
        DatabaseLifecycle::Registered {
            imported_from: None,
            ..
        } => ImportAnswer::ImportConflict {
            existing: None,
            offered: request.digest.clone(),
        },
    }
}

/// Create an unheld epoch-one copy, then reserve its database identity for the offered import.
pub async fn import<C, D>(
    ctx: &mut C,
    request: &Import,
    config: &RegistryConfig,
) -> Result<ImportAnswer, LeaseError>
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
                    return Ok(Decision::Answer(ImportAnswer::Retired));
                }
                let mut attempt =
                    open_lifecycle(scope, operation, LifecycleIntent::Import(offered.clone()))
                        .await?;
                if let Some(outcome) = attempt.outcome {
                    if let Some(error) = saved_failure(&outcome) {
                        return Err(step_error(error));
                    }
                    return match outcome {
                        LifecycleOutcome::Import(answer) => Ok(Decision::Answer(answer)),
                        _ => Err(step_error(LeaseError::Corrupt {
                            field: "import outcome kind".into(),
                        })),
                    };
                }
                let Some(database) = database else {
                    return Ok(Decision::Continue);
                };
                let answer = existing_import(database, &offered);
                attempt.outcome = Some(LifecycleOutcome::Import(answer.clone()));
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
    let epoch = Epoch::new(1).map_err(|error| LeaseError::InvalidInput {
        field: error.to_string(),
    })?;
    let answer = ctx
        .call(
            step("create_copy")?,
            CopyCreate {
                copy: config.authoritative_receiver,
            },
            CreateCopy {
                database: request.database,
                epoch,
                holder: None,
                origin: CopyOrigin::AwaitingSeed {
                    expected_position: None,
                },
            },
        )
        .await?;
    let terminal = match answer {
        CreateCopyAnswer::Retired => Some(ImportAnswer::Retired),
        CreateCopyAnswer::CreationConflict { .. } => Some(ImportAnswer::ImportConflict {
            existing: None,
            offered: request.digest.clone(),
        }),
        CreateCopyAnswer::CopyCreated | CreateCopyAnswer::AlreadyCreated => None,
    };
    let request = request.clone();
    let now = ctx.now();
    Ok(ctx
        .local(step("reserve")?, move |scope| {
            Box::pin(async move {
                let mut storage = RegistryStorage(scope);
                let mut attempt = storage
                    .lifecycle(request.attempt)
                    .await
                    .map_err(step_error)?
                    .ok_or_else(|| {
                        step_error(LeaseError::Corrupt {
                            field: "import attempt".into(),
                        })
                    })?;
                let database = storage
                    .database(request.database)
                    .await
                    .map_err(step_error)?;
                let answer = if let Some(database) = database {
                    existing_import(database, &request)
                } else if let Some(terminal) = terminal {
                    terminal
                } else {
                    if storage
                        .floor(request.database)
                        .await
                        .map_err(step_error)?
                        .is_some()
                        || storage
                            .lease(request.database)
                            .await
                            .map_err(step_error)?
                            .is_some()
                    {
                        return Err(step_error(LeaseError::Corrupt {
                            field: "unknown import already has registry authority".into(),
                        }));
                    }
                    storage
                        .save_database(&DatabaseRecord::initial(
                            request.database,
                            DatabaseLifecycle::Unheld {
                                import_digest: request.digest.clone(),
                            },
                            epoch,
                            now,
                        ))
                        .await
                        .map_err(step_error)?;
                    storage
                        .save_floor(request.database, epoch)
                        .await
                        .map_err(step_error)?;
                    ImportAnswer::Imported
                };
                attempt.outcome = Some(LifecycleOutcome::Import(answer.clone()));
                storage.save_lifecycle(&attempt).await.map_err(step_error)?;
                Ok(answer)
            })
        })
        .await?)
}
