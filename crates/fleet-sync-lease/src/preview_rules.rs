use crate::{
    CopyRead, LeaseError, RegistryConfig,
    operation_support::{step, step_error},
    registry_records::{DatabaseLifecycle, PreviewRecord},
    registry_storage::RegistryStorage,
};
use durable_steps::{AtomicStore, OperationContext, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// Observe the authoritative copy outside a transaction, then save its confirmation token.
pub async fn preview_takeover<C, D>(
    ctx: &mut C,
    request: &PreviewTakeover,
    config: &RegistryConfig,
) -> Result<TakeoverPreview, LeaseError>
where
    C: OperationContext,
    D: Database,
    for<'t> <C::Store as AtomicStore>::Scope<'t>: TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    let database = request.database;
    let lease = ctx
        .local(step("check")?, move |scope| {
            Box::pin(async move {
                let mut storage = RegistryStorage(scope);
                let record = storage
                    .database(database)
                    .await
                    .map_err(step_error)?
                    .ok_or_else(|| step_error(LeaseError::NotRegistered))?;
                if matches!(
                    record.lifecycle,
                    DatabaseLifecycle::Retired { .. } | DatabaseLifecycle::Retiring { .. }
                ) {
                    return Err(step_error(LeaseError::Retired));
                }
                storage
                    .lease(database)
                    .await
                    .map_err(step_error)?
                    .ok_or_else(|| step_error(LeaseError::NotHeld))
            })
        })
        .await?;
    let status = ctx
        .call(
            step("copy_status")?,
            CopyRead {
                copy: config.authoritative_receiver,
                database,
            },
            (),
        )
        .await?;
    let status = match status {
        CopyStatusAnswer::Status(status) => status,
        CopyStatusAnswer::NotInitialized => return Err(LeaseError::NoAuthoritativeCopy),
        CopyStatusAnswer::Retired => return Err(LeaseError::Retired),
    };
    if status.database != database
        || status.role != CopyRole::Authoritative
        || status.fence < lease.epoch
    {
        return Err(LeaseError::Corrupt {
            field: "authoritative copy preview".into(),
        });
    }
    let token = PreviewToken::new(ctx.id().as_uuid().to_string()).map_err(|error| {
        LeaseError::InvalidInput {
            field: error.to_string(),
        }
    })?;
    Ok(ctx
        .local(step("save")?, move |scope| {
            Box::pin(async move {
                let mut storage = RegistryStorage(scope);
                let record = storage
                    .database(database)
                    .await
                    .map_err(step_error)?
                    .ok_or_else(|| step_error(LeaseError::NotRegistered))?;
                if matches!(
                    record.lifecycle,
                    DatabaseLifecycle::Retired { .. } | DatabaseLifecycle::Retiring { .. }
                ) {
                    return Err(step_error(LeaseError::Retired));
                }
                let current = storage
                    .lease(database)
                    .await
                    .map_err(step_error)?
                    .ok_or_else(|| step_error(LeaseError::NotHeld))?;
                if current.epoch != lease.epoch || current.holder != lease.holder {
                    return Err(step_error(LeaseError::PreviewStale));
                }
                let preview = TakeoverPreview {
                    token,
                    fence: status.fence,
                    acked: status.acked,
                    holder: current.holder,
                    last_seen: Some(current.last_renewed_at),
                };
                storage
                    .save_preview(&PreviewRecord {
                        database,
                        preview: preview.clone(),
                    })
                    .await
                    .map_err(step_error)?;
                Ok(preview)
            })
        })
        .await?)
}
