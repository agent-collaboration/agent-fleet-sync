use crate::{registry_records::TransferAdmission, registry_storage::RegistryStorage, *};
use durable_steps::{Admitted, AtomicStore, JournalScope, storage::TransactionScope};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// Admit on the registry transaction before the host drives the returned operation.
pub async fn admit_holder_change<S, D>(
    scope: &mut S::Scope<'_>,
    request: &HolderChangeRequest,
    requested_by: &ActorRef,
    now: SyncTime,
) -> Result<Admitted, LeaseError>
where
    S: AtomicStore,
    D: Database,
    for<'t> S::Scope<'t>: TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    let id = crate::change_checks::attempt(request);
    let admitted = scope
        .admit::<HolderChangeTask<S, D>>(&holder_change_request_key(id)?, request)
        .await
        .map_err(|error| LeaseError::InvalidInput {
            field: error.to_string(),
        })?;
    let mut storage = RegistryStorage(scope);
    if let Some(existing) = storage.admission(id).await? {
        if existing.operation != admitted.id() || existing.request != *request {
            return Err(LeaseError::Corrupt {
                field: "holder-change admission identity".into(),
            });
        }
    } else {
        storage
            .save_admission(&TransferAdmission {
                operation: admitted.id(),
                request: request.clone(),
                requested_by: requested_by.clone(),
                admitted_at: now,
                follow_up: None,
            })
            .await?;
    }
    scope.ensure_wake(now).await.map_err(journal_error)?;
    Ok(admitted)
}

pub async fn cancel_holder_change<S>(
    scope: &mut S,
    cancel: &CancelHolderChange,
    now: SyncTime,
) -> Result<CancelAnswer, LeaseError>
where
    S: TransactionScope + JournalScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let mut storage = RegistryStorage(scope);
    if storage.admission(cancel.attempt).await?.is_none() {
        return Ok(CancelAnswer::NotFound);
    }
    let transfer = storage.transfer(cancel.attempt).await?;
    let phase = transfer
        .as_ref()
        .map_or(HolderChangePhase::Requested, |record| record.phase.clone());
    if let Some(recorded) = storage.cancel(cancel.attempt).await? {
        return Ok(CancelAnswer::Accepted { recorded, phase });
    }
    if let Some(answer) = transfer.as_ref().and_then(|record| record.outcome.clone()) {
        return Ok(CancelAnswer::Ended(answer));
    }
    if !phase.cancellable() {
        return Ok(CancelAnswer::TooLate { phase });
    }
    let recorded = CancelRecord {
        by: cancel.by.clone(),
        reason: cancel.reason.clone(),
        at: now,
    };
    storage.save_cancel(cancel.attempt, &recorded).await?;
    scope.ensure_wake(now).await.map_err(journal_error)?;
    Ok(CancelAnswer::Accepted { recorded, phase })
}

pub(crate) fn journal_error(error: durable_steps::StoreError) -> LeaseError {
    LeaseError::Operation(durable_steps::OperationFailure::Storage { error })
}

pub(crate) async fn bound_operation<S: durable_steps::storage::JournalStorage>(
    scope: &mut S,
    id: durable_steps::OperationId,
    kind: &str,
    key: &durable_steps::RequestKey,
) -> Result<durable_steps::storage::OperationRecord, LeaseError> {
    let record = scope
        .operation(id)
        .await
        .map_err(journal_error)?
        .ok_or_else(|| LeaseError::Corrupt {
            field: "holder-change operation binding".into(),
        })?;
    if record.kind != kind || record.request_key != *key {
        return Err(LeaseError::Corrupt {
            field: "holder-change operation binding".into(),
        });
    }
    Ok(record)
}
