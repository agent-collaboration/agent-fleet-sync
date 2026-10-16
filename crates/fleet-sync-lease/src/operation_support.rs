use crate::{
    LeaseError,
    registry_records::{LifecycleAttempt, LifecycleIntent, LifecycleOutcome},
    registry_storage::RegistryStorage,
};
use durable_steps::{
    OperationFailure, OperationId, RequestKey, StepError, StepName,
    storage::{JournalStorage, TransactionScope},
};
use fleet_sync_protocol::{FailureCode, RequestId};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub(crate) fn step(name: &str) -> Result<StepName, OperationFailure> {
    StepName::new(name).map_err(|error| OperationFailure::InvalidPayload {
        message: error.to_string(),
    })
}
pub(crate) fn step_error(error: LeaseError) -> StepError {
    match error {
        LeaseError::Storage { message } => {
            StepError::Storage(durable_steps::StoreError::Backend { message })
        }
        error => StepError::Failed(error.into()),
    }
}
pub(crate) fn storage_error(error: LeaseError) -> durable_steps::StoreError {
    match error {
        LeaseError::Storage { message } => durable_steps::StoreError::Backend { message },
        error => durable_steps::StoreError::Corrupt {
            field: "registry operation".into(),
            message: error.to_string(),
        },
    }
}
/// Lifecycle task admission uses its attempt ID as the engine request key.
pub fn lifecycle_request_key(attempt: RequestId) -> Result<RequestKey, LeaseError> {
    RequestKey::new(attempt.as_uuid().to_string()).map_err(|error| LeaseError::InvalidInput {
        field: error.to_string(),
    })
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) enum Decision<T> {
    Continue,
    Answer(T),
}

pub(crate) async fn open_lifecycle<S>(
    scope: &mut S,
    operation: OperationId,
    intent: LifecycleIntent,
) -> Result<LifecycleAttempt, StepError>
where
    S: TransactionScope + JournalStorage,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let record = scope
        .operation(operation)
        .await?
        .ok_or(durable_steps::StoreError::MissingOperation { id: operation })?;
    if record.request_key != lifecycle_request_key(intent.attempt()).map_err(step_error)? {
        return Err(StepError::Failed(OperationFailure::InvalidPayload {
            message: "lifecycle task request key must be its attempt ID".into(),
        }));
    }
    let mut storage = RegistryStorage(scope);
    if let Some(existing) = storage
        .lifecycle(intent.attempt())
        .await
        .map_err(step_error)?
    {
        if existing.intent != intent {
            return Err(step_error(LeaseError::InvalidInput {
                field: "lifecycle attempt has different contents".into(),
            }));
        }
        return Ok(existing);
    }
    if storage
        .active_lifecycle(intent.database())
        .await
        .map_err(step_error)?
        .is_some()
    {
        // Another attempt owns the copy-first sequence. No check result is saved on retry.
        return Err(StepError::Retry {
            after: std::time::Duration::ZERO,
        });
    }
    let attempt = LifecycleAttempt {
        intent,
        outcome: None,
    };
    storage.save_lifecycle(&attempt).await.map_err(step_error)?;
    Ok(attempt)
}

pub(crate) fn failure_code(
    failure: &OperationFailure,
) -> Result<FailureCode, durable_steps::StoreError> {
    let code = match failure {
        OperationFailure::Nondeterministic { .. } => "nondeterministic",
        OperationFailure::VersionMismatch { .. } => "versionMismatch",
        OperationFailure::Uncertain { .. } => "uncertain",
        OperationFailure::Refused { .. } => "refused",
        OperationFailure::InvalidPayload { .. } => "invalidPayload",
        OperationFailure::Retry { .. } => "retry",
        OperationFailure::Storage { .. } => "storage",
    };
    FailureCode::new(code).map_err(durable_steps::StoreError::backend)
}

pub(crate) async fn fail_lifecycle<S>(
    scope: &mut S,
    key: &RequestKey,
    failure: &OperationFailure,
) -> Result<(), durable_steps::StoreError>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let Some(attempt) = uuid::Uuid::parse_str(key.as_str())
        .ok()
        .and_then(|id| RequestId::new(id).ok())
    else {
        return Ok(());
    };
    let mut storage = RegistryStorage(scope);
    if let Some(mut record) = storage.lifecycle(attempt).await.map_err(storage_error)?
        && record.outcome.is_none()
    {
        record.outcome = Some(LifecycleOutcome::Failed {
            reason: failure_code(failure)?,
        });
        storage
            .save_lifecycle(&record)
            .await
            .map_err(storage_error)?;
    }
    Ok(())
}

pub(crate) fn saved_failure(outcome: &LifecycleOutcome) -> Option<LeaseError> {
    match outcome {
        LifecycleOutcome::Failed { reason } => {
            Some(LeaseError::Operation(OperationFailure::Refused {
                code: reason.as_str().to_owned(),
                detail: "lifecycle attempt previously failed".into(),
            }))
        }
        _ => None,
    }
}
