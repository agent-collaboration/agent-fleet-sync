use crate::custody_record::encode;
use crate::{DeliveryAttempt, DeliveryError, DeliveryFailureKind, InboxEffects, ReportRef};
use durable_steps::{
    Admitted, JournalScope, OperationFailure, RequestKey, StepError, StepName, StoreError, Task,
};
use serde::Serialize;

pub(crate) fn step(name: &str) -> Result<StepName, OperationFailure> {
    StepName::new(name).map_err(|error| OperationFailure::InvalidPayload {
        message: error.to_string(),
    })
}
pub fn delivery_request_key(value: &impl Serialize) -> Result<RequestKey, DeliveryError> {
    RequestKey::new(encode(value)?).map_err(|error| DeliveryError::Encoding(error.to_string()))
}
pub(crate) fn step_error(error: DeliveryError) -> StepError {
    match error {
        DeliveryError::Storage(message) => StepError::Storage(StoreError::Backend { message }),
        error => StepError::Failed(OperationFailure::Refused {
            code: error.failure_code().as_str().to_owned(),
            detail: error.to_string(),
        }),
    }
}
pub(crate) fn store_error(error: DeliveryError) -> StoreError {
    match error {
        DeliveryError::Storage(message) => StoreError::Backend { message },
        error => StoreError::Corrupt {
            field: "delivery operation".into(),
            message: error.to_string(),
        },
    }
}
pub(crate) fn failure_code(failure: &OperationFailure) -> crate::FailureCode {
    match failure {
        OperationFailure::Refused { code, .. } => crate::FailureCode::new(code.clone())
            .unwrap_or_else(|_| DeliveryFailureKind::ExecutionFailed.into()),
        OperationFailure::Nondeterministic { .. } => DeliveryFailureKind::Nondeterministic.into(),
        OperationFailure::VersionMismatch { .. } => DeliveryFailureKind::VersionMismatch.into(),
        OperationFailure::InvalidPayload { .. } => DeliveryFailureKind::InvalidPayload.into(),
        _ => DeliveryFailureKind::ExecutionFailed.into(),
    }
}
pub(crate) async fn admit_effects<S, T, R>(
    scope: &mut S,
    effects: InboxEffects,
) -> Result<Vec<Admitted>, DeliveryError>
where
    S: JournalScope,
    T: Task<Input = DeliveryAttempt>,
    R: Task<Input = ReportRef>,
{
    let mut admitted = Vec::new();
    for input in effects.attempts {
        admitted.push(
            scope
                .admit::<T>(&delivery_request_key(&(input.item, input.attempt))?, &input)
                .await
                .map_err(|error| DeliveryError::Storage(error.to_string()))?,
        );
    }
    for input in effects.reports {
        admitted.push(
            scope
                .admit::<R>(
                    &delivery_request_key(&(input.item, input.report_seq))?,
                    &input,
                )
                .await
                .map_err(|error| DeliveryError::Storage(error.to_string()))?,
        );
    }
    Ok(admitted)
}

pub(crate) fn delay_until(now: crate::SyncTime, until: crate::SyncTime) -> std::time::Duration {
    std::time::Duration::from_millis(
        u64::try_from(until.unix_millis().saturating_sub(now.unix_millis())).unwrap_or(0),
    )
}

pub(crate) type TaskTypes<S, Db, D> = std::marker::PhantomData<fn() -> (S, Db, D)>;
