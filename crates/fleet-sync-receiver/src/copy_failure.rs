//! Certainty is assigned only after the store has returned from its boundary.
use crate::{ApplyFailure, ReceiverError, StoreError as ReceiverStoreError};
use durable_steps::{CallFailure, CallRetryReason, OperationFailure, StoreError};
use fleet_sync_protocol::{
    CopyUnavailableReason, DiagnosticText, OutcomeUnknownReason, ReplicationAnswer,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CopyTransactionFailure {
    Unavailable(CopyUnavailableReason),
    OutcomeUnknown(OutcomeUnknownReason),
}
impl CopyTransactionFailure {
    pub fn answer(self) -> ReplicationAnswer {
        match self {
            Self::Unavailable(reason) => ReplicationAnswer::CopyUnavailable { reason },
            Self::OutcomeUnknown(reason) => ReplicationAnswer::OutcomeUnknown { reason },
        }
    }
    pub fn call_failure(self) -> CallFailure {
        match self {
            Self::Unavailable(reason) => retryable_copy(reason),
            Self::OutcomeUnknown(_) => CallFailure::AnswerLost,
        }
    }
}
pub fn retryable_copy(reason: CopyUnavailableReason) -> CallFailure {
    let (code, detail) = reason.encode_retry();
    CallFailure::Retryable {
        reason: CallRetryReason { code, detail },
    }
}
pub fn copy_rollback(reason: CopyUnavailableReason) -> StoreError {
    let (code, detail) = reason.encode_retry();
    StoreError::StepRollback {
        failure: Box::new(OperationFailure::Refused { code, detail }),
    }
}
/// Use inside an AtomicStore callback. Only a returned rollback preserves this
/// no-new-effect marker; a failed begin/commit/rollback replaces it with Backend.
pub fn receiver_rollback(error: ReceiverError) -> StoreError {
    match unavailable_receiver(&error) {
        Some(reason) => copy_rollback(reason),
        None => StoreError::backend(error),
    }
}
/// Encode a transient apply refusal inside a callback that must roll back.
/// Final domain refusals stay with the caller's normal terminal answer mapping.
pub fn apply_rollback(error: ApplyFailure) -> StoreError {
    match error {
        ApplyFailure::Refused(fleet_sync_protocol::PieceRefusal::NotInitialized) => {
            copy_rollback(CopyUnavailableReason::Uninitialized)
        }
        ApplyFailure::Refused(fleet_sync_protocol::PieceRefusal::CopyFull) => {
            copy_rollback(CopyUnavailableReason::StorageFull)
        }
        ApplyFailure::Storage(error) => receiver_rollback(ReceiverError::Storage(error)),
        error => StoreError::backend(error),
    }
}
pub fn unavailable_receiver(error: &ReceiverError) -> Option<CopyUnavailableReason> {
    match error {
        ReceiverError::NotInitialized => Some(CopyUnavailableReason::Uninitialized),
        ReceiverError::Storage(ReceiverStoreError::Full) => {
            Some(CopyUnavailableReason::StorageFull)
        }
        ReceiverError::Storage(error) => Some(CopyUnavailableReason::Damaged {
            detail: error.to_string(),
        }),
        _ => None,
    }
}
/// Shared by native and DurableObjectStore producers. Backend, Aborted and
/// Corrupt are never promoted to no-effect, even if their text resembles a code.
pub fn classify_copy_store_error(error: StoreError) -> CopyTransactionFailure {
    if let StoreError::StepRollback { failure } = &error
        && let OperationFailure::Refused { code, detail } = failure.as_ref()
    {
        match CopyUnavailableReason::decode_retry(code, detail) {
            Ok(reason) => return CopyTransactionFailure::Unavailable(reason),
            Err(_) => {
                return unknown(StoreError::Corrupt {
                    field: "copy retry reason".into(),
                    message: "unknown or malformed reason".into(),
                });
            }
        }
    }
    unknown(error)
}
fn unknown(error: StoreError) -> CopyTransactionFailure {
    CopyTransactionFailure::OutcomeUnknown(OutcomeUnknownReason::StoreBoundary {
        detail: DiagnosticText::from_error(error),
    })
}
