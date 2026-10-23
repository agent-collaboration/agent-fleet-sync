//! Certainty is assigned only after the store has returned from its boundary.
use crate::{ApplyFailure, ReceiverError, StoreError as ReceiverStoreError};
use durable_steps::{CallFailure, CallRetryReason, OperationFailure, StoreError};
use fleet_sync_protocol::{
    CopyUnavailableReason, DiagnosticText, OutcomeUnknownReason, PieceRefusal, ReplicationAnswer,
    SeedAnswer, TransactionKey,
};
use serde::{Deserialize, Serialize};

const PIECE_REFUSED: &str = "copy_piece_refused";
const DISCARDING_PARTS: &str = "copy_piece_refused_discarding_parts";
const SEED_REFUSED: &str = "copy_seed_refused";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscardingPartsRefusal {
    refusal: PieceRefusal,
    transaction: TransactionKey,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CopyTransactionFailure {
    Unavailable(CopyUnavailableReason),
    Refused(PieceRefusal),
    SeedRefused(SeedAnswer),
    /// Commit `discard_staged_parts` in a new transaction before answering Refused.
    RefusedDiscardingParts {
        refusal: PieceRefusal,
        transaction: TransactionKey,
    },
    OutcomeUnknown(OutcomeUnknownReason),
}
impl CopyTransactionFailure {
    /// Convert a completed failure. Match `RefusedDiscardingParts` first and
    /// commit its cleanup; this conversion fails closed when cleanup is still owed.
    pub fn answer(self) -> ReplicationAnswer {
        match self {
            Self::Unavailable(reason) => ReplicationAnswer::CopyUnavailable { reason },
            Self::Refused(refusal) => ReplicationAnswer::Refused(refusal),
            Self::SeedRefused(answer) => ReplicationAnswer::SeedComplete(answer),
            Self::RefusedDiscardingParts { .. } => unknown(StoreError::Corrupt {
                field: "copy refusal cleanup".into(),
                message: "staged parts must be discarded before answering".into(),
            })
            .answer(),
            Self::OutcomeUnknown(reason) => ReplicationAnswer::OutcomeUnknown { reason },
        }
    }
    pub fn call_failure(self) -> CallFailure {
        match self {
            Self::Unavailable(reason) => retryable_copy(reason),
            Self::Refused(_)
            | Self::SeedRefused(_)
            | Self::RefusedDiscardingParts { .. }
            | Self::OutcomeUnknown(_) => CallFailure::AnswerLost,
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
/// Use inside an AtomicStore callback that must roll back. Classify the store's
/// returned error only when it preserves markers after successful rollback and
/// replaces them with Backend on a failed storage boundary.
pub fn receiver_rollback(error: ReceiverError) -> StoreError {
    match error {
        ReceiverError::SeedRefused { answer } => refusal_rollback(SEED_REFUSED, &answer),
        ReceiverError::Retired => refusal_rollback(PIECE_REFUSED, &PieceRefusal::Retired),
        ReceiverError::NotWriter { current } => {
            refusal_rollback(PIECE_REFUSED, &PieceRefusal::NotWriter { current })
        }
        ReceiverError::SchemaUnsupported { form } => {
            refusal_rollback(PIECE_REFUSED, &PieceRefusal::SchemaUnsupported { form })
        }
        error @ (ReceiverError::NotInitialized
        | ReceiverError::Storage(_)
        | ReceiverError::InvalidInput { .. }
        | ReceiverError::SnapshotSchema { .. }
        | ReceiverError::UnitTooLarge(_)
        | ReceiverError::FenceConflict) => match unavailable_receiver(&error) {
            Some(reason) => copy_rollback(reason),
            None => StoreError::backend(error),
        },
    }
}
/// Preserve an apply refusal through a successful AtomicStore rollback.
/// Classify the returned store error, then commit any requested staged-part
/// cleanup in a new transaction before answering the terminal refusal.
pub fn apply_rollback(error: ApplyFailure) -> StoreError {
    match error {
        ApplyFailure::Refused(fleet_sync_protocol::PieceRefusal::NotInitialized) => {
            copy_rollback(CopyUnavailableReason::Uninitialized)
        }
        ApplyFailure::Refused(fleet_sync_protocol::PieceRefusal::CopyFull) => {
            copy_rollback(CopyUnavailableReason::StorageFull)
        }
        ApplyFailure::Storage(error) => receiver_rollback(ReceiverError::Storage(error)),
        ApplyFailure::Refused(refusal) => refusal_rollback(PIECE_REFUSED, &refusal),
        ApplyFailure::RefusedDiscardingParts {
            refusal,
            transaction,
        } => refusal_rollback(
            DISCARDING_PARTS,
            &DiscardingPartsRefusal {
                refusal,
                transaction,
            },
        ),
    }
}

fn refusal_rollback(code: &str, payload: &impl Serialize) -> StoreError {
    match serde_json::to_string(payload) {
        Ok(detail) => StoreError::StepRollback {
            failure: Box::new(OperationFailure::Refused {
                code: code.into(),
                detail,
            }),
        },
        Err(error) => StoreError::backend(error),
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
/// The caller's store must return a marker only after successful rollback;
/// decoding it cannot establish the store's failure precedence.
pub fn classify_copy_store_error(error: StoreError) -> CopyTransactionFailure {
    if let StoreError::StepRollback { failure } = &error
        && let OperationFailure::Refused { code, detail } = failure.as_ref()
    {
        let refusal = match code.as_str() {
            SEED_REFUSED => serde_json::from_str(detail)
                .map(CopyTransactionFailure::SeedRefused)
                .ok(),
            PIECE_REFUSED => serde_json::from_str(detail)
                .map(CopyTransactionFailure::Refused)
                .ok(),
            DISCARDING_PARTS => serde_json::from_str::<DiscardingPartsRefusal>(detail)
                .map(|payload| CopyTransactionFailure::RefusedDiscardingParts {
                    refusal: payload.refusal,
                    transaction: payload.transaction,
                })
                .ok(),
            _ => CopyUnavailableReason::decode_retry(code, detail)
                .map(CopyTransactionFailure::Unavailable)
                .ok(),
        };
        match refusal {
            Some(refusal) => return refusal,
            None => {
                return unknown(StoreError::Corrupt {
                    field: "copy rollback reason".into(),
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
