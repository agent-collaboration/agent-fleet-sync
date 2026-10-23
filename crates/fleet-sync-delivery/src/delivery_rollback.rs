//! Delivery certainty is assigned from the final store result after rollback.
use crate::DeliveryError;
use durable_steps::{OperationFailure, StoreError};
use fleet_sync_protocol::DeliveryRefusal;
use serde::{Deserialize, Serialize};

const DELIVERY_REFUSED: &str = "delivery_refused";
const DELIVERY_CORRUPT: &str = "delivery_custody_corrupt";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CorruptCustody {
    field: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DeliveryTransactionFailure {
    #[error("delivery refused: {0:?}")]
    Refused(DeliveryRefusal),
    #[error("invalid stored delivery field {field}")]
    Corrupt { field: String },
    #[error("delivery transaction outcome unknown: {0}")]
    OutcomeUnknown(StoreError),
}

/// Use in an AtomicStore callback that must roll back. Only classify the final
/// store result: a failed rollback must replace this marker with a storage error.
pub fn delivery_rollback(error: DeliveryError) -> StoreError {
    match error {
        DeliveryError::Refused { reason } => encode_rollback(DELIVERY_REFUSED, &reason),
        DeliveryError::Corrupt { field } => encode_rollback(
            DELIVERY_CORRUPT,
            &CorruptCustody {
                field: field.into(),
            },
        ),
        error => StoreError::backend(error),
    }
}

fn encode_rollback(code: &str, payload: &impl Serialize) -> StoreError {
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

/// Recognize only delivery-owned markers returned after successful rollback.
/// Malformed marker data is uncertainty, never evidence of corrupt custody.
pub fn classify_delivery_store_error(error: StoreError) -> DeliveryTransactionFailure {
    if let StoreError::StepRollback { failure } = &error
        && let OperationFailure::Refused { code, detail } = failure.as_ref()
    {
        match code.as_str() {
            DELIVERY_REFUSED => {
                if let Ok(reason) = serde_json::from_str::<DeliveryRefusal>(detail) {
                    return DeliveryTransactionFailure::Refused(reason);
                }
            }
            DELIVERY_CORRUPT => {
                if let Ok(payload) = serde_json::from_str::<CorruptCustody>(detail)
                    && !payload.field.trim().is_empty()
                    && !payload.field.chars().any(char::is_control)
                {
                    return DeliveryTransactionFailure::Corrupt {
                        field: payload.field,
                    };
                }
            }
            _ => return DeliveryTransactionFailure::OutcomeUnknown(error),
        }
        return DeliveryTransactionFailure::OutcomeUnknown(StoreError::Corrupt {
            field: "delivery rollback reason".into(),
            message: "malformed delivery rollback payload".into(),
        });
    }
    DeliveryTransactionFailure::OutcomeUnknown(error)
}
