use crate::{
    DeliveryEnvelope, DeliveryError, DeliveryItem, DeliveryRefusal, DeliverySizeField,
    DeliveryStatus, DeliveryTypes, RecipientOutcome,
};
use serde::Serialize;

pub(crate) fn canonical_size(value: &impl Serialize) -> Result<u64, DeliveryError> {
    let mut encoded =
        serde_json::to_value(value).map_err(|error| DeliveryError::Encoding(error.to_string()))?;
    encoded.sort_all_objects();
    let encoded =
        serde_json::to_vec(&encoded).map_err(|error| DeliveryError::Encoding(error.to_string()))?;
    u64::try_from(encoded.len()).map_err(|_| DeliveryError::SequenceExhausted)
}
fn check(
    value: &impl Serialize,
    maximum: u32,
    field: DeliverySizeField,
) -> Result<(), DeliveryError> {
    if canonical_size(value)? > u64::from(maximum) {
        return Err(DeliveryError::Refused {
            reason: DeliveryRefusal::TooLarge { field },
        });
    }
    Ok(())
}
pub(crate) fn check_item<D: DeliveryTypes>(item: &DeliveryItem<D>) -> Result<(), DeliveryError> {
    check(
        item.sender(),
        D::MAX_SENDER_BYTES,
        DeliverySizeField::Sender,
    )?;
    check(
        &item.recipient().address,
        D::MAX_ADDRESS_BYTES,
        DeliverySizeField::Address,
    )
}
pub(crate) fn check_envelope<D: DeliveryTypes>(
    item: &DeliveryEnvelope<D>,
) -> Result<(), DeliveryError> {
    check(
        item.sender(),
        D::MAX_SENDER_BYTES,
        DeliverySizeField::Sender,
    )?;
    check(
        &item.recipient().address,
        D::MAX_ADDRESS_BYTES,
        DeliverySizeField::Address,
    )
}
pub(crate) fn check_status<D: DeliveryTypes>(
    status: &DeliveryStatus<D::Acceptance>,
) -> Result<(), DeliveryError> {
    if let DeliveryStatus::Delivered { acceptance } = status {
        check(
            acceptance,
            D::MAX_ACCEPTANCE_BYTES,
            DeliverySizeField::Acceptance,
        )?;
    }
    Ok(())
}
pub(crate) fn check_outcome<D: DeliveryTypes>(
    outcome: &RecipientOutcome<D::Acceptance>,
) -> Result<(), DeliveryError> {
    if let RecipientOutcome::Delivered { acceptance } = outcome {
        check(
            acceptance,
            D::MAX_ACCEPTANCE_BYTES,
            DeliverySizeField::Acceptance,
        )?;
    }
    Ok(())
}

pub(crate) fn check_local_outcome<D: DeliveryTypes>(
    outcome: &crate::LocalOutcome<D::Acceptance>,
) -> Result<(), DeliveryError> {
    // Every other variant contains only closed protocol types with fixed bounds.
    if let crate::LocalOutcome::Delivered { acceptance } = outcome {
        check(
            acceptance,
            D::MAX_ACCEPTANCE_BYTES,
            DeliverySizeField::Acceptance,
        )?;
    }
    Ok(())
}
