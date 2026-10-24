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
pub(crate) fn check_status_route<D: DeliveryTypes>(
    route: &D::StatusRoute,
) -> Result<(), DeliveryError> {
    check(
        route,
        D::MAX_STATUS_ROUTE_BYTES,
        DeliverySizeField::StatusRoute,
    )
}
pub(crate) fn check_item<D: DeliveryTypes>(item: &DeliveryItem<D>) -> Result<(), DeliveryError> {
    body_acceptance_bound::<D>(item.body())?;
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
    envelope_acceptance_bound(item)?;
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
    maximum: u32,
) -> Result<(), DeliveryError> {
    // Every other variant contains only closed protocol types with fixed bounds.
    if let crate::LocalOutcome::Delivered { acceptance } = outcome {
        check(acceptance, maximum, DeliverySizeField::Acceptance)?;
    }
    Ok(())
}

/// Legacy consumers omit the field to preserve their stored representation.
/// Opted-in items retain their admission bound even if the global ceiling changes.
pub(crate) fn body_acceptance_bound<D: DeliveryTypes>(
    body: &D::Body,
) -> Result<Option<u32>, crate::DeliveryConfigError> {
    if !D::PER_ITEM_SETTLEMENT {
        return Ok(None);
    }
    let supplied = D::max_acceptance_bytes_for(body);
    if supplied > D::MAX_ACCEPTANCE_BYTES {
        return Err(crate::DeliveryConfigError::AcceptanceBoundTooLarge {
            supplied,
            maximum: D::MAX_ACCEPTANCE_BYTES,
        });
    }
    Ok(Some(supplied))
}
pub(crate) fn envelope_acceptance_bound<D: DeliveryTypes>(
    envelope: &DeliveryEnvelope<D>,
) -> Result<Option<u32>, crate::DeliveryConfigError> {
    match envelope {
        DeliveryEnvelope::Item(item) => body_acceptance_bound::<D>(item.body()),
        DeliveryEnvelope::Cancel(_) => Ok(None),
    }
}
pub(crate) fn stored_acceptance_bound<D: DeliveryTypes>(
    bound: Option<u32>,
) -> Result<u32, DeliveryError> {
    let maximum = bound.unwrap_or(D::MAX_ACCEPTANCE_BYTES);
    if maximum > D::MAX_ACCEPTANCE_BYTES {
        return Err(DeliveryError::Corrupt {
            field: "stored acceptance bound",
        });
    }
    Ok(maximum)
}
pub(crate) fn check_status_bound<D: DeliveryTypes>(
    status: &DeliveryStatus<D::Acceptance>,
    bound: Option<u32>,
) -> Result<(), DeliveryError> {
    let maximum = stored_acceptance_bound::<D>(bound)?;
    if let DeliveryStatus::Delivered { acceptance } = status {
        check(acceptance, maximum, DeliverySizeField::Acceptance)?;
    }
    Ok(())
}
pub(crate) fn check_outcome_bound<D: DeliveryTypes>(
    outcome: &RecipientOutcome<D::Acceptance>,
    bound: Option<u32>,
) -> Result<(), DeliveryError> {
    let maximum = stored_acceptance_bound::<D>(bound)?;
    if let RecipientOutcome::Delivered { acceptance } = outcome {
        check(acceptance, maximum, DeliverySizeField::Acceptance)?;
    }
    Ok(())
}
