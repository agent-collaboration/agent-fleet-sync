use crate::{
    DeliveryError, checked_queries::StoredInboxRow, custody_record::*, inbox_record::*,
    stored_outcome::*,
};
use fleet_sync_protocol::*;

pub(crate) fn decode_header<D: DeliveryTypes>(
    row: &StoredInboxRow,
) -> Result<InboxHeader<D>, DeliveryError> {
    let refusal = || RefusalFields {
        kind: row.refusal_kind.clone(),
        code: row.refusal_code.clone(),
        failure: row.refusal_failure.clone(),
        size_field: row.refusal_size_field.clone(),
        digest: row.refusal_digest.clone(),
        item_id: row.refusal_item_id.clone(),
    };
    let field = "inbox progress";
    let attempt = || {
        integer(
            row.attempt.clone(),
            row.attempt_class.clone(),
            "inbox attempt",
        )
    };
    let outcome = || -> Result<RecipientOutcome<D::Acceptance>, DeliveryError> {
        Ok(match row.outcome_kind.as_deref() {
            Some("held") => RecipientOutcome::Held,
            Some("delivered") => RecipientOutcome::Delivered {
                acceptance: decode(row.outcome_acceptance.clone(), "inbox acceptance")?,
            },
            Some("uncertain") => RecipientOutcome::Uncertain,
            Some("expired") => RecipientOutcome::Expired,
            Some("dropped") => RecipientOutcome::Dropped,
            Some("prevented") => RecipientOutcome::Prevented {
                cancel: stored_enum(row.outcome_cancel_item_id.clone(), "inbox outcome cancel")?,
            },
            Some("tooLate") => RecipientOutcome::TooLate {
                cancel: stored_enum(row.outcome_cancel_item_id.clone(), "inbox outcome cancel")?,
                at: stored_enum(row.outcome_too_late_at.clone(), "inbox too late")?,
            },
            Some("refused") => RecipientOutcome::Refused {
                reason: refusal().decode()?,
            },
            _ => return Err(DeliveryError::Corrupt { field }),
        })
    };
    let progress = match row.progress.as_deref() {
        Some("pending") => InboxProgress::Pending {
            attempt: attempt()?,
            not_before: stored_time(
                row.not_before.clone(),
                row.not_before_class.clone(),
                "inbox not before",
            )?
            .ok_or(DeliveryError::Corrupt { field })?,
        },
        Some("attempting") => InboxProgress::Attempting {
            attempt: attempt()?,
        },
        Some("held") => InboxProgress::Held {
            attempt: attempt()?,
        },
        Some("final") => InboxProgress::Final {
            attempt: attempt()?,
            outcome: outcome()?,
        },
        Some("control") => InboxProgress::Control {
            outcome: match row.outcome_kind.as_deref() {
                Some("pending") => ControlOutcome::Pending,
                Some("prevented") => ControlOutcome::Prevented,
                Some("tooLate") => ControlOutcome::TooLate {
                    at: stored_enum(row.outcome_too_late_at.clone(), "inbox too late")?,
                },
                Some("refused") => ControlOutcome::Refused {
                    reason: refusal().decode()?,
                },
                Some("expired") => ControlOutcome::Expired,
                _ => return Err(DeliveryError::Corrupt { field }),
            },
        },
        _ => return Err(DeliveryError::Corrupt { field }),
    };
    Ok(InboxHeader {
        id: stored_enum(row.item_id.clone(), "inbox item id")?,
        digest: required(row.digest.clone(), "inbox digest")?,
        sender: decode(row.sender.clone(), "inbox sender")?,
        recipient: Recipient {
            peer: stored_enum(row.recipient_machine_id.clone(), "inbox recipient")?,
            address: decode(row.recipient_address.clone(), "inbox address")?,
        },
        relay: row
            .relay_machine_id
            .clone()
            .map(|value| stored_enum(Some(value), "inbox relay"))
            .transpose()?,
        cancel_target: row
            .cancel_target_item_id
            .clone()
            .map(|value| stored_enum(Some(value), "inbox cancel target"))
            .transpose()?,
        valid_until: stored_time(
            row.valid_until.clone(),
            row.valid_until_class.clone(),
            "inbox valid until",
        )?,
        stream: stored_enum(row.stream.clone(), "inbox stream")?,
        route: stored_enum(row.route.clone(), "inbox route")?,
        progress,
        report_seq: ReportSeq::new(integer(
            row.report_seq.clone(),
            row.report_seq_class.clone(),
            "inbox report sequence",
        )?),
        acceptance_bound: optional_number(
            row.acceptance_bound.clone(),
            row.acceptance_bound_class.clone(),
            "inbox acceptance bound",
        )?
        .map(u32::try_from)
        .transpose()
        .map_err(|_| DeliveryError::Corrupt {
            field: "inbox acceptance bound",
        })?,
        settlement_bytes: u32::try_from(integer(
            row.settlement_bytes.clone(),
            row.settlement_bytes_class.clone(),
            "inbox settlement reserve",
        )?)
        .map_err(|_| DeliveryError::Corrupt {
            field: "inbox settlement reserve",
        })?,
        release_requested: stored_bool(
            row.release_requested.clone(),
            row.release_requested_class.clone(),
            "inbox release requested",
        )?,
        created_at: stored_time(
            row.created_at.clone(),
            row.created_at_class.clone(),
            "inbox created at",
        )?
        .ok_or(DeliveryError::Corrupt {
            field: "inbox created at",
        })?,
        accepted_at: stored_time(
            row.accepted_at.clone(),
            row.accepted_at_class.clone(),
            "inbox accepted at",
        )?,
        delivered_at: stored_time(
            row.delivered_at.clone(),
            row.delivered_at_class.clone(),
            "inbox delivered at",
        )?,
    })
}
