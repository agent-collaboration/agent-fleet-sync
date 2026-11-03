use crate::{
    DeliveryError, checked_queries::StoredOutboxRow, custody_record::*, outbox_record::*,
    stored_outcome::*,
};
use fleet_sync_protocol::*;
use serde::de::DeserializeOwned;

#[cfg(test)]
#[path = "outbox_storage_test.rs"]
mod tests;

pub(crate) fn decode_header<A: DeserializeOwned>(
    row: &StoredOutboxRow,
) -> Result<OutboxHeaderState<A>, DeliveryError> {
    let id: ItemId = stored_enum(row.item_id.clone(), "outbox item id")?;
    if row.record_kind.as_deref() == Some("unresolvable") {
        return Ok(OutboxHeaderState::Unresolvable {
            item: id,
            kind: stored_enum(row.failure_kind.clone(), "outbox failure")?,
        });
    }
    let status = stored_status(
        row.status_kind.clone(),
        optional_number(
            row.status_sequence.clone(),
            row.status_sequence_class.clone(),
            "outbox status sequence",
        )?,
        row.status_acceptance.clone(),
        RefusalFields {
            kind: row.refusal_kind.clone(),
            code: row.refusal_code.clone(),
            failure: row.refusal_failure.clone(),
            size_field: row.refusal_size_field.clone(),
            digest: row.refusal_digest.clone(),
            item_id: row.refusal_item_id.clone(),
        },
    )?;
    let version = StatusVersion::new(integer(
        row.status_version.clone(),
        row.status_version_class.clone(),
        "outbox status version",
    )?);
    let cancel = stored_cancel(row.cancel_outcome.clone(), row.cancel_too_late_at.clone())?;
    let acceptance_bound = optional_number(
        row.acceptance_bound.clone(),
        row.acceptance_bound_class.clone(),
        "outbox acceptance bound",
    )?
    .map(u32::try_from)
    .transpose()
    .map_err(|_| DeliveryError::Corrupt {
        field: "outbox acceptance bound",
    })?;
    let digest = required(row.digest.clone(), "outbox digest")?;
    match row.record_kind.as_deref() {
        Some("recovered") => Ok(OutboxHeaderState::Recovered(RecoveredOutbox {
            acceptance_bound,
            item: id,
            digest: stored_enum(Some(digest), "outbox digest")?,
            status,
            version,
            cancel,
        })),
        Some("item") => Ok(OutboxHeaderState::Item(OutboxHeader {
            id,
            digest,
            status_route: row.status_route.clone(),
            relay: stored_enum(row.relay_machine_id.clone(), "outbox relay")?,
            submitter: row
                .submitter_machine_id
                .clone()
                .map(|value| stored_enum(Some(value), "outbox submitter"))
                .transpose()?,
            recipient: stored_enum(row.recipient_machine_id.clone(), "outbox recipient")?,
            created_at: stored_time(
                row.created_at.clone(),
                row.created_at_class.clone(),
                "outbox created at",
            )?
            .ok_or(DeliveryError::Corrupt {
                field: "outbox created at",
            })?,
            admitted_at: stored_time(
                row.admitted_at.clone(),
                row.admitted_at_class.clone(),
                "outbox admitted at",
            )?,
            handed_off_at: stored_time(
                row.handed_off_at.clone(),
                row.handed_off_at_class.clone(),
                "outbox handed off at",
            )?,
            last_report_seq: ReportSeq::new(integer(
                row.last_report_seq.clone(),
                row.last_report_seq_class.clone(),
                "outbox report sequence",
            )?),
            valid_until: stored_time(
                row.valid_until.clone(),
                row.valid_until_class.clone(),
                "outbox valid until",
            )?,
            status,
            version,
            submit_attempted: stored_bool(
                row.submit_attempted.clone(),
                row.submit_attempted_class.clone(),
                "outbox submit attempted",
            )?,
            cancel,
            cancel_target: row
                .cancel_target_item_id
                .clone()
                .map(|value| stored_enum(Some(value), "outbox cancel target"))
                .transpose()?,
            acceptance_bound,
            settlement_bytes: u32::try_from(integer(
                row.settlement_bytes.clone(),
                row.settlement_bytes_class.clone(),
                "outbox settlement reserve",
            )?)
            .map_err(|_| DeliveryError::Corrupt {
                field: "outbox settlement reserve",
            })?,
            capacity_slot: stored_bool(
                row.capacity_slot.clone(),
                row.capacity_slot_class.clone(),
                "outbox capacity slot",
            )?,
        })),
        _ => Err(DeliveryError::Corrupt {
            field: "outbox record kind",
        }),
    }
}
