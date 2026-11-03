use crate::{
    DeliveryError, checked_queries::StoredOutboxRow, custody_record::*, outbox_record::*,
    stored_outcome::*,
};
use fleet_sync_protocol::*;

#[cfg(test)]
#[path = "outbox_storage_test.rs"]
mod tests;

pub(crate) fn decode_header<D: DeliveryTypes>(
    row: &StoredOutboxRow,
) -> Result<OutboxHeaderState<D::Acceptance>, DeliveryError> {
    let id: ItemId = stored_enum(row.item_id.clone(), "outbox item id")?;
    let legacy = stored_bool(
        row.legacy_read_only.clone(),
        row.legacy_read_only_class.clone(),
        "legacy outbox",
    )?;
    let retention = if legacy {
        crate::submitter::SourceRetention::Legacy
    } else if row.record_kind.as_deref() == Some("item") {
        crate::submitter::SourceRetention::Item
    } else {
        crate::submitter::SourceRetention::Negative
    };
    let sender = crate::submitter::checked_sender::<D>(
        row.submitter_kind.clone(),
        row.submitter_id.clone(),
        row.consumer_sender.as_deref(),
        retention,
        "_fleet_sync_outbox_items",
        &id.as_uuid().to_string(),
    )?;

    if row.record_kind.as_deref() == Some("unresolvable") {
        let kind = stored_enum(row.failure_kind.clone(), "outbox failure")?;
        return Ok(if legacy {
            OutboxHeaderState::Legacy {
                item: id,
                digest: None,
                status: DeliveryStatus::Refused {
                    reason: DeliveryRefusal::ProcessingFailed { kind },
                },
                version: StatusVersion::new(0),
                cancel: None,
            }
        } else {
            OutboxHeaderState::Unresolvable { item: id, kind }
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
    if legacy {
        if row.payload.is_some() || status.rank() == 0 {
            return Err(DeliveryError::CorruptRow {
                table: "_fleet_sync_outbox_items",
                key: id.as_uuid().to_string(),
                field: "legacy custody",
            });
        }
        return Ok(OutboxHeaderState::Legacy {
            item: id,
            digest: Some(stored_enum(Some(digest), "legacy digest")?),
            status,
            version,
            cancel,
        });
    }

    match row.record_kind.as_deref() {
        Some("recovered") => Ok(OutboxHeaderState::Recovered(RecoveredOutbox {
            submitter: sender.submitter,
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
            submitter: sender.submitter,
            consumer_sender: row.consumer_sender.clone(),
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
