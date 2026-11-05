use crate::{
    DeliveryError, checked_queries::StoredReportRow, custody_record::*, stored_outcome::*,
};
use fleet_sync_protocol::*;

pub(crate) fn decode_report<D: DeliveryTypes>(
    row: &StoredReportRow,
) -> Result<RecipientReport<D>, DeliveryError> {
    let field = "stored report outcome";
    let outcome = match row.outcome_kind.as_deref() {
        Some("held") => RecipientOutcome::Held,
        Some("delivered") => RecipientOutcome::Delivered {
            acceptance: decode(row.outcome_acceptance.clone(), field)?,
        },
        Some("uncertain") => RecipientOutcome::Uncertain,
        Some("expired") => RecipientOutcome::Expired,
        Some("dropped") => RecipientOutcome::Dropped,
        Some("prevented") => RecipientOutcome::Prevented {
            cancel: stored_enum(row.outcome_cancel_item_id.clone(), field)?,
        },
        Some("tooLate") => RecipientOutcome::TooLate {
            cancel: stored_enum(row.outcome_cancel_item_id.clone(), field)?,
            at: stored_enum(row.outcome_too_late_at.clone(), field)?,
        },
        Some("refused") => RecipientOutcome::Refused {
            reason: RefusalFields {
                kind: row.refusal_kind.clone(),
                code: row.refusal_code.clone(),
                failure: row.refusal_failure.clone(),
                size_field: row.refusal_size_field.clone(),
                digest: row.refusal_digest.clone(),
                item_id: row.refusal_item_id.clone(),
            }
            .decode()?,
        },
        _ => return Err(DeliveryError::Corrupt { field }),
    };
    Ok(RecipientReport {
        item: stored_enum(row.item_id.clone(), "report item id")?,
        report_seq: ReportSeq::new(integer(
            row.report_seq.clone(),
            row.report_seq_class.clone(),
            "report sequence",
        )?),
        outcome,
    })
}
