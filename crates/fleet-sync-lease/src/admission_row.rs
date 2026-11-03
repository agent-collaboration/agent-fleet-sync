use crate::{
    LeaseError, checked_queries::StoredTransferAdmissionRow, registry_records::TransferAdmission,
    registry_row::*,
};
use fleet_sync_protocol::*;
impl StoredTransferAdmissionRow {
    pub(crate) fn admission_record(&self) -> Result<TransferAdmission, LeaseError> {
        let payload = [
            ("from_epoch", &self.from_epoch),
            ("to_peer", &self.to_peer),
            ("claimant", &self.claimant),
            ("reason", &self.reason),
            ("preview_token", &self.preview_token),
            ("preview_holder", &self.preview_holder),
            ("preview_fence", &self.preview_fence),
            ("preview_acked_epoch", &self.preview_acked_epoch),
            ("preview_acked_sequence", &self.preview_acked_sequence),
            ("preview_last_seen", &self.preview_last_seen),
        ];
        let attempt = stored_scalar(self.attempt.clone(), "admission attempt")?;
        let database = stored_scalar(self.database.clone(), "admission database")?;
        let request = match self.request_kind.as_deref() {
            Some("move") => {
                validate_variant_columns(
                    &payload,
                    &["from_epoch", "to_peer", "reason"],
                    "move request columns",
                )?;
                HolderChangeRequest::Move {
                    attempt,
                    database,
                    from_epoch: stored_epoch(self.from_epoch.clone(), "move epoch")?,
                    to: stored_scalar(self.to_peer.clone(), "move target")?,
                    reason: stored_scalar(self.reason.clone(), "move reason")?,
                }
            }
            Some("takeover") => {
                validate_variant_columns(
                    &payload,
                    &[
                        "to_peer",
                        "reason",
                        "preview_token",
                        "preview_holder",
                        "preview_fence",
                        "preview_acked_epoch",
                        "preview_acked_sequence",
                        "preview_last_seen",
                    ],
                    "takeover request columns",
                )?;
                HolderChangeRequest::Takeover {
                    attempt,
                    database,
                    to: stored_scalar(self.to_peer.clone(), "takeover target")?,
                    reason: stored_scalar(self.reason.clone(), "takeover reason")?,
                    preview: TakeoverPreview {
                        token: stored_scalar(self.preview_token.clone(), "takeover token")?,
                        fence: stored_epoch(self.preview_fence.clone(), "takeover fence")?,
                        acked: optional_position(
                            self.preview_acked_epoch.clone(),
                            self.preview_acked_sequence.clone(),
                            "takeover position",
                        )?
                        .ok_or_else(|| corrupt_column("takeover position"))?,
                        holder: stored_scalar(self.preview_holder.clone(), "takeover holder")?,
                        last_seen: self
                            .preview_last_seen
                            .clone()
                            .map(|v| stored_time(Some(v), "takeover last seen"))
                            .transpose()?,
                    },
                }
            }
            Some("claim") => {
                validate_variant_columns(&payload, &["claimant"], "claim request columns")?;
                HolderChangeRequest::Claim {
                    attempt,
                    database,
                    claimant: stored_scalar(self.claimant.clone(), "claimant")?,
                }
            }
            _ => return Err(corrupt_column("holder-change request")),
        };
        Ok(TransferAdmission {
            operation: stored_scalar(self.operation.clone(), "admission operation")?,
            request,
            requested_by: stored_scalar(self.requested_by.clone(), "admission actor")?,
            admitted_at: stored_time(self.admitted_at.clone(), "admission time")?,
            target_schema: self
                .target_schema_max_version
                .clone()
                .map(|value| {
                    stored_counter(Some(value), "target schema version")
                        .map(|max_version| SchemaSupport { max_version })
                })
                .transpose()?,
            follow_up: self
                .follow_up
                .clone()
                .map(|value| stored_scalar(Some(value), "follow-up operation"))
                .transpose()?,
        })
    }
}
