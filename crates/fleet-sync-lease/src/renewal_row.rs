use crate::{
    LeaseError, checked_queries::StoredRenewalRow, registry_records::RenewalAttempt,
    registry_row::*,
};
use fleet_sync_protocol::*;
impl StoredRenewalRow {
    pub(crate) fn renewal_record(&self) -> Result<RenewalAttempt, LeaseError> {
        let key = format!(
            "{}/{}",
            self.database.as_deref().unwrap_or("<missing>"),
            self.attempt.as_deref().unwrap_or("<missing>")
        );
        stored_row_id(&self.id, "_fleet_sync_renewal_attempts", &key)?;
        let payload = [
            ("recorded_epoch", &self.recorded_epoch),
            ("recorded_expires_at", &self.recorded_expires_at),
            ("current_database", &self.current_database),
            ("current_holder", &self.current_holder),
            ("current_state", &self.current_state),
            ("current_epoch", &self.current_epoch),
            ("current_remaining_ms", &self.current_remaining_ms),
            ("current_expires_at", &self.current_expires_at),
            ("transfer_in_progress", &self.transfer_in_progress),
            ("replayed", &self.replayed),
            ("refusal_kind", &self.refusal_kind),
            ("refusal_current_epoch", &self.refusal_current_epoch),
            ("refusal_ended_epoch", &self.refusal_ended_epoch),
            ("refusal_ended_sequence", &self.refusal_ended_sequence),
        ];
        let answer = match self.answer_kind.as_deref() {
            Some("renewed") => {
                validate_variant_columns(
                    &payload,
                    &[
                        "recorded_epoch",
                        "recorded_expires_at",
                        "current_database",
                        "current_epoch",
                        "current_holder",
                        "current_state",
                        "current_expires_at",
                        "current_remaining_ms",
                        "transfer_in_progress",
                        "replayed",
                    ],
                    "renewed columns",
                )?;
                RenewAnswer::Renewed {
                    recorded: RenewResult {
                        epoch: stored_epoch(self.recorded_epoch.clone(), "recorded renewal epoch")?,
                        expires_at: stored_time(
                            self.recorded_expires_at.clone(),
                            "recorded renewal expiry",
                        )?,
                    },
                    current: LeaseStatus {
                        database: stored_scalar(
                            self.current_database.clone(),
                            "renewal current database",
                        )?,
                        epoch: stored_epoch(self.current_epoch.clone(), "renewal current epoch")?,
                        holder: stored_scalar(
                            self.current_holder.clone(),
                            "renewal current holder",
                        )?,
                        state: match self.current_state.as_deref() {
                            Some("held") => LeaseState::Held,
                            Some("expired") => LeaseState::Expired,
                            _ => return Err(corrupt_column("renewal lease state")),
                        },
                        expires_at: stored_time(
                            self.current_expires_at.clone(),
                            "renewal current expiry",
                        )?,
                        remaining_ms: stored_counter(
                            self.current_remaining_ms.clone(),
                            "renewal remaining milliseconds",
                        )?,
                    },
                    transfer_in_progress: self
                        .transfer_in_progress
                        .clone()
                        .map(|value| stored_scalar(Some(value), "renewal transfer"))
                        .transpose()?,
                    replayed: stored_boolean(self.replayed.clone(), "renewal replayed")?,
                }
            }
            Some("refused") => {
                let (reason, allowed) = match self.refusal_kind.as_deref() {
                    Some("staleEpoch") => (
                        LeaseRefusal::StaleEpoch {
                            current_epoch: stored_epoch(
                                self.refusal_current_epoch.clone(),
                                "refusal current epoch",
                            )?,
                            epoch_ended_at: optional_position(
                                self.refusal_ended_epoch.clone(),
                                self.refusal_ended_sequence.clone(),
                                "refusal epoch end",
                            )?
                            .ok_or_else(|| corrupt_column("refusal epoch end"))?,
                        },
                        &[
                            "refusal_kind",
                            "refusal_current_epoch",
                            "refusal_ended_epoch",
                            "refusal_ended_sequence",
                        ][..],
                    ),
                    Some("notWriter") => (
                        LeaseRefusal::NotWriter {
                            current: self
                                .refusal_current_epoch
                                .clone()
                                .map(|v| stored_epoch(Some(v), "refusal current epoch"))
                                .transpose()?,
                        },
                        &["refusal_kind", "refusal_current_epoch"][..],
                    ),
                    Some("noAuthoritativeCopy") => {
                        (LeaseRefusal::NoAuthoritativeCopy, &["refusal_kind"][..])
                    }
                    Some("epochUnprovable") => {
                        (LeaseRefusal::EpochUnprovable, &["refusal_kind"][..])
                    }
                    Some("notRegistered") => (LeaseRefusal::NotRegistered, &["refusal_kind"][..]),
                    Some("retired") => (LeaseRefusal::Retired, &["refusal_kind"][..]),
                    _ => return Err(corrupt_column("renewal refusal")),
                };
                validate_variant_columns(&payload, allowed, "renewal refusal columns")?;
                RenewAnswer::Refused(reason)
            }
            _ => return Err(corrupt_column("renewal answer")),
        };
        Ok(RenewalAttempt {
            request: RenewRequest {
                database: stored_scalar(self.database.clone(), "renewal database")?,
                attempt: stored_scalar(self.attempt.clone(), "renewal attempt")?,
                epoch: stored_epoch(self.request_epoch.clone(), "renewal requested epoch")?,
            },
            holder: stored_scalar(self.holder.clone(), "renewal holder")?,
            answer,
        })
    }
}
