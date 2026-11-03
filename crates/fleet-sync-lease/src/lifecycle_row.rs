use crate::{
    LeaseError, checked_queries::StoredLifecycleRow, registry_records::*, registry_row::*,
};
use fleet_sync_protocol::*;
impl StoredLifecycleRow {
    pub(crate) fn lifecycle_record(&self) -> Result<LifecycleAttempt, LeaseError> {
        let request_payload = [
            ("writer", &self.writer),
            ("origin_kind", &self.origin_kind),
            ("import_digest", &self.import_digest),
            ("origin_epoch", &self.origin_epoch),
            ("origin_sequence", &self.origin_sequence),
            ("expect_fence", &self.expect_fence),
            ("expect_acked_epoch", &self.expect_acked_epoch),
            ("expect_acked_sequence", &self.expect_acked_sequence),
        ];
        let attempt = stored_scalar(self.attempt.clone(), "lifecycle attempt")?;
        let database = stored_scalar(self.database.clone(), "lifecycle database")?;
        let intent = match self.intent_kind.as_deref() {
            Some("register") => {
                validate_variant_columns(
                    &request_payload,
                    &["writer", "origin_kind", "origin_epoch", "origin_sequence"],
                    "registration request columns",
                )?;
                LifecycleIntent::Register(Registration {
                    attempt,
                    database,
                    writer: stored_scalar(self.writer.clone(), "writer")?,
                    origin: stored_origin(
                        self.origin_kind.clone(),
                        self.origin_epoch.clone(),
                        self.origin_sequence.clone(),
                    )?,
                })
            }
            Some("import") => {
                validate_variant_columns(
                    &request_payload,
                    &["import_digest"],
                    "import request columns",
                )?;
                LifecycleIntent::Import(Import {
                    attempt,
                    database,
                    digest: stored_scalar(self.import_digest.clone(), "import_digest")?,
                })
            }
            Some("retire") => {
                validate_variant_columns(
                    &request_payload,
                    &[
                        "expect_fence",
                        "expect_acked_epoch",
                        "expect_acked_sequence",
                    ],
                    "retirement request columns",
                )?;
                LifecycleIntent::Retire(Retirement {
                    attempt,
                    database,
                    expect_fence: stored_epoch(self.expect_fence.clone(), "expect_fence")?,
                    expect_acked: optional_position(
                        self.expect_acked_epoch.clone(),
                        self.expect_acked_sequence.clone(),
                        "expect_acked",
                    )?
                    .ok_or_else(|| corrupt_column("expect_acked"))?,
                })
            }
            _ => return Err(corrupt_column("lifecycle intent")),
        };
        let payload = [
            ("answer_kind", &self.answer_kind),
            ("failure_reason", &self.failure_reason),
            ("lease_database", &self.lease_database),
            ("lease_holder", &self.lease_holder),
            ("lease_state", &self.lease_state),
            ("lease_epoch", &self.lease_epoch),
            ("lease_remaining_ms", &self.lease_remaining_ms),
            ("lease_expires_at", &self.lease_expires_at),
            ("replayed", &self.replayed),
            ("refusal_kind", &self.refusal_kind),
            ("gate_code", &self.gate_code),
            ("gate_detail", &self.gate_detail),
            ("existing_digest", &self.existing_digest),
            ("offered_digest", &self.offered_digest),
            ("current_epoch", &self.current_epoch),
            ("retired_at", &self.retired_at),
            ("mismatch_fence", &self.mismatch_fence),
            ("mismatch_acked_epoch", &self.mismatch_acked_epoch),
            ("mismatch_acked_sequence", &self.mismatch_acked_sequence),
        ];
        let (outcome, allowed) = match self.outcome_kind.as_deref() {
            None => (None, &[][..]),
            Some("failed") => (
                Some(LifecycleOutcome::Failed {
                    reason: stored_scalar(self.failure_reason.clone(), "failure_reason")?,
                }),
                &["failure_reason"][..],
            ),
            Some("registration") => {
                let (answer, allowed) = match self.answer_kind.as_deref() {
                    Some("registered") => (
                        RegistrationAnswer::Registered {
                            lease: LeaseStatus {
                                database: stored_scalar(
                                    self.lease_database.clone(),
                                    "lease_database",
                                )?,
                                epoch: stored_epoch(self.lease_epoch.clone(), "lease_epoch")?,
                                holder: stored_scalar(self.lease_holder.clone(), "lease_holder")?,
                                state: match self.lease_state.as_deref() {
                                    Some("held") => LeaseState::Held,
                                    Some("expired") => LeaseState::Expired,
                                    _ => return Err(corrupt_column("lease state")),
                                },
                                expires_at: stored_time(
                                    self.lease_expires_at.clone(),
                                    "lease_expires_at",
                                )?,
                                remaining_ms: stored_counter(
                                    self.lease_remaining_ms.clone(),
                                    "lease_remaining_ms",
                                )?,
                            },
                            replayed: stored_boolean(
                                self.replayed.clone(),
                                "registration replayed",
                            )?,
                        },
                        &[
                            "answer_kind",
                            "lease_database",
                            "lease_epoch",
                            "lease_holder",
                            "lease_state",
                            "lease_expires_at",
                            "lease_remaining_ms",
                            "replayed",
                        ][..],
                    ),
                    Some("refused") => {
                        let (refusal, allowed) = match self.refusal_kind.as_deref() {
                            Some("databaseRejected") => (
                                RegistrationRefusal::DatabaseRejected {
                                    reason: GateReason {
                                        code: stored_scalar(self.gate_code.clone(), "gate_code")?,
                                        detail: required_text(
                                            self.gate_detail.clone(),
                                            "gate detail",
                                        )?,
                                    },
                                },
                                &["answer_kind", "refusal_kind", "gate_code", "gate_detail"][..],
                            ),
                            Some("notWriter") => (
                                RegistrationRefusal::NotWriter {
                                    current: self
                                        .current_epoch
                                        .clone()
                                        .map(|value| stored_epoch(Some(value), "current epoch"))
                                        .transpose()?,
                                },
                                &["answer_kind", "refusal_kind", "current_epoch"][..],
                            ),
                            Some("retired") => (
                                RegistrationRefusal::Retired,
                                &["answer_kind", "refusal_kind"][..],
                            ),
                            _ => return Err(corrupt_column("registration refusal")),
                        };
                        (RegistrationAnswer::Refused(refusal), allowed)
                    }
                    _ => return Err(corrupt_column("registration answer")),
                };
                (Some(LifecycleOutcome::Registration(answer)), allowed)
            }
            Some("import") => {
                let (answer, allowed) = match self.answer_kind.as_deref() {
                    Some("imported") => (ImportAnswer::Imported, &["answer_kind"][..]),
                    Some("alreadyImported") => {
                        (ImportAnswer::AlreadyImported, &["answer_kind"][..])
                    }
                    Some("importConflict") => (
                        ImportAnswer::ImportConflict {
                            existing: self
                                .existing_digest
                                .clone()
                                .map(|value| stored_scalar(Some(value), "existing import digest"))
                                .transpose()?,
                            offered: stored_scalar(self.offered_digest.clone(), "offered_digest")?,
                        },
                        &["answer_kind", "existing_digest", "offered_digest"][..],
                    ),
                    Some("retired") => (ImportAnswer::Retired, &["answer_kind"][..]),
                    _ => return Err(corrupt_column("import answer")),
                };
                (Some(LifecycleOutcome::Import(answer)), allowed)
            }
            Some("retirement") => {
                let (answer, allowed) = match self.answer_kind.as_deref() {
                    Some("retired") => (
                        RetirementAnswer::Retired {
                            at: stored_time(self.retired_at.clone(), "retired_at")?,
                        },
                        &["answer_kind", "retired_at"][..],
                    ),
                    Some("refused") => {
                        let (refusal, allowed) = match self.refusal_kind.as_deref() {
                            Some("copyNotInitialized") => (
                                RetirementRefusal::CopyNotInitialized,
                                &["answer_kind", "refusal_kind"][..],
                            ),
                            Some("leaseHeld") => (
                                RetirementRefusal::LeaseHeld,
                                &["answer_kind", "refusal_kind"][..],
                            ),
                            Some("transferInProgress") => (
                                RetirementRefusal::TransferInProgress,
                                &["answer_kind", "refusal_kind"][..],
                            ),
                            Some("gateRefused") => (
                                RetirementRefusal::GateRefused {
                                    reason: GateReason {
                                        code: stored_scalar(self.gate_code.clone(), "gate_code")?,
                                        detail: required_text(
                                            self.gate_detail.clone(),
                                            "gate detail",
                                        )?,
                                    },
                                },
                                &["answer_kind", "refusal_kind", "gate_code", "gate_detail"][..],
                            ),
                            Some("copyMismatch") => (
                                RetirementRefusal::CopyMismatch {
                                    fence: stored_epoch(
                                        self.mismatch_fence.clone(),
                                        "mismatch_fence",
                                    )?,
                                    acked: optional_position(
                                        self.mismatch_acked_epoch.clone(),
                                        self.mismatch_acked_sequence.clone(),
                                        "mismatch_acked",
                                    )?
                                    .ok_or_else(|| corrupt_column("mismatch_acked"))?,
                                },
                                &[
                                    "answer_kind",
                                    "refusal_kind",
                                    "mismatch_fence",
                                    "mismatch_acked_epoch",
                                    "mismatch_acked_sequence",
                                ][..],
                            ),
                            _ => return Err(corrupt_column("retirement refusal")),
                        };
                        (RetirementAnswer::Refused(refusal), allowed)
                    }
                    _ => return Err(corrupt_column("retirement answer")),
                };
                (Some(LifecycleOutcome::Retirement(answer)), allowed)
            }
            _ => return Err(corrupt_column("lifecycle outcome")),
        };
        validate_variant_columns(&payload, allowed, "lifecycle outcome columns")?;
        if !matches!(
            (&intent, &outcome),
            (_, None | Some(LifecycleOutcome::Failed { .. }))
                | (
                    LifecycleIntent::Register(_),
                    Some(LifecycleOutcome::Registration(_))
                )
                | (
                    LifecycleIntent::Import(_),
                    Some(LifecycleOutcome::Import(_))
                )
                | (
                    LifecycleIntent::Retire(_),
                    Some(LifecycleOutcome::Retirement(_))
                )
        ) {
            return Err(corrupt_column("lifecycle outcome kind"));
        }
        Ok(LifecycleAttempt { intent, outcome })
    }
}
