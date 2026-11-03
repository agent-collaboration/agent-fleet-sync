use crate::{checked_queries::*, registry_records::CleanupFailure, registry_row::*, *};
use fleet_sync_protocol::*;
pub(crate) struct TransferFacts {
    pub attempt: TransferId,
    pub database: DatabaseId,
    pub new_epoch: Option<Epoch>,
    pub outcome: Option<HolderChangeAnswer>,
    pub phase: HolderChangePhase,
    pub cleanup_failed: Option<CleanupFailure>,
}
fn stored_phase(
    kind: Option<String>,
    epoch: Option<String>,
    sequence: Option<String>,
) -> Result<HolderChangePhase, LeaseError> {
    let position = optional_position(epoch, sequence, "released phase position")?;
    match (kind.as_deref(), position) {
        (Some("requested"), None) => Ok(HolderChangePhase::Requested),
        (Some("releasing"), None) => Ok(HolderChangePhase::Releasing),
        (Some("released"), Some(final_position)) => {
            Ok(HolderChangePhase::Released { final_position })
        }
        (Some("reserved"), None) => Ok(HolderChangePhase::Reserved),
        (Some("fencing"), None) => Ok(HolderChangePhase::Fencing),
        (Some("cancelling"), None) => Ok(HolderChangePhase::Cancelling),
        (Some("ended"), None) => Ok(HolderChangePhase::Ended),
        _ => Err(corrupt_column("holder-change phase")),
    }
}
impl StoredTransferRow {
    pub(crate) fn transfer_facts(&self) -> Result<TransferFacts, LeaseError> {
        let payload = [
            ("cancelled_by", &self.cancelled_by),
            ("granted_epoch", &self.granted_epoch),
            ("granted_start_epoch", &self.granted_start_epoch),
            ("granted_start_sequence", &self.granted_start_sequence),
            ("granted_expires_at", &self.granted_expires_at),
            ("pending_transfer", &self.pending_transfer),
            ("failure_reason", &self.failure_reason),
            ("refusal_kind", &self.refusal_kind),
            ("refusal_last_renewed_at", &self.refusal_last_renewed_at),
            ("refusal_last_seen", &self.refusal_last_seen),
            ("refusal_gate_code", &self.refusal_gate_code),
            ("refusal_gate_detail", &self.refusal_gate_detail),
            ("refusal_transfer", &self.refusal_transfer),
            ("refusal_migration_state", &self.refusal_migration_state),
            ("refusal_store_fingerprint", &self.refusal_store_fingerprint),
            ("refusal_holder", &self.refusal_holder),
            ("refusal_superseded_by", &self.refusal_superseded_by),
            ("refusal_migration", &self.refusal_migration),
            ("refusal_store_version", &self.refusal_store_version),
            ("refusal_supported_version", &self.refusal_supported_version),
        ];
        let (outcome, allowed) = match self.outcome_kind.as_deref() {
            None => (None, &[][..]),
            Some("cancelled") => (
                Some(HolderChangeAnswer::Cancelled {
                    by: stored_scalar(self.cancelled_by.clone(), "cancelled_by")?,
                }),
                &["cancelled_by"][..],
            ),
            Some("granted") => (
                Some(HolderChangeAnswer::Granted {
                    epoch: stored_epoch(self.granted_epoch.clone(), "granted_epoch")?,
                    expires_at: stored_time(self.granted_expires_at.clone(), "granted_expires_at")?,
                    start: optional_position(
                        self.granted_start_epoch.clone(),
                        self.granted_start_sequence.clone(),
                        "granted_start",
                    )?
                    .ok_or_else(|| corrupt_column("granted_start"))?,
                }),
                &[
                    "granted_epoch",
                    "granted_expires_at",
                    "granted_start_epoch",
                    "granted_start_sequence",
                ][..],
            ),
            Some("pending") => (
                Some(HolderChangeAnswer::Pending {
                    transfer: stored_scalar(self.pending_transfer.clone(), "pending_transfer")?,
                }),
                &["pending_transfer"][..],
            ),
            Some("failed") => (
                Some(HolderChangeAnswer::Failed {
                    reason: stored_scalar(self.failure_reason.clone(), "failure_reason")?,
                }),
                &["failure_reason"][..],
            ),
            Some("refused") => {
                let (reason, allowed) = match self.refusal_kind.as_deref() {
                    Some("holderStale") => (
                        HolderChangeRefusal::HolderStale {
                            last_renewed_at: stored_time(
                                self.refusal_last_renewed_at.clone(),
                                "refusal_last_renewed_at",
                            )?,
                        },
                        &["refusal_kind", "refusal_last_renewed_at"][..],
                    ),
                    Some("gateRefused") => (
                        HolderChangeRefusal::GateRefused {
                            reason: GateReason {
                                code: stored_scalar(
                                    self.refusal_gate_code.clone(),
                                    "refusal_gate_code",
                                )?,
                                detail: required_text(
                                    self.refusal_gate_detail.clone(),
                                    "gate detail",
                                )?,
                            },
                        },
                        &["refusal_kind", "refusal_gate_code", "refusal_gate_detail"][..],
                    ),
                    Some("transferInProgress") => (
                        HolderChangeRefusal::TransferInProgress {
                            transfer: stored_scalar(
                                self.refusal_transfer.clone(),
                                "refusal_transfer",
                            )?,
                        },
                        &["refusal_kind", "refusal_transfer"][..],
                    ),
                    Some("previewStale") => {
                        (HolderChangeRefusal::PreviewStale, &["refusal_kind"][..])
                    }
                    Some("migrationPending") => (
                        HolderChangeRefusal::MigrationPending {
                            migration: stored_counter(
                                self.refusal_migration.clone(),
                                "refusal_migration",
                            )?,
                            state: match self.refusal_migration_state.as_deref() {
                                Some("pending") => MigrationState::Pending,
                                Some("failed") => MigrationState::Failed,
                                _ => return Err(corrupt_column("migration state")),
                            },
                        },
                        &[
                            "refusal_kind",
                            "refusal_migration",
                            "refusal_migration_state",
                        ][..],
                    ),
                    Some("schemaTooNew") => (
                        HolderChangeRefusal::SchemaTooNew {
                            store: SchemaIdentity::new(
                                stored_counter(
                                    self.refusal_store_version.clone(),
                                    "refusal_store_version",
                                )?,
                                required_text(
                                    self.refusal_store_fingerprint.clone(),
                                    "schema fingerprint",
                                )?,
                            )
                            .map_err(|_| corrupt_column("schema identity"))?,
                            supported: self
                                .refusal_supported_version
                                .clone()
                                .map(|value| stored_counter(Some(value), "supported version"))
                                .transpose()?,
                        },
                        &[
                            "refusal_kind",
                            "refusal_store_version",
                            "refusal_store_fingerprint",
                            "refusal_supported_version",
                        ][..],
                    ),
                    Some("alreadyHeld") => (
                        HolderChangeRefusal::AlreadyHeld {
                            holder: stored_scalar(self.refusal_holder.clone(), "refusal_holder")?,
                        },
                        &["refusal_kind", "refusal_holder"][..],
                    ),
                    Some("notFound") => (HolderChangeRefusal::NotFound, &["refusal_kind"][..]),
                    Some("unavailable") => {
                        (HolderChangeRefusal::Unavailable, &["refusal_kind"][..])
                    }
                    Some("epochUnprovable") => {
                        (HolderChangeRefusal::EpochUnprovable, &["refusal_kind"][..])
                    }
                    Some("superseded") => (
                        HolderChangeRefusal::Superseded {
                            by: stored_scalar(
                                self.refusal_superseded_by.clone(),
                                "refusal_superseded_by",
                            )?,
                        },
                        &["refusal_kind", "refusal_superseded_by"][..],
                    ),
                    Some("holderUnreachable") => (
                        HolderChangeRefusal::HolderUnreachable {
                            last_seen: self
                                .refusal_last_seen
                                .clone()
                                .map(|value| stored_time(Some(value), "refusal_last_seen"))
                                .transpose()?,
                        },
                        &["refusal_kind", "refusal_last_seen"][..],
                    ),
                    Some("retired") => (HolderChangeRefusal::Retired, &["refusal_kind"][..]),
                    _ => return Err(corrupt_column("holder-change refusal")),
                };
                (Some(HolderChangeAnswer::Refused(reason)), allowed)
            }
            _ => return Err(corrupt_column("holder-change outcome")),
        };
        validate_variant_columns(&payload, allowed, "holder-change outcome columns")?;
        let cleanup_failed = match (
            self.cleanup_failure_reason.clone(),
            self.cleanup_failed_at.clone(),
        ) {
            (None, None) => None,
            (Some(reason), Some(at)) => Some(CleanupFailure {
                reason: stored_scalar(Some(reason), "cleanup failure reason")?,
                at: stored_time(Some(at), "cleanup failure time")?,
            }),
            _ => return Err(corrupt_column("cleanup failure")),
        };
        Ok(TransferFacts {
            attempt: stored_scalar(self.attempt.clone(), "transfer attempt")?,
            database: stored_scalar(self.database.clone(), "transfer database")?,
            new_epoch: self
                .new_epoch
                .clone()
                .map(|value| stored_epoch(Some(value), "reserved epoch"))
                .transpose()?,
            outcome,
            phase: stored_phase(
                self.phase_kind.clone(),
                self.phase_final_epoch.clone(),
                self.phase_final_sequence.clone(),
            )?,
            cleanup_failed,
        })
    }
}

impl StoredPhaseHistoryRow {
    pub(crate) fn phase_entry(&self) -> Result<(u64, PhaseEntry), LeaseError> {
        let position = stored_counter(self.position.clone(), "phase history position")?;
        if position >= 8 {
            return Err(corrupt_column("holder-change phase history"));
        }
        Ok((
            position,
            PhaseEntry {
                phase: stored_phase(
                    self.phase_kind.clone(),
                    self.final_epoch.clone(),
                    self.final_sequence.clone(),
                )?,
                entered_at: stored_time(self.entered_at.clone(), "phase entry time")?,
            },
        ))
    }
}
