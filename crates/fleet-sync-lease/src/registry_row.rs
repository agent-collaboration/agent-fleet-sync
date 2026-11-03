//! Decode typed registry columns before they enter lease rules.
use crate::{checked_queries::*, registry_records::*, *};
use fleet_sync_protocol::*;
use serde::de::DeserializeOwned;

pub(crate) fn corrupt_column(field: &str) -> LeaseError {
    LeaseError::Corrupt {
        field: field.into(),
    }
}
pub(crate) fn required_text(value: Option<String>, field: &str) -> Result<String, LeaseError> {
    value.ok_or_else(|| corrupt_column(field))
}
pub(crate) fn stored_scalar<T: DeserializeOwned>(
    value: Option<String>,
    field: &str,
) -> Result<T, LeaseError> {
    let text = required_text(value, field)?;
    let encoded = serde_json::to_string(&text).map_err(|_| corrupt_column(field))?;
    serde_json::from_str(&encoded).map_err(|_| corrupt_column(field))
}
pub(crate) fn stored_counter(value: Option<String>, field: &str) -> Result<u64, LeaseError> {
    let text = required_text(value, field)?;
    let number = text.parse::<u64>().map_err(|_| corrupt_column(field))?;
    if number.to_string() != text {
        return Err(corrupt_column(field));
    }
    Ok(number)
}
pub(crate) fn stored_epoch(value: Option<String>, field: &str) -> Result<Epoch, LeaseError> {
    Epoch::new(stored_counter(value, field)?).map_err(|_| corrupt_column(field))
}
pub(crate) fn stored_time(value: Option<String>, field: &str) -> Result<SyncTime, LeaseError> {
    let text = required_text(value, field)?;
    let milliseconds = text.parse::<i64>().map_err(|_| corrupt_column(field))?;
    if milliseconds.to_string() != text {
        return Err(corrupt_column(field));
    }
    Ok(SyncTime::from_unix_millis(milliseconds))
}
pub(crate) fn optional_position(
    epoch: Option<String>,
    sequence: Option<String>,
    field: &str,
) -> Result<Option<ReplicationPosition>, LeaseError> {
    match (epoch, sequence) {
        (None, None) => Ok(None),
        (Some(epoch), Some(sequence)) => Ok(Some(ReplicationPosition::new(
            stored_epoch(Some(epoch), field)?,
            CommitSequence::new(stored_counter(Some(sequence), field)?),
        ))),
        _ => Err(corrupt_column(field)),
    }
}
pub(crate) fn stored_origin(
    kind: Option<String>,
    epoch: Option<String>,
    sequence: Option<String>,
) -> Result<Origin, LeaseError> {
    match (kind.as_deref(), epoch, sequence) {
        (Some("empty"), None, None) => Ok(Origin::Empty),
        (Some("seeded"), epoch, sequence) => Ok(Origin::Seeded {
            position: optional_position(epoch, sequence, "origin position")?
                .ok_or_else(|| corrupt_column("origin position"))?,
        }),
        _ => Err(corrupt_column("database origin")),
    }
}
impl StoredDatabaseRow {
    pub(crate) fn database_record(&self) -> Result<DatabaseRecord, LeaseError> {
        let lifecycle = match self.lifecycle_kind.as_deref() {
            Some("registered") if self.retiring_attempt.is_none() && self.retired_at.is_none() => {
                DatabaseLifecycle::Registered {
                    origin: stored_origin(
                        self.origin_kind.clone(),
                        self.origin_epoch.clone(),
                        self.origin_sequence.clone(),
                    )?,
                    imported_from: self
                        .import_digest
                        .clone()
                        .map(|digest| {
                            stored_scalar(Some(digest), "import digest")
                                .map(|digest| ImportOrigin { digest })
                        })
                        .transpose()?,
                }
            }
            Some("unheld")
                if self.origin_kind.is_none()
                    && self.origin_epoch.is_none()
                    && self.origin_sequence.is_none()
                    && self.retiring_attempt.is_none()
                    && self.retired_at.is_none() =>
            {
                DatabaseLifecycle::Unheld {
                    import_digest: stored_scalar(self.import_digest.clone(), "import digest")?,
                }
            }
            Some("retiring")
                if self.origin_kind.is_none()
                    && self.origin_epoch.is_none()
                    && self.origin_sequence.is_none()
                    && self.import_digest.is_none()
                    && self.retired_at.is_none() =>
            {
                DatabaseLifecycle::Retiring {
                    attempt: stored_scalar(self.retiring_attempt.clone(), "retiring attempt")?,
                }
            }
            Some("retired")
                if self.origin_kind.is_none()
                    && self.origin_epoch.is_none()
                    && self.origin_sequence.is_none()
                    && self.import_digest.is_none()
                    && self.retiring_attempt.is_none() =>
            {
                DatabaseLifecycle::Retired {
                    at: stored_time(self.retired_at.clone(), "retired time")?,
                }
            }
            _ => return Err(corrupt_column("database lifecycle")),
        };
        Ok(DatabaseRecord {
            database: stored_scalar(self.database.clone(), "database id")?,
            lifecycle,
            revision: RegistryRevision {
                epoch: stored_epoch(self.revision_epoch.clone(), "revision epoch")?,
                change: std::num::NonZeroU64::new(stored_counter(
                    self.revision_change.clone(),
                    "revision change",
                )?)
                .ok_or_else(|| corrupt_column("revision change"))?,
            },
            changed_at: stored_time(self.changed_at.clone(), "database changed time")?,
        })
    }
}

impl StoredLeaseRow {
    pub(crate) fn lease_record(&self) -> Result<RegistryLease, LeaseError> {
        Ok(RegistryLease {
            database: stored_scalar(self.database.clone(), "lease database")?,
            holder: stored_scalar(self.holder.clone(), "lease holder")?,
            epoch: stored_epoch(self.epoch.clone(), "lease epoch")?,
            expires_at: stored_time(self.expires_at.clone(), "lease expiry")?,
            last_renewed_at: stored_time(self.last_renewed_at.clone(), "lease renewal time")?,
        })
    }
}
impl StoredHistoryRow {
    pub(crate) fn history_record(&self) -> Result<EpochHistory, LeaseError> {
        Ok(EpochHistory {
            database: stored_scalar(self.database.clone(), "history database")?,
            epoch: stored_epoch(self.epoch.clone(), "history epoch")?,
            holder: stored_scalar(self.holder.clone(), "history holder")?,
            granted_at: stored_time(self.granted_at.clone(), "history grant time")?,
            ended_at: optional_position(
                self.ended_epoch.clone(),
                self.ended_sequence.clone(),
                "history end",
            )?,
        })
    }
}

impl StoredPreviewRow {
    pub(crate) fn preview_record(&self) -> Result<PreviewRecord, LeaseError> {
        Ok(PreviewRecord {
            database: stored_scalar(self.database.clone(), "preview database")?,
            preview: TakeoverPreview {
                token: stored_scalar(self.token.clone(), "preview token")?,
                fence: stored_epoch(self.fence.clone(), "preview fence")?,
                acked: optional_position(
                    self.acked_epoch.clone(),
                    self.acked_sequence.clone(),
                    "preview position",
                )?
                .ok_or_else(|| corrupt_column("preview position"))?,
                holder: stored_scalar(self.holder.clone(), "preview holder")?,
                last_seen: self
                    .last_seen
                    .clone()
                    .map(|time| stored_time(Some(time), "preview last seen"))
                    .transpose()?,
            },
        })
    }
}
impl StoredTransferCancelRow {
    pub(crate) fn cancel_record(&self, id: TransferId) -> Result<CancelRecord, LeaseError> {
        if self.attempt.as_deref() != Some(id.to_string().as_str()) {
            return Err(corrupt_column("cancel attempt"));
        }
        Ok(CancelRecord {
            by: stored_scalar(self.cancelled_by.clone(), "cancel actor")?,
            reason: stored_scalar(self.reason.clone(), "cancel reason")?,
            at: stored_time(self.cancelled_at.clone(), "cancel time")?,
        })
    }
}
impl StoredSchemaSupportRow {
    pub(crate) fn schema_report(&self) -> Result<ReportSchemaSupport, LeaseError> {
        Ok(ReportSchemaSupport {
            peer: stored_scalar(self.peer.clone(), "support peer")?,
            session_id: stored_scalar(self.session_id.clone(), "support session")?,
            attach_seq: AttachSequence::new(stored_counter(
                self.attach_seq.clone(),
                "support attach sequence",
            )?)
            .map_err(|_| corrupt_column("support attach sequence"))?,
            support: SchemaSupport {
                max_version: stored_counter(self.max_version.clone(), "supported version")?,
            },
        })
    }
}

pub(crate) fn stored_boolean(value: Option<String>, field: &str) -> Result<bool, LeaseError> {
    match value.as_deref() {
        Some("0") => Ok(false),
        Some("1") => Ok(true),
        _ => Err(corrupt_column(field)),
    }
}
pub(crate) fn validate_variant_columns(
    columns: &[(&str, &Option<String>)],
    allowed: &[&str],
    field: &str,
) -> Result<(), LeaseError> {
    if columns
        .iter()
        .any(|(name, value)| value.is_some() && !allowed.contains(name))
    {
        return Err(corrupt_column(field));
    }
    Ok(())
}
