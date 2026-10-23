//! Registry-owned transfer state; journal execution remains in durable-steps.
use crate::{
    checked_queries,
    registry_records::*,
    registry_storage::{RegistryStorage, decode, encode},
    *,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

impl<S> RegistryStorage<'_, S>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub(crate) async fn admission(
        &mut self,
        id: TransferId,
    ) -> Result<Option<TransferAdmission>, LeaseError> {
        let Some(row) =
            checked_queries::read_transfer_admission(self.0, id.as_uuid().to_string()).await?
        else {
            return Ok(None);
        };
        let record = TransferAdmission {
            operation: decode(row.operation, "holder-change operation")?,
            request: decode(row.request, "holder-change request")?,
            requested_by: decode(row.requested_by, "holder-change requester")?,
            admitted_at: decode(row.admitted_at, "holder-change admission time")?,
            target_schema: decode(row.target_schema, "holder-change target schema")?,
            follow_up: row
                .follow_up
                .map(|value| decode(Some(value), "holder-change follow-up"))
                .transpose()?,
        };
        if crate::change_checks::attempt(&record.request) != id
            || record.operation.as_uuid().is_nil()
            || record.follow_up.is_some_and(|operation| {
                operation.as_uuid().is_nil() || operation == record.operation
            })
        {
            return Err(LeaseError::Corrupt {
                field: "holder-change admission".into(),
            });
        }
        Ok(Some(record))
    }
    pub(crate) async fn save_admission(
        &mut self,
        record: &TransferAdmission,
    ) -> Result<(), LeaseError> {
        let id = crate::change_checks::attempt(&record.request);
        if let Some(existing) = self.admission(id).await?
            && (existing.operation != record.operation
                || existing.request != record.request
                || existing.requested_by != record.requested_by
                || existing.admitted_at != record.admitted_at
                || existing.target_schema != record.target_schema
                || existing
                    .follow_up
                    .is_some_and(|id| Some(id) != record.follow_up))
        {
            return Err(LeaseError::Corrupt {
                field: "immutable holder-change admission".into(),
            });
        }
        checked_queries::write_transfer_admission(
            self.0,
            id.as_uuid().to_string(),
            encode(&record.operation)?,
            encode(&record.request)?,
            encode(&record.requested_by)?,
            encode(&record.admitted_at)?,
            encode(&record.target_schema)?,
            record.follow_up.as_ref().map(encode).transpose()?,
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn cancel(
        &mut self,
        id: TransferId,
    ) -> Result<Option<CancelRecord>, LeaseError> {
        checked_queries::read_transfer_cancel(self.0, id.as_uuid().to_string())
            .await?
            .map(|row| decode(row.record, "holder-change cancel"))
            .transpose()
    }
    pub(crate) async fn save_cancel(
        &mut self,
        id: TransferId,
        record: &CancelRecord,
    ) -> Result<(), LeaseError> {
        checked_queries::write_transfer_cancel(self.0, id.as_uuid().to_string(), encode(record)?)
            .await?;
        Ok(())
    }
    async fn transfer_row(
        &mut self,
        id: TransferId,
        intent: Option<String>,
        outcome: Option<String>,
        phase: Option<String>,
        history: Option<String>,
        cleanup_failed: Option<String>,
    ) -> Result<TransferRecord, LeaseError> {
        let admission = self
            .admission(id)
            .await?
            .ok_or_else(|| LeaseError::Corrupt {
                field: "transfer admission missing".into(),
            })?;
        let record = TransferRecord {
            intent: TransferIntent {
                request: admission.request,
                new_epoch: decode(intent, "transfer reservation")?,
                target_schema: admission.target_schema,
            },
            outcome: outcome
                .map(|value| decode(Some(value), "transfer outcome"))
                .transpose()?,
            phase: decode(phase, "holder-change phase")?,
            phase_history: decode(history, "holder-change phase history")?,
            cleanup_failed: cleanup_failed
                .map(|value| decode(Some(value), "holder-change cleanup failure"))
                .transpose()?,
        };
        record.validate()?;
        Ok(record)
    }
    pub(crate) async fn transfer(
        &mut self,
        id: TransferId,
    ) -> Result<Option<TransferRecord>, LeaseError> {
        let Some(row) = checked_queries::read_transfer(self.0, id.as_uuid().to_string()).await?
        else {
            return Ok(None);
        };
        self.transfer_row(
            id,
            row.intent,
            row.outcome,
            row.phase,
            row.phase_history,
            row.cleanup_failed,
        )
        .await
        .map(Some)
    }
    pub(crate) async fn active_transfer(
        &mut self,
        database: DatabaseId,
    ) -> Result<Option<TransferRecord>, LeaseError> {
        let Some(row) =
            checked_queries::active_transfer(self.0, database.as_uuid().to_string()).await?
        else {
            return Ok(None);
        };
        let id = decode(
            row.attempt.map(|value| format!("\"{value}\"")),
            "active transfer attempt",
        )?;
        let record = self
            .transfer_row(
                id,
                row.intent,
                row.outcome,
                row.phase,
                row.phase_history,
                row.cleanup_failed,
            )
            .await?;
        if record.intent.database() != database || record.outcome.is_some() {
            return Err(LeaseError::Corrupt {
                field: "active transfer".into(),
            });
        }
        Ok(Some(record))
    }
    pub(crate) async fn save_transfer(
        &mut self,
        record: &TransferRecord,
    ) -> Result<(), LeaseError> {
        record.validate()?;
        let id = record.intent.attempt();
        let admission = self
            .admission(id)
            .await?
            .ok_or_else(|| LeaseError::Corrupt {
                field: "transfer admission missing".into(),
            })?;
        if admission.request != record.intent.request
            || admission.target_schema != record.intent.target_schema
        {
            return Err(LeaseError::Corrupt {
                field: "transfer request identity".into(),
            });
        }
        if let Some(existing) = self.transfer(id).await?
            && (existing.intent != record.intent
                || (existing.outcome.is_some() && existing.outcome != record.outcome)
                || !record.phase_history.starts_with(&existing.phase_history)
                || (existing.cleanup_failed.is_some()
                    && existing.cleanup_failed != record.cleanup_failed))
        {
            return Err(LeaseError::Corrupt {
                field: "immutable transfer state".into(),
            });
        }
        checked_queries::write_transfer(
            self.0,
            id.as_uuid().to_string(),
            record.intent.database().as_uuid().to_string(),
            encode(&record.intent.new_epoch)?,
            record.outcome.as_ref().map(encode).transpose()?,
            encode(&record.phase)?,
            encode(&record.phase_history)?,
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn repair_transfer_epoch(
        &mut self,
        id: TransferId,
        epoch: Epoch,
    ) -> Result<(), LeaseError> {
        let record = self
            .transfer(id)
            .await?
            .ok_or_else(|| LeaseError::Corrupt {
                field: "missing transfer reservation".into(),
            })?;
        if record.outcome.is_some()
            || record.intent.new_epoch.is_none_or(|old| epoch <= old)
            || record.phase != HolderChangePhase::Fencing
        {
            return Err(LeaseError::Corrupt {
                field: "transfer reservation must advance while fencing".into(),
            });
        }
        checked_queries::repair_transfer_epoch(
            self.0,
            encode(&Some(epoch))?,
            id.as_uuid().to_string(),
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn save_cleanup_failure(
        &mut self,
        id: TransferId,
        failure: &CleanupFailure,
    ) -> Result<(), LeaseError> {
        let mut record = self
            .transfer(id)
            .await?
            .ok_or_else(|| LeaseError::Corrupt {
                field: "cleanup transfer missing".into(),
            })?;
        if let Some(existing) = &record.cleanup_failed {
            return if existing == failure {
                Ok(())
            } else {
                Err(LeaseError::Corrupt {
                    field: "immutable cleanup failure".into(),
                })
            };
        }
        record.cleanup_failed = Some(failure.clone());
        record.validate()?;
        if record.outcome.is_some() {
            return Err(LeaseError::Corrupt {
                field: "cleanup failure after terminal outcome".into(),
            });
        }
        checked_queries::write_cleanup_failure(self.0, encode(failure)?, id.as_uuid().to_string())
            .await?;
        Ok(())
    }
}
