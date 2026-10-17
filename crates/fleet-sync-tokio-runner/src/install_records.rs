use crate::{
    InstallProgress, InstallRecord, InstallRequest, InstallStatus, ObjectKey,
    checked_native_queries as queries, install_contract::corrupt,
};
use durable_steps::{OperationFailure, StoreError, Uuid, storage::TransactionScope};
use fleet_sync_receiver::SnapshotInstall;
use sqlx_turso::Turso;

pub(crate) struct InstallRecords<'a, S>(pub &'a mut S);
impl<S: TransactionScope<Database = Turso>> InstallRecords<'_, S> {
    pub async fn get(&mut self, id: Uuid) -> Result<InstallRecord, StoreError> {
        let row = queries::read_install(self.0, id.to_string())
            .await?
            .ok_or_else(|| corrupt("install record is missing"))?;
        let record = decode(row)?;
        if record.install_id != id {
            return Err(corrupt("install record key differs"));
        }
        Ok(record)
    }
    pub async fn current(
        &mut self,
        target: &ObjectKey,
    ) -> Result<Option<InstallRecord>, StoreError> {
        queries::read_current_install(self.0, target.as_str().into())
            .await?
            .map(decode)
            .transpose()?
            .map(|record| {
                if record.request.target != *target {
                    Err(corrupt("current install target differs"))
                } else {
                    Ok(record)
                }
            })
            .transpose()
    }
    pub async fn history(&mut self, target: &ObjectKey) -> Result<Vec<InstallRecord>, StoreError> {
        let mut records = queries::read_target_installs(self.0, target.as_str().into())
            .await?
            .into_iter()
            .map(decode)
            .collect::<Result<Vec<_>, _>>()?;
        if records
            .iter()
            .any(|record| record.request.target != *target)
        {
            return Err(corrupt("install history target differs"));
        }
        records.sort_by_key(|record| record.generation);
        if records
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left.generation == right.generation))
        {
            return Err(corrupt("duplicate install generation"));
        }
        Ok(records)
    }
    pub async fn save(&mut self, record: &InstallRecord) -> Result<(), StoreError> {
        record.validate()?;
        queries::write_install(
            self.0,
            record.install_id.to_string(),
            record.request.target.as_str().into(),
            serde_json::to_string(record).map_err(StoreError::backend)?,
        )
        .await?;
        Ok(())
    }
    pub async fn reserve(
        &mut self,
        request: &InstallRequest,
        id: Uuid,
    ) -> Result<InstallRecord, StoreError> {
        // Admission and a source-moved restart both use the target's same owner
        // transaction. Only this record holds the rising generation.
        if let Some(existing) = queries::read_install(self.0, id.to_string()).await? {
            let existing = decode(existing)?;
            if existing.request != *request {
                return Err(corrupt("install identity reused with different request"));
            }
            return Ok(existing);
        }
        let current = self.current(&request.target).await?;
        let generation = current
            .as_ref()
            .map_or(Some(1), |record| record.generation.checked_add(1))
            .ok_or_else(|| corrupt("install generation exhausted"))?;
        if let Some(mut old) = current
            && let InstallStatus::Running { progress } = old.status
        {
            old.status = InstallStatus::Superseded { by: id, progress };
            self.save(&old).await?;
        }
        let record = InstallRecord {
            request: request.clone(),
            install_id: id,
            generation,
            status: InstallStatus::Running {
                progress: InstallProgress::Reserved,
            },
        };
        self.save(&record).await?;
        queries::write_current_install(self.0, request.target.as_str().into(), id.to_string())
            .await?;
        Ok(record)
    }
    pub async fn pin(
        &mut self,
        id: Uuid,
        snapshot: SnapshotInstall,
    ) -> Result<InstallRecord, StoreError> {
        let mut record = self.get(id).await?;
        if let InstallStatus::Running { progress } = &record.status {
            match progress {
                InstallProgress::Reserved => {
                    record.status = InstallStatus::Running {
                        progress: InstallProgress::Pinned { snapshot },
                    }
                }
                _ if record.status.snapshot() == Some(&snapshot) => {}
                _ => return Err(corrupt("install was pinned to a different snapshot")),
            }
            self.save(&record).await?;
        }
        Ok(record)
    }
    pub async fn fail_attempt(
        &mut self,
        target: &ObjectKey,
        attempt: Uuid,
        failure: &OperationFailure,
    ) -> Result<(), StoreError> {
        let mut record = self
            .history(target)
            .await?
            .into_iter()
            .rev()
            .find(|record| record.request.attempt == attempt)
            .ok_or_else(|| corrupt("failed install attempt has no record"))?;
        if let InstallStatus::Running { progress } = record.status {
            record.status = InstallStatus::Failed {
                reason: failure.clone(),
                progress,
            };
            self.save(&record).await?;
        }
        Ok(())
    }
    pub async fn needs_snapshot(&mut self, target: &ObjectKey) -> Result<bool, StoreError> {
        for record in self.history(target).await?.into_iter().rev() {
            match record.status {
                InstallStatus::Failed { .. } => return Ok(true),
                InstallStatus::Ready { .. } => return Ok(false),
                _ => {}
            }
        }
        Ok(false)
    }
}
fn decode(row: queries::InstallRecordRow) -> Result<InstallRecord, StoreError> {
    if row.record_class.as_deref() != Some("text") {
        return Err(corrupt("install record is not TEXT"));
    }
    let record: InstallRecord = serde_json::from_str(
        row.record
            .as_deref()
            .ok_or_else(|| corrupt("null install record"))?,
    )
    .map_err(|error| corrupt(error.to_string()))?;
    record.validate()?;
    Ok(record)
}
