//! Journal semantics shared by the two SQL adapters; queries stay with each driver.

use crate::{
    Admitted, DriveLease, JournalScope, OperationId, RequestKey, StartRefusal, StoreError,
    SyncTime, Task,
    sql_rows::{StoredOperationRow, StoredStepRow, corrupt},
    storage::{JournalStorage, OperationExecution, OperationOutcome, OperationRecord, StepRecord},
};
use sha2::{Digest, Sha256};
use std::future::Future;
use uuid::Uuid;

impl From<sqlx_core::error::Error> for StoreError {
    fn from(error: sqlx_core::error::Error) -> Self {
        Self::backend(error)
    }
}

pub(crate) trait JournalQueries: Send {
    fn delete_steps(&mut self, id: String) -> impl Future<Output = Result<(), StoreError>> + Send;
    fn delete_operation(
        &mut self,
        id: String,
        token: String,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;
    fn now(&self) -> SyncTime;
    fn new_id(&self) -> Uuid;
    fn fetch_operation(
        &mut self,
        id: String,
    ) -> impl Future<Output = Result<Option<StoredOperationRow>, StoreError>> + Send;
    fn fetch_request(
        &mut self,
        kind: String,
        key: String,
    ) -> impl Future<Output = Result<Option<StoredOperationRow>, StoreError>> + Send;
    fn insert_operation(
        &mut self,
        row: StoredOperationRow,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
    fn update_operation(
        &mut self,
        row: StoredOperationRow,
        token: Option<String>,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;
    fn fetch_due(
        &mut self,
        now: i64,
    ) -> impl Future<Output = Result<Vec<String>, StoreError>> + Send;
    fn fetch_step(
        &mut self,
        id: String,
        position: i64,
    ) -> impl Future<Output = Result<Option<StoredStepRow>, StoreError>> + Send;
    fn upsert_step(
        &mut self,
        row: StoredStepRow,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
    fn count_steps(&mut self, id: String) -> impl Future<Output = Result<i64, StoreError>> + Send;
    fn earliest_deadline(&mut self)
    -> impl Future<Output = Result<Option<i64>, StoreError>> + Send;
    fn ensure_alarm(&mut self, at: SyncTime)
    -> impl Future<Output = Result<(), StoreError>> + Send;
}

impl<S: JournalQueries> JournalScope for S {
    async fn admit<T: Task>(
        &mut self,
        request_key: &RequestKey,
        input: &T::Input,
    ) -> Result<Admitted, StartRefusal> {
        let input = crate::canonical_payload::encode(input).map_err(StoreError::backend)?;
        let digest = Sha256::digest(&input).to_vec();
        if let Some(row) = self
            .fetch_request(T::KIND.into(), request_key.as_str().into())
            .await?
        {
            let existing = OperationRecord::try_from(row)?;
            return if existing.input_digest == digest {
                Ok(Admitted::Existing(existing.id))
            } else {
                Err(StartRefusal::RequestConflict {
                    existing: existing.id,
                })
            };
        }
        let id = OperationId::new(self.new_id()).map_err(StoreError::backend)?;
        let now = self.now();
        self.insert_operation(StoredOperationRow::encode(OperationRecord {
            id,
            kind: T::KIND.into(),
            version: T::VERSION,
            request_key: request_key.clone(),
            input_digest: digest,
            execution: OperationExecution::Running {
                input,
                next_attempt_at: now,
                attempt: 0,
                drive: None,
            },
        })?)
        .await?;
        if let Some(earliest) = self.earliest().await? {
            self.ensure_wake(earliest).await?;
        }
        Ok(Admitted::New(id))
    }
    async fn ensure_wake(&mut self, at: SyncTime) -> Result<(), StoreError> {
        self.ensure_alarm(at).await
    }
}

impl<S: JournalQueries> JournalStorage for S {
    async fn operation(&mut self, id: OperationId) -> Result<Option<OperationRecord>, StoreError> {
        self.fetch_operation(id.as_uuid().to_string())
            .await?
            .map(OperationRecord::try_from)
            .transpose()
    }
    async fn due(&mut self, now: SyncTime) -> Result<Vec<OperationId>, StoreError> {
        self.fetch_due(now.unix_millis())
            .await?
            .into_iter()
            .map(|id| OperationId::try_from(id).map_err(|error| corrupt("operation_id", error)))
            .collect()
    }
    async fn claim(
        &mut self,
        id: OperationId,
        now: SyncTime,
        lease: DriveLease,
    ) -> Result<Option<OperationRecord>, StoreError> {
        let Some(mut record) = self.operation(id).await? else {
            return Ok(None);
        };
        let previous_token = match &mut record.execution {
            OperationExecution::Running {
                next_attempt_at,
                drive,
                ..
            } if *next_attempt_at <= now
                && drive.as_ref().is_none_or(|current| current.until <= now) =>
            {
                let previous = drive.as_ref().map(|current| current.token.to_string());
                *drive = Some(lease);
                previous
            }
            _ => return Ok(None),
        };
        let updated = self
            .update_operation(StoredOperationRow::encode(record.clone())?, previous_token)
            .await?;
        Ok(updated.then_some(record))
    }
    async fn check_token(&mut self, id: OperationId, token: Uuid) -> Result<(), StoreError> {
        match self.operation(id).await?.map(|record| record.execution) {
            Some(OperationExecution::Running {
                drive: Some(lease), ..
            }) if lease.token == token => Ok(()),
            _ => Err(StoreError::StaleDriveToken { id }),
        }
    }
    async fn retry(
        &mut self,
        id: OperationId,
        token: Uuid,
        next: SyncTime,
        attempt: u32,
    ) -> Result<(), StoreError> {
        self.check_token(id, token).await?;
        let mut record = self
            .operation(id)
            .await?
            .ok_or(StoreError::MissingOperation { id })?;
        match &mut record.execution {
            OperationExecution::Running {
                next_attempt_at,
                attempt: current,
                drive,
                ..
            } => {
                *next_attempt_at = next;
                *current = attempt;
                *drive = None;
            }
            _ => return Err(StoreError::StaleDriveToken { id }),
        }
        if self
            .update_operation(StoredOperationRow::encode(record)?, Some(token.to_string()))
            .await?
        {
            Ok(())
        } else {
            Err(StoreError::StaleDriveToken { id })
        }
    }

    async fn finish(
        &mut self,
        id: OperationId,
        token: Uuid,
        outcome: OperationOutcome,
        at: SyncTime,
        keep_result: bool,
    ) -> Result<(), StoreError> {
        self.check_token(id, token).await?;
        self.delete_steps(id.as_uuid().to_string()).await?;
        if keep_result {
            let mut record = self
                .operation(id)
                .await?
                .ok_or(StoreError::MissingOperation { id })?;
            record.execution = match outcome {
                OperationOutcome::Completed { result } => OperationExecution::Completed {
                    result,
                    finished_at: at,
                },
                OperationOutcome::Failed { reason } => OperationExecution::Failed {
                    reason,
                    finished_at: at,
                },
            };
            if !self
                .update_operation(StoredOperationRow::encode(record)?, Some(token.to_string()))
                .await?
            {
                return Err(StoreError::StaleDriveToken { id });
            }
        } else if !self
            .delete_operation(id.as_uuid().to_string(), token.to_string())
            .await?
        {
            return Err(StoreError::StaleDriveToken { id });
        }
        Ok(())
    }

    async fn step(
        &mut self,
        id: OperationId,
        position: u64,
    ) -> Result<Option<StepRecord>, StoreError> {
        self.fetch_step(
            id.as_uuid().to_string(),
            i64::try_from(position).map_err(StoreError::backend)?,
        )
        .await?
        .map(StepRecord::try_from)
        .transpose()
    }
    async fn save_step(&mut self, step: StepRecord, token: Uuid) -> Result<(), StoreError> {
        self.check_token(step.operation, token).await?;
        self.upsert_step(StoredStepRow::encode(step)?).await
    }
    async fn step_count(&mut self, id: OperationId) -> Result<u64, StoreError> {
        u64::try_from(self.count_steps(id.as_uuid().to_string()).await?)
            .map_err(|error| corrupt("step_count", error))
    }
    async fn earliest(&mut self) -> Result<Option<SyncTime>, StoreError> {
        Ok(self
            .earliest_deadline()
            .await?
            .map(SyncTime::from_unix_millis))
    }
}
