//! This control forwards real SQL/journal work and blocks after its first admission.
use super::*;
use durable_steps::{
    Admitted, DriveLease, JournalScope, OperationId, RequestKey, StartRefusal, StoreError, Task,
    Uuid,
    storage::{JournalStorage, OperationOutcome, OperationRecord, StepRecord, TransactionScope},
    turso_store::TursoScope,
};
use sqlx_turso::{Turso, TursoConnection};
use std::io::Write;

pub const CHECKPOINT: &str = "FIRST_EXISTING_REMOVAL_ADMITTED_NOT_COMMITTED";
pub struct StartupCheckpointScope<'view, 'transaction> {
    pub inner: &'view mut TursoScope<'transaction, TestHost>,
}
impl TransactionScope for StartupCheckpointScope<'_, '_> {
    type Database = Turso;
    type Executor<'query>
        = &'query mut TursoConnection
    where
        Self: 'query;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.inner.executor()
    }
}
impl JournalScope for StartupCheckpointScope<'_, '_> {
    async fn running_request_keys(&mut self, kind: &str) -> Result<Vec<RequestKey>, StoreError> {
        self.inner.running_request_keys(kind).await
    }
    async fn ensure_wake(&mut self, at: SyncTime) -> Result<(), StoreError> {
        self.inner.ensure_wake(at).await
    }
    async fn admit<T: Task>(
        &mut self,
        key: &RequestKey,
        input: &T::Input,
    ) -> Result<Admitted, StartRefusal> {
        let admitted = self.inner.admit::<T>(key, input).await?;
        if matches!(admitted, Admitted::New(_)) {
            println!("\n{CHECKPOINT}");
            std::io::stdout().flush().map_err(StoreError::backend)?;
            std::future::pending::<()>().await;
        }
        Ok(admitted)
    }
}
impl JournalStorage for StartupCheckpointScope<'_, '_> {
    async fn operation(&mut self, id: OperationId) -> Result<Option<OperationRecord>, StoreError> {
        self.inner.operation(id).await
    }
    async fn due(&mut self, now: SyncTime) -> Result<Vec<OperationId>, StoreError> {
        self.inner.due(now).await
    }
    async fn claim(
        &mut self,
        id: OperationId,
        now: SyncTime,
        lease: DriveLease,
    ) -> Result<Option<OperationRecord>, StoreError> {
        self.inner.claim(id, now, lease).await
    }
    async fn check_token(&mut self, id: OperationId, token: Uuid) -> Result<(), StoreError> {
        self.inner.check_token(id, token).await
    }
    async fn retry(
        &mut self,
        id: OperationId,
        token: Uuid,
        next: SyncTime,
        attempt: u32,
    ) -> Result<(), StoreError> {
        self.inner.retry(id, token, next, attempt).await
    }
    async fn finish(
        &mut self,
        id: OperationId,
        token: Uuid,
        outcome: OperationOutcome,
        at: SyncTime,
        keep: bool,
    ) -> Result<(), StoreError> {
        self.inner.finish(id, token, outcome, at, keep).await
    }
    async fn step(
        &mut self,
        id: OperationId,
        position: u64,
    ) -> Result<Option<StepRecord>, StoreError> {
        self.inner.step(id, position).await
    }
    async fn save_step(&mut self, record: StepRecord, token: Uuid) -> Result<(), StoreError> {
        self.inner.save_step(record, token).await
    }
    async fn step_count(&mut self, id: OperationId) -> Result<u64, StoreError> {
        self.inner.step_count(id).await
    }
    async fn earliest(&mut self) -> Result<Option<SyncTime>, StoreError> {
        self.inner.earliest().await
    }
}
