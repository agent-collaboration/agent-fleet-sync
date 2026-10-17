use crate::WriterJournalScope;
use durable_steps::{
    DriveLease, Host, OperationId, StoreError, SyncTime, Uuid,
    storage::{JournalStorage, OperationOutcome, OperationRecord, StepRecord},
};

impl<H: Host> JournalStorage for WriterJournalScope<'_, H> {
    async fn operation(&mut self, id: OperationId) -> Result<Option<OperationRecord>, StoreError> {
        self.journal().operation(id).await
    }
    async fn due(&mut self, now: SyncTime) -> Result<Vec<OperationId>, StoreError> {
        self.journal().due(now).await
    }
    async fn claim(
        &mut self,
        id: OperationId,
        now: SyncTime,
        lease: DriveLease,
    ) -> Result<Option<OperationRecord>, StoreError> {
        self.journal().claim(id, now, lease).await
    }
    async fn check_token(&mut self, id: OperationId, token: Uuid) -> Result<(), StoreError> {
        self.journal().check_token(id, token).await
    }
    async fn retry(
        &mut self,
        id: OperationId,
        token: Uuid,
        next_attempt_at: SyncTime,
        attempt: u32,
    ) -> Result<(), StoreError> {
        self.journal()
            .retry(id, token, next_attempt_at, attempt)
            .await
    }
    async fn finish(
        &mut self,
        id: OperationId,
        token: Uuid,
        outcome: OperationOutcome,
        at: SyncTime,
        keep_result: bool,
    ) -> Result<(), StoreError> {
        self.journal()
            .finish(id, token, outcome, at, keep_result)
            .await
    }
    async fn step(
        &mut self,
        id: OperationId,
        position: u64,
    ) -> Result<Option<StepRecord>, StoreError> {
        self.journal().step(id, position).await
    }
    async fn save_step(&mut self, step: StepRecord, token: Uuid) -> Result<(), StoreError> {
        self.journal().save_step(step, token).await
    }
    async fn step_count(&mut self, id: OperationId) -> Result<u64, StoreError> {
        self.journal().step_count(id).await
    }
    async fn earliest(&mut self) -> Result<Option<SyncTime>, StoreError> {
        self.journal().earliest().await
    }
}
