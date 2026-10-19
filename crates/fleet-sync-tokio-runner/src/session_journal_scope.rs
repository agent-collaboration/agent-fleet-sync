//! Forward journal operations to the session transaction's existing SQL scope.
use crate::{SessionHandler, session_store::SessionTransaction};
use durable_steps::{
    Admitted, DriveLease, Host, JournalScope, OperationId, RequestKey, StartRefusal, StoreError,
    SyncTime, Task, Uuid,
    storage::{JournalStorage, OperationOutcome, OperationRecord, StepRecord},
};
use fleet_sync_protocol::ConsumerProtocol;
use fleet_sync_session::PeerAuthenticator;

impl<
    P: ConsumerProtocol,
    A: PeerAuthenticator<Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute>,
    D: SessionHandler<P>,
    H: Host,
> JournalScope for SessionTransaction<'_, P, A, D, H>
{
    async fn admit<T: Task>(
        &mut self,
        key: &RequestKey,
        input: &T::Input,
    ) -> Result<Admitted, StartRefusal> {
        self.inner.admit::<T>(key, input).await
    }
    async fn ensure_wake(&mut self, at: SyncTime) -> Result<(), StoreError> {
        self.inner.ensure_wake(at).await
    }
}
impl<
    P: ConsumerProtocol,
    A: PeerAuthenticator<Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute>,
    D: SessionHandler<P>,
    H: Host,
> JournalStorage for SessionTransaction<'_, P, A, D, H>
{
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
        at: SyncTime,
        attempt: u32,
    ) -> Result<(), StoreError> {
        self.inner.retry(id, token, at, attempt).await
    }
    async fn finish(
        &mut self,
        id: OperationId,
        token: Uuid,
        outcome: OperationOutcome,
        at: SyncTime,
        keep_result: bool,
    ) -> Result<(), StoreError> {
        self.inner.finish(id, token, outcome, at, keep_result).await
    }
    async fn step(
        &mut self,
        id: OperationId,
        position: u64,
    ) -> Result<Option<StepRecord>, StoreError> {
        self.inner.step(id, position).await
    }
    async fn save_step(&mut self, step: StepRecord, token: Uuid) -> Result<(), StoreError> {
        self.inner.save_step(step, token).await
    }
    async fn step_count(&mut self, id: OperationId) -> Result<u64, StoreError> {
        self.inner.step_count(id).await
    }
    async fn earliest(&mut self) -> Result<Option<SyncTime>, StoreError> {
        self.inner.earliest().await
    }
}
