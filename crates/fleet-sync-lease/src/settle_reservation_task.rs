use crate::{ClaimSettlement, operation_support::step};
use durable_steps::{AtomicStore, OperationContext, OperationFailure, Task};
use fleet_sync_protocol::{ClaimSettlement as SettlementRequest, SettlementRecorded};
use std::marker::PhantomData;

/// Settlement debt admitted atomically when a claim's engine operation fails.
pub struct SettleReservationTask<S>(PhantomData<fn() -> S>);
impl<S> SettleReservationTask<S> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}
impl<S> Default for SettleReservationTask<S> {
    fn default() -> Self {
        Self::new()
    }
}
impl<S: AtomicStore> Task for SettleReservationTask<S> {
    const KIND: &'static str = "fleet-sync.settle-reservation";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = SettlementRequest;
    type Output = SettlementRecorded;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        ctx: &mut C,
        request: &SettlementRequest,
    ) -> Result<SettlementRecorded, OperationFailure> {
        Ok(ctx
            .call(step("settle")?, ClaimSettlement, request.clone())
            .await?)
    }
}
