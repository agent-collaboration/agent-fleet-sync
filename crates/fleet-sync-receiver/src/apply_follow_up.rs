use crate::FollowUpDone;
use durable_steps::{
    Admitted, AtomicStore, CallTarget, JournalScope, OperationContext, OperationFailure,
    RequestKey, StartRefusal, StepName, Task,
};
use fleet_sync_protocol::{DatabaseId, Epoch, ReplicationPosition};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::marker::PhantomData;

/// One consumer item and its identity, admitted with the piece's own commit.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FollowUpInput<F> {
    pub database: DatabaseId,
    pub epoch: Epoch,
    pub through: ReplicationPosition,
    pub index: u32,
    pub item: F,
}

/// The host routes this call to its configured ReceiverHook::follow_up.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, bound = "")]
pub struct FollowUpCall<F> {
    pub database: DatabaseId,
    #[serde(skip)]
    item: PhantomData<fn() -> F>,
}
impl<F> FollowUpCall<F> {
    pub fn new(database: DatabaseId) -> Self {
        Self {
            database,
            item: PhantomData,
        }
    }
}
impl<F> CallTarget for FollowUpCall<F>
where
    F: Serialize + DeserializeOwned + Send + 'static,
{
    const KIND: &'static str = "fleet-sync.apply-follow-up";
    type Request = F;
    type Answer = FollowUpDone;
}

/// A single receipted call; piece receipts already own repeat answers.
pub struct ApplyFollowUp<S, F>(PhantomData<fn() -> (S, F)>);
impl<S, F> ApplyFollowUp<S, F> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}
impl<S, F> Default for ApplyFollowUp<S, F> {
    fn default() -> Self {
        Self::new()
    }
}
impl<S, F> Task for ApplyFollowUp<S, F>
where
    S: AtomicStore,
    F: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    const KIND: &'static str = "fleet-sync.apply-follow-up";
    const VERSION: u32 = 1;
    const KEEP_RESULT: bool = false;
    type Store = S;
    type Input = FollowUpInput<F>;
    type Output = FollowUpDone;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        ctx: &mut C,
        input: &Self::Input,
    ) -> Result<Self::Output, OperationFailure> {
        // The journal owns serialized input, so the consumer need not implement Clone.
        let item: F =
            serde_json::from_slice(&serde_json::to_vec(&input.item).map_err(payload_failure)?)
                .map_err(payload_failure)?;
        let step = StepName::new("follow_up").map_err(payload_failure)?;
        Ok(ctx
            .call(step, FollowUpCall::<F>::new(input.database), item)
            .await?)
    }
}
fn payload_failure(error: impl std::fmt::Display) -> OperationFailure {
    OperationFailure::InvalidPayload {
        message: error.to_string(),
    }
}

/// Call inside the host's successful apply transaction, before it commits.
pub async fn admit_follow_ups<S, F>(
    scope: &mut S::Scope<'_>,
    database: DatabaseId,
    epoch: Epoch,
    through: ReplicationPosition,
    items: Vec<F>,
) -> Result<Vec<Admitted>, StartRefusal>
where
    S: AtomicStore,
    F: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let mut admitted = Vec::with_capacity(items.len());
    for (index, item) in items.into_iter().enumerate() {
        let index = u32::try_from(index).map_err(durable_steps::StoreError::backend)?;
        let key = serde_json::to_string(&(database, epoch, through, index))
            .map_err(durable_steps::StoreError::backend)?;
        let key = RequestKey::new(key).map_err(durable_steps::StoreError::backend)?;
        admitted.push(
            scope
                .admit::<ApplyFollowUp<S, F>>(
                    &key,
                    &FollowUpInput {
                        database,
                        epoch,
                        through,
                        index,
                        item,
                    },
                )
                .await?,
        );
    }
    Ok(admitted)
}
