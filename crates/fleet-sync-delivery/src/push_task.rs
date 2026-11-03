use crate::{
    DeliveryError, Outbox, PublicationRef, PushInput, PushResult, StatusPublicationResult,
    task_support::*, *,
};
use durable_steps::{
    AtomicStore, BoxFuture, CallFailure, CallRetryReason, CallTarget, OperationContext,
    OperationFailure, RequestKey, Resume, StoreError, Task, storage::TransactionScope,
};
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, bound = "")]
pub struct PushCall<D> {
    pub recipient: PeerId,
    #[serde(skip)]
    payload: PhantomData<fn() -> D>,
}
impl<D> PushCall<D> {
    pub const fn new(recipient: PeerId) -> Self {
        Self {
            recipient,
            payload: PhantomData,
        }
    }
    /// No-capacity proves no intake; keep it retryable instead of journaling a
    /// permanent answer that would prevent a later retry from reaching B.
    pub fn validated_answer<A>(answer: PushResult<A>) -> Result<PushResult<A>, CallFailure> {
        if matches!(
            answer,
            PushResult::Refused {
                reason: DeliveryRefusal::MailboxFull
            }
        ) {
            return Err(CallFailure::Retryable {
                reason: CallRetryReason {
                    code: "noCapacity".into(),
                    detail: "recipient inbox has no capacity".into(),
                },
            });
        }
        Ok(answer)
    }
}
impl<D: DeliveryTypes> CallTarget for PushCall<D> {
    const KIND: &'static str = "fleet-sync.delivery.push";
    type Request = DeliveryItemRef;
    type Answer = PushResult<D::Acceptance>;
}

pub struct PushItem<S, Db, D, TPublish> {
    limits: DeliveryLimits,
    store: TaskTypes<S, Db, D>,
    publisher: PhantomData<fn() -> TPublish>,
}
impl<S, Db, D, TPublish> PushItem<S, Db, D, TPublish> {
    pub const fn new(limits: DeliveryLimits) -> Self {
        Self {
            limits,
            store: PhantomData,
            publisher: PhantomData,
        }
    }
}
impl<S, Db, D, TPublish> Task for PushItem<S, Db, D, TPublish>
where
    S: AtomicStore,
    Db: Database,
    D: DeliveryTypes,
    TPublish: Task<Input = PublicationRef<D::StatusRoute>>,
    for<'t> S::Scope<'t>: TransactionScope<Database = Db>,
    Db::Arguments: IntoArguments<Db>,
    for<'v> String: Type<Db> + Encode<'v, Db>,
    for<'v> Option<String>: Type<Db> + Decode<'v, Db> + Encode<'v, Db>,
    for<'r> &'r str: ColumnIndex<Db::Row>,
{
    const KIND: &'static str = "fleet-sync.delivery.push-item";
    const VERSION: u32 = 1;
    const KEEP_RESULT: bool = false;
    type Store = S;
    type Input = PushInput;
    type Output = ();
    async fn run<O: OperationContext<Store = S>>(
        &self,
        ctx: &mut O,
        input: &PushInput,
    ) -> Result<(), OperationFailure> {
        let answer = ctx
            .call(
                step("push")?,
                PushCall::<D>::new(input.recipient),
                input.reference.clone(),
            )
            .await?;
        if matches!(
            answer,
            PushResult::Refused {
                reason: DeliveryRefusal::MailboxFull
            }
        ) {
            return Err(OperationFailure::InvalidPayload {
                message: "Push host must return no-capacity through PushCall::validated_answer"
                    .into(),
            });
        }
        let input = input.clone();
        let limits = self.limits;
        let now = ctx.now();
        ctx.local(step("receipt")?, move |scope| {
            Box::pin(async move {
                match Outbox::<_, D>::new(scope, limits)
                    .receipt::<TPublish>(input.recipient, &input.reference, answer, now)
                    .await
                    .map_err(step_error)?
                {
                    StatusPublicationResult::Applied => Ok(()),
                    StatusPublicationResult::Refused { reason } => {
                        Err(step_error(DeliveryError::Refused { reason }))
                    }
                }
            })
        })
        .await?;
        Ok(())
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<()>, StoreError>> {
        Box::pin(async move {
            let item: ItemId = serde_json::from_str(key.as_str()).map_err(StoreError::backend)?;
            let mut outbox = Outbox::<_, D>::new(scope, self.limits);
            let header = outbox.header_state(item).await.map_err(store_error)?;
            match header {
                Some(crate::outbox_record::OutboxHeaderState::Item(header))
                    if !header.status.is_terminal() && header.handed_off_at.is_none() =>
                {
                    Ok(Resume::Continue)
                }
                _ => Ok(Resume::Finish(())),
            }
        })
    }
}
