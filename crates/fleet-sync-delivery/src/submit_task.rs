use crate::task_support::*;
use crate::*;
use durable_steps::{
    AtomicStore, BoxFuture, OperationContext, OperationFailure, RequestKey, Resume, StepError,
    StoreError, Task, storage::TransactionScope,
};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::{marker::PhantomData, sync::Arc};

pub struct SubmitItem<S, Db, D, C> {
    limits: DeliveryLimits,
    consumer: Arc<C>,
    store: TaskTypes<S, Db, D>,
}
impl<S, Db, D, C> SubmitItem<S, Db, D, C> {
    pub fn new(limits: DeliveryLimits, consumer: Arc<C>) -> Self {
        Self {
            limits,
            consumer,
            store: PhantomData,
        }
    }
}
impl<S, Db, D, C> Task for SubmitItem<S, Db, D, C>
where
    S: AtomicStore,
    Db: Database,
    D: DeliveryTypes,
    for<'t> S::Scope<'t>: TransactionScope<Database = Db>,
    Db::Arguments: IntoArguments<Db>,
    for<'v> String: Type<Db> + Encode<'v, Db>,
    for<'v> Option<String>: Type<Db> + Decode<'v, Db> + Encode<'v, Db>,
    for<'r> &'r str: ColumnIndex<Db::Row>,
    C: Send + Sync + 'static,
    for<'t> C: DeliveryConsumer<S::Scope<'t>>,
    for<'t> <C as DeliveryConsumer<S::Scope<'t>>>::Protocol: ConsumerProtocol<Delivery = D>,
{
    const KIND: &'static str = "fleet-sync.delivery.submit-item";
    const VERSION: u32 = 1;
    const KEEP_RESULT: bool = false;
    type Store = S;
    type Input = SubmitInput;
    type Output = ();
    async fn run<O: OperationContext<Store = S>>(
        &self,
        ctx: &mut O,
        input: &SubmitInput,
    ) -> Result<(), OperationFailure> {
        let gate_input = input.clone();
        let limits = self.limits;
        let consumer = Arc::clone(&self.consumer);
        let now = ctx.now();
        ctx.local(step("gate")?, move |scope| {
            Box::pin(async move {
                let mut outbox = Outbox::<_, D>::new(scope, limits);
                match outbox
                    .gate(&gate_input, consumer.as_ref(), now)
                    .await
                    .map_err(step_error)?
                {
                    GateDecision::Release => {
                        outbox
                            .mark_submit_attempted(gate_input.item)
                            .await
                            .map_err(step_error)?;
                        Ok(())
                    }
                    GateDecision::Hold { until } => Err(StepError::Retry {
                        after: until.map_or(std::time::Duration::from_secs(1), |until| {
                            delay_until(now, until)
                        }),
                    }),
                }
            })
        })
        .await?;
        let answer = ctx
            .call(
                step("submit")?,
                SubmitCall::<D>::new(input.relay, input.mailbox),
                input.reference(),
            )
            .await?;
        let input = input.clone();
        match answer {
            DispatchOutcome::Answered(answer) => {
                ctx.local(step("mark_sent")?, move |scope| {
                    Box::pin(async move {
                        Outbox::<_, D>::new(scope, limits)
                            .mark_submitted(&input, answer)
                            .await
                            .map_err(step_error)
                    })
                })
                .await?;
                Ok(())
            }
            DispatchOutcome::Unresolvable { reason } => {
                let kind = reason;
                ctx.local(step("mark_failed")?, move |scope| {
                    Box::pin(async move {
                        Outbox::<_, D>::new(scope, limits)
                            .record_submission_failure(&input, kind)
                            .await
                            .map_err(step_error)
                    })
                })
                .await?;
                Err(OperationFailure::Refused {
                    code: reason.as_str().to_owned(),
                    detail: "sender custody is absent and relay reports unknown".into(),
                })
            }
        }
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<()>, StoreError>> {
        Box::pin(async move {
            let item: ItemId = serde_json::from_str(key.as_str()).map_err(StoreError::backend)?;
            let state = match Outbox::<_, D>::new(scope, self.limits).status(item).await {
                Ok(status) => status.state,
                Err(DeliveryError::Corrupt { .. }) => return Ok(Resume::Continue),
                Err(error) => return Err(store_error(error)),
            };
            if matches!(state,OutboxStatusState::Known{status,..} if status.is_terminal()) {
                Ok(Resume::Finish(()))
            } else {
                Ok(Resume::Continue)
            }
        })
    }
    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        key: &'s RequestKey,
        failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>> {
        Box::pin(async move {
            let item: ItemId = serde_json::from_str(key.as_str()).map_err(StoreError::backend)?;
            Outbox::<_, D>::new(scope, self.limits)
                .record_failure(item, failure_kind(failure))
                .await
                .map_err(store_error)
        })
    }
}
