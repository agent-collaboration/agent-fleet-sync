use crate::task_support::*;
use crate::*;
use durable_steps::{
    AtomicStore, BoxFuture, JournalScope, OperationContext, OperationFailure, RequestKey,
    StepError, StoreError, Task, storage::TransactionScope,
};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::{marker::PhantomData, sync::Arc};

pub struct ReceiveItems<S, Db, D, C> {
    config: DeliveryTaskConfig,
    consumer: Arc<C>,
    store: TaskTypes<S, Db, D>,
}
impl<S, Db, D, C> ReceiveItems<S, Db, D, C> {
    pub fn new(config: DeliveryTaskConfig, consumer: Arc<C>) -> Self {
        Self {
            config,
            consumer,
            store: PhantomData,
        }
    }
}
impl<S, Db, D, C> Task for ReceiveItems<S, Db, D, C>
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
    const KIND: &'static str = "fleet-sync.delivery.receive-items";
    const VERSION: u32 = 1;
    const KEEP_RESULT: bool = false;
    type Store = S;
    type Input = ReceiveInput;
    type Output = ();
    async fn run<O: OperationContext<Store = S>>(
        &self,
        ctx: &mut O,
        input: &ReceiveInput,
    ) -> Result<(), OperationFailure> {
        let input = *input;
        let config = self.config;
        let (max_items, max_bytes) = ctx
            .local(step("budget")?, move |scope| {
                Box::pin(async move {
                    Inbox::<_, D>::new(scope, config.local.owner, config.local.limits)
                        .receive_budget(input, config.fetch_items, config.fetch_bytes)
                        .await
                        .map_err(step_error)
                })
            })
            .await?;
        let batch = ctx
            .call(
                step("fetch")?,
                FetchCall::<D>::new(config.relay, config.local.owner),
                FetchInput {
                    stream: input.stream,
                    after: input.after,
                    max_items,
                    max_bytes,
                },
            )
            .await?;
        let consumer = Arc::clone(&self.consumer);
        let now = ctx.now();
        let through = ctx
            .local(step("store")?, move |scope| {
                Box::pin(async move {
                    let through = batch.through;
                    let effects =
                        Inbox::<_, D>::new(scope, config.local.owner, config.local.limits)
                            .store_batch(input, batch, consumer.as_ref(), now)
                            .await
                            .map_err(|error| match error {
                                DeliveryError::InboxFull => StepError::Retry {
                                    after: std::time::Duration::from_secs(1),
                                },
                                error => step_error(error),
                            })?;
                    admit_effects::<_, DeliverLocally<S, Db, D, C>, ReportOutcome<S, Db, D>>(
                        scope, effects,
                    )
                    .await
                    .map_err(step_error)?;
                    Ok(through)
                })
            })
            .await?;
        let receipt = ctx
            .call(
                step("acknowledge")?,
                ReceivedCall {
                    relay: config.relay,
                    mailbox: config.local.owner,
                },
                ReceiptInput {
                    stream: input.stream,
                    through,
                },
            )
            .await?;
        ctx.local(step("finish")?, move |scope| {
            Box::pin(async move {
                let mut inbox = Inbox::<_, D>::new(scope, config.local.owner, config.local.limits);
                inbox
                    .acknowledge_receive(input, receipt)
                    .await
                    .map_err(step_error)?;
                if let Some(next) = inbox.finish_receive(input).await.map_err(step_error)? {
                    scope
                        .admit::<Self>(
                            &delivery_request_key(&(next.stream, next.cycle))
                                .map_err(step_error)?,
                            &next,
                        )
                        .await?;
                }
                Ok(())
            })
        })
        .await?;
        Ok(())
    }

    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        key: &'s RequestKey,
        _failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>> {
        Box::pin(async move {
            let (stream, cycle): (DeliveryStream, ReceiveCycle) =
                serde_json::from_str(key.as_str()).map_err(StoreError::backend)?;
            Inbox::<_, D>::new(scope, self.config.local.owner, self.config.local.limits)
                .fail_receive(stream, cycle)
                .await
                .map_err(store_error)
        })
    }
}
