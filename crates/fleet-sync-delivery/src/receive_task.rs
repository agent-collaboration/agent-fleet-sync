use crate::task_support::*;
use crate::*;
use durable_steps::{
    AtomicStore, BoxFuture, JournalScope, OperationContext, OperationFailure, RequestKey,
    StepError, StoreError, Task, storage::TransactionScope,
};
use runtime_primitives::Clock;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::{marker::PhantomData, sync::Arc};

// Keep the v0.2.0 tuple readable at its original step identity, including
// when a consumer enables per-item budgeting while an operation is pending.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum ReceiveBudget {
    Legacy((u32, u64)),
    PerItem(FetchInput),
}

pub struct ReceiveItems<S, Db, D, C, B> {
    binding: Arc<B>,
    config: DeliveryTaskConfig,
    consumer: Arc<C>,
    clock: Arc<dyn Clock>,
    store: TaskTypes<S, Db, D>,
}
impl<S, Db, D, C, B> ReceiveItems<S, Db, D, C, B> {
    pub fn new(
        config: DeliveryTaskConfig,
        consumer: Arc<C>,
        clock: Arc<dyn Clock>,
        binding: Arc<B>,
    ) -> Self {
        Self {
            binding,
            config,
            consumer,
            clock,
            store: PhantomData,
        }
    }
}
impl<S, Db, D, C, B> Task for ReceiveItems<S, Db, D, C, B>
where
    S: AtomicStore,
    Db: Database,
    D: DeliveryTypes,
    B: Send + Sync + 'static,
    for<'t> B: SenderBinding<S::Scope<'t>, D>,
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
        let budget = {
            let binding = Arc::clone(&self.binding);
            ctx.local(step("budget")?, move |scope| {
                Box::pin(async move {
                    let mut inbox = Inbox::<_, D, B>::new(
                        scope,
                        config.local.owner,
                        config.local.limits,
                        binding.as_ref(),
                    );
                    if D::PER_ITEM_SETTLEMENT {
                        inbox
                            .receive_request(input, config.fetch_items, config.fetch_bytes)
                            .await
                            .map(ReceiveBudget::PerItem)
                            .map_err(step_error)
                    } else {
                        inbox
                            .receive_budget(input, config.fetch_items, config.fetch_bytes)
                            .await
                            .map(ReceiveBudget::Legacy)
                            .map_err(step_error)
                    }
                })
            })
        }
        .await?;
        let fetch_input = match budget {
            ReceiveBudget::Legacy((max_items, max_bytes)) => FetchInput {
                stream: input.stream,
                after: input.after,
                max_items,
                max_bytes,
                inbox_room_bytes: None,
            },
            ReceiveBudget::PerItem(input) => input,
        };
        let batch = ctx
            .call(
                step("fetch")?,
                FetchCall::<D>::new(config.relay, config.local.owner),
                fetch_input,
            )
            .await?;
        let consumer = Arc::clone(&self.consumer);
        let now = ctx.now();
        let through = {
            let binding = Arc::clone(&self.binding);
            ctx.local(step("store")?, move |scope| {
                Box::pin(async move {
                    let through = batch.through;
                    let effects = Inbox::<_, D, B>::new(
                        scope,
                        config.local.owner,
                        config.local.limits,
                        binding.as_ref(),
                    )
                    .store_batch(input, batch, consumer.as_ref(), now)
                    .await
                    .map_err(|error| match error {
                        DeliveryError::InboxFull => StepError::Retry {
                            after: std::time::Duration::from_secs(1),
                        },
                        error => step_error(error),
                    })?;
                    admit_effects::<_, DeliverLocally<S, Db, D, C, B>, ReportOutcome<S, Db, D, B>>(
                        scope, effects,
                    )
                    .await
                    .map_err(step_error)?;
                    Ok(through)
                })
            })
        }
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
        let now = ctx.now();
        {
            let binding = Arc::clone(&self.binding);
            ctx.local(step("finish")?, move |scope| {
                Box::pin(async move {
                    let mut inbox = Inbox::<_, D, B>::new(
                        scope,
                        config.local.owner,
                        config.local.limits,
                        binding.as_ref(),
                    );
                    inbox
                        .acknowledge_receive(input, receipt, now)
                        .await
                        .map_err(step_error)?;
                    if let Some(next) =
                        inbox.finish_receive(input, now).await.map_err(step_error)?
                    {
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
        }
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
            Inbox::<_, D, B>::new(
                scope,
                self.config.local.owner,
                self.config.local.limits,
                self.binding.as_ref(),
            )
            .fail_receive(stream, cycle, self.clock.now())
            .await
            .map_err(store_error)
        })
    }
}
