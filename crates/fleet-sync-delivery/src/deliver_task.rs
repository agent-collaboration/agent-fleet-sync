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

pub struct DeliverLocally<S, Db, D, C, B> {
    binding: Arc<B>,
    config: LocalDeliveryConfig,
    consumer: Arc<C>,
    clock: Arc<dyn runtime_primitives::Clock>,
    store: TaskTypes<S, Db, D>,
}
pub(crate) const DELIVER_LOCALLY_KIND: &str = "fleet-sync.delivery.deliver-locally";
impl<S, Db, D, C, B> DeliverLocally<S, Db, D, C, B> {
    pub fn new(
        config: LocalDeliveryConfig,
        consumer: Arc<C>,
        clock: Arc<dyn runtime_primitives::Clock>,
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
impl<S, Db, D, C, B> Task for DeliverLocally<S, Db, D, C, B>
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
    const KIND: &'static str = DELIVER_LOCALLY_KIND;
    const VERSION: u32 = 2;
    const KEEP_RESULT: bool = false;
    type Store = S;
    type Input = DeliveryAttempt;
    type Output = ();
    async fn run<O: OperationContext<Store = S>>(
        &self,
        ctx: &mut O,
        input: &DeliveryAttempt,
    ) -> Result<(), OperationFailure> {
        let input = *input;
        let config = self.config;
        let now = ctx.now();
        let check = {
            let binding = Arc::clone(&self.binding);
            ctx.local(step("check")?, move |scope| {
                Box::pin(async move {
                    let check =
                        Inbox::<_, D, B>::new(scope, config.owner, config.limits, binding.as_ref())
                            .check_delivery(input, now)
                            .await
                            .map_err(step_error)?;
                    match check {
                        DeliveryCheck::Wait { until } => Err(StepError::Retry {
                            after: delay_until(now, until),
                        }),
                        check => Ok(check),
                    }
                })
            })
        }
        .await?;
        let (outcome, observation) = match check {
            DeliveryCheck::Finish { outcome: None } => return Ok(()),
            DeliveryCheck::Finish {
                outcome: Some(RecipientOutcome::Expired),
            } => (LocalOutcome::Expired, None),
            DeliveryCheck::Finish { .. } | DeliveryCheck::Wait { .. } => {
                return Err(OperationFailure::InvalidPayload {
                    message: "invalid saved delivery check".into(),
                });
            }
            DeliveryCheck::Deliver { reference } => match ctx
                .call(step("deliver")?, DeliverCall::<D>::new(), reference)
                .await?
            {
                DispatchOutcome::Answered(LocalDeliveryAnswer::Observed {
                    outcome,
                    observation,
                }) => (outcome, Some(observation)),
                DispatchOutcome::Answered(LocalDeliveryAnswer::Uncertain) => {
                    (LocalOutcome::Uncertain, None)
                }
                DispatchOutcome::Unresolvable { reason } => {
                    return Err(OperationFailure::Refused {
                        code: reason.as_str().to_owned(),
                        detail: "recipient custody cannot be resolved".into(),
                    });
                }
            },
        };
        let consumer = Arc::clone(&self.consumer);
        let now = ctx.now();
        {
            let binding = Arc::clone(&self.binding);
            ctx.local(step("outcome")?, move |scope| {
                Box::pin(async move {
                    let effects =
                        Inbox::<_, D, B>::new(scope, config.owner, config.limits, binding.as_ref())
                            .complete_attempt(
                                input,
                                outcome,
                                consumer.as_ref(),
                                now,
                                observation.as_ref(),
                            )
                            .await
                            .map_err(step_error)?;
                    admit_effects::<_, Self, ReportOutcome<S, Db, D, B>>(scope, effects)
                        .await
                        .map_err(step_error)?;
                    Ok(())
                })
            })
        }
        .await?;
        Ok(())
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        _operation: &'s durable_steps::OperationId,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<()>, StoreError>> {
        Box::pin(async move {
            let (item, attempt): (ItemId, u64) =
                serde_json::from_str(key.as_str()).map_err(StoreError::backend)?;
            let now = self.clock.now();
            if now >= crate::removal_deadline(item) {
                return Ok(Resume::Finish(()));
            }
            let mut inbox = Inbox::<_, D, B>::new(
                scope,
                self.config.owner,
                self.config.limits,
                self.binding.as_ref(),
            );
            let Some(header) = inbox.header(item).await.map_err(store_error)? else {
                return Ok(Resume::Continue);
            };
            if header.progress.attempt() != attempt
                || matches!(
                    header.progress,
                    crate::inbox_record::InboxProgress::Final { .. }
                        | crate::inbox_record::InboxProgress::Held { .. }
                )
            {
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
            let (item, attempt): (ItemId, u64) =
                serde_json::from_str(key.as_str()).map_err(StoreError::backend)?;
            let now = self.clock.now();
            if now >= crate::removal_deadline(item) {
                return Ok(());
            }
            let input = DeliveryAttempt {
                item,
                attempt,
                not_before: SyncTime::from_unix_millis(0),
            };
            let outcome = if matches!(failure, OperationFailure::Uncertain { .. }) {
                RecipientOutcome::Uncertain
            } else {
                RecipientOutcome::Refused {
                    reason: DeliveryRefusal::ProcessingFailed {
                        kind: failure_kind(failure),
                    },
                }
            };
            let mut inbox = Inbox::<_, D, B>::new(
                scope,
                self.config.owner,
                self.config.limits,
                self.binding.as_ref(),
            );
            let mut effects = inbox
                .record_terminal(input, outcome, self.consumer.as_ref(), now)
                .await
                .map_err(store_error)?;
            let cancelled = inbox
                .apply_pending_cancel(item, self.consumer.as_ref(), now, false)
                .await
                .map_err(store_error)?;
            effects.attempts.extend(cancelled.attempts);
            effects.reports.extend(cancelled.reports);
            admit_effects::<_, Self, ReportOutcome<S, Db, D, B>>(scope, effects)
                .await
                .map_err(store_error)?;
            Ok(())
        })
    }
}
