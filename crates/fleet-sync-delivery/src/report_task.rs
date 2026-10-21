use crate::task_support::*;
use crate::*;
use durable_steps::{
    AtomicStore, BoxFuture, OperationContext, OperationFailure, RequestKey, Resume, StoreError,
    Task, storage::TransactionScope,
};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

pub struct ReportOutcome<S, Db, D> {
    config: DeliveryTaskConfig,
    store: TaskTypes<S, Db, D>,
}
pub(crate) const REPORT_OUTCOME_KIND: &str = "fleet-sync.delivery.report-outcome";
impl<S, Db, D> ReportOutcome<S, Db, D> {
    pub const fn new(config: DeliveryTaskConfig) -> Self {
        Self {
            config,
            store: PhantomData,
        }
    }
}
impl<S, Db, D> Task for ReportOutcome<S, Db, D>
where
    S: AtomicStore,
    Db: Database,
    D: DeliveryTypes,
    for<'t> S::Scope<'t>: TransactionScope<Database = Db>,
    Db::Arguments: IntoArguments<Db>,
    for<'v> String: Type<Db> + Encode<'v, Db>,
    for<'v> Option<String>: Type<Db> + Decode<'v, Db> + Encode<'v, Db>,
    for<'r> &'r str: ColumnIndex<Db::Row>,
{
    const KIND: &'static str = REPORT_OUTCOME_KIND;
    const VERSION: u32 = 1;
    const KEEP_RESULT: bool = false;
    type Store = S;
    type Input = ReportRef;
    type Output = ();
    async fn run<O: OperationContext<Store = S>>(
        &self,
        ctx: &mut O,
        input: &ReportRef,
    ) -> Result<(), OperationFailure> {
        let input = *input;
        let config = self.config;
        let report = ctx
            .local(step("read_report")?, move |scope| {
                Box::pin(async move {
                    Inbox::<_, D>::new(scope, config.local.owner, config.local.limits)
                        .resolve_report(input)
                        .await
                        .map_err(step_error)
                })
            })
            .await?;
        let answer = ctx
            .call(
                step("report")?,
                ReportCall::<D>::new(config.relay, config.local.owner),
                report,
            )
            .await?;
        match answer {
            ReportResult::Applied {
                item, acknowledged, ..
            } if item == input.item && acknowledged >= input.report_seq => {}
            ReportResult::Refused { reason } => {
                return Err(step_error(DeliveryError::Refused { reason }).into());
            }
            _ => {
                return Err(OperationFailure::InvalidPayload {
                    message: "report acknowledgment has another identity".into(),
                });
            }
        }
        ctx.local(step("retire_report")?, move |scope| {
            Box::pin(async move {
                Inbox::<_, D>::new(scope, config.local.owner, config.local.limits)
                    .retire_report(input)
                    .await
                    .map_err(step_error)
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
            let (item, report_seq): (ItemId, ReportSeq) =
                serde_json::from_str(key.as_str()).map_err(StoreError::backend)?;
            let reference = ReportRef { item, report_seq };
            if Inbox::<_, D>::new(scope, self.config.local.owner, self.config.local.limits)
                .report_superseded(reference)
                .await
                .map_err(store_error)?
            {
                Ok(Resume::Finish(()))
            } else {
                Ok(Resume::Continue)
            }
        })
    }
}
