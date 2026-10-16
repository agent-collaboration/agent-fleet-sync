use crate::{
    RegistryConfig, SettleReservationTask,
    holder_change_rules::holder_change,
    operation_support::{failure_code, storage_error},
    registry_storage::RegistryStorage,
};
use durable_steps::{
    AtomicStore, BoxFuture, JournalScope, OperationContext, OperationFailure, RequestKey, Resume,
    StoreError, Task, storage::TransactionScope,
};
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

pub struct HolderChangeTask<S, D> {
    config: RegistryConfig,
    store: PhantomData<fn() -> (S, D)>,
}
impl<S, D> HolderChangeTask<S, D> {
    pub const fn new(config: RegistryConfig) -> Self {
        Self {
            config,
            store: PhantomData,
        }
    }
}
impl<S, D> Task for HolderChangeTask<S, D>
where
    S: AtomicStore,
    D: Database,
    for<'t> S::Scope<'t>: TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    const KIND: &'static str = "fleet-sync.holder-change";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = HolderChangeRequest;
    type Output = HolderChangeAnswer;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        ctx: &mut C,
        request: &HolderChangeRequest,
    ) -> Result<HolderChangeAnswer, OperationFailure> {
        holder_change::<C, D>(ctx, request, &self.config)
            .await
            .map_err(Into::into)
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<Self::Output>, StoreError>> {
        Box::pin(async move {
            let Some(id) = uuid::Uuid::parse_str(key.as_str())
                .ok()
                .and_then(|id| TransferId::new(id).ok())
            else {
                return Ok(Resume::Continue);
            };
            let Some(record) = RegistryStorage(scope)
                .transfer(id)
                .await
                .map_err(storage_error)?
            else {
                return Ok(Resume::Continue);
            };
            // Only a takeover's persisted supersession bypasses pending calls. In
            // particular, a committed claim grant still has to settle its gate.
            match record.outcome {
                Some(
                    answer @ HolderChangeAnswer::Refused(HolderChangeRefusal::Superseded { .. }),
                ) if matches!(record.intent.request, HolderChangeRequest::Move { .. }) => {
                    Ok(Resume::Finish(answer))
                }
                _ => Ok(Resume::Continue),
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
            let Some(id) = uuid::Uuid::parse_str(key.as_str())
                .ok()
                .and_then(|id| TransferId::new(id).ok())
            else {
                return Ok(());
            };
            let Some(mut record) = RegistryStorage(scope)
                .transfer(id)
                .await
                .map_err(storage_error)?
            else {
                return Ok(());
            };
            let settlement = match &record.outcome {
                Some(HolderChangeAnswer::Granted { epoch, .. }) => {
                    SettleOutcome::Granted { epoch: *epoch }
                }
                Some(HolderChangeAnswer::Refused(reason)) => SettleOutcome::Refused {
                    reason: reason.clone(),
                },
                _ => SettleOutcome::Refused {
                    reason: HolderChangeRefusal::Unavailable,
                },
            };
            if record.outcome.is_none() {
                record.outcome = Some(HolderChangeAnswer::Failed {
                    reason: failure_code(failure)?,
                });
                RegistryStorage(scope)
                    .save_transfer(&record)
                    .await
                    .map_err(storage_error)?;
            }
            if matches!(record.intent.request, HolderChangeRequest::Claim { .. }) {
                let key = RequestKey::new(format!("claim-settle:{}", id.as_uuid()))
                    .map_err(StoreError::backend)?;
                scope
                    .admit::<SettleReservationTask<S>>(
                        &key,
                        &ClaimSettlement {
                            change: record.intent.request,
                            outcome: settlement,
                        },
                    )
                    .await
                    .map_err(StoreError::backend)?;
            }
            Ok(())
        })
    }
}
