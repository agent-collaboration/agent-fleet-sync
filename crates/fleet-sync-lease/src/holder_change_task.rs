use crate::{
    RegistryConfig,
    holder_change_rules::holder_change,
    operation_support::{failure_code, storage_error},
    registry_storage::RegistryStorage,
};
use durable_steps::{
    AtomicStore, BoxFuture, OperationContext, OperationFailure, RequestKey, Resume, StoreError,
    Task, storage::TransactionScope,
};
use fleet_sync_protocol::*;
use runtime_primitives::Clock;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::{marker::PhantomData, sync::Arc};

pub struct HolderChangeTask<S, D> {
    config: RegistryConfig,
    clock: Arc<dyn Clock>,
    store: PhantomData<fn() -> (S, D)>,
}
impl<S, D> HolderChangeTask<S, D> {
    pub const fn new(config: RegistryConfig, clock: Arc<dyn Clock>) -> Self {
        Self {
            config,
            clock,
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
            let now = self.clock.now();
            let Some(id) = TransferId::try_from(key.as_str().to_owned()).ok() else {
                return Ok(Resume::Continue);
            };
            if let Some(answer) =
                crate::transfer_cancellation::process_cancel::<S, D>(scope, id, now)
                    .await
                    .map_err(storage_error)?
            {
                return Ok(Resume::Finish(answer));
            }
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
            let now = self.clock.now();
            let Some(id) = TransferId::try_from(key.as_str().to_owned()).ok() else {
                return Ok(());
            };
            let Some(admission) = RegistryStorage(scope)
                .admission(id)
                .await
                .map_err(storage_error)?
            else {
                return Ok(());
            };
            crate::transfer_cancellation::process_cancel::<S, D>(scope, id, now)
                .await
                .map_err(storage_error)?;
            let mut record = RegistryStorage(scope)
                .transfer(id)
                .await
                .map_err(storage_error)?
                .unwrap_or_else(|| {
                    crate::registry_records::TransferRecord::requested(
                        crate::registry_records::TransferIntent {
                            request: admission.request,
                            new_epoch: None,
                        },
                        admission.admitted_at,
                    )
                });
            if record.phase == crate::HolderChangePhase::Cancelling {
                return Ok(());
            }
            let settlement = match &record.outcome {
                Some(HolderChangeAnswer::Granted { epoch, .. }) => {
                    SettleOutcome::Granted { epoch: *epoch }
                }
                Some(HolderChangeAnswer::Refused(reason)) => SettleOutcome::Refused {
                    reason: reason.clone(),
                },
                Some(HolderChangeAnswer::Cancelled { .. }) => SettleOutcome::Cancelled,
                _ => SettleOutcome::Refused {
                    reason: HolderChangeRefusal::Unavailable,
                },
            };
            if record.outcome.is_none() {
                record.outcome = Some(HolderChangeAnswer::Failed {
                    reason: failure_code(failure)?,
                });
                record
                    .enter(crate::HolderChangePhase::Ended, now)
                    .map_err(storage_error)?;
                RegistryStorage(scope)
                    .save_transfer(&record)
                    .await
                    .map_err(storage_error)?;
            }
            if matches!(record.intent.request, HolderChangeRequest::Claim { .. }) {
                crate::transfer_cancellation::admit_settlement::<S, D>(scope, id, settlement)
                    .await
                    .map_err(storage_error)?;
            }
            Ok(())
        })
    }
}
