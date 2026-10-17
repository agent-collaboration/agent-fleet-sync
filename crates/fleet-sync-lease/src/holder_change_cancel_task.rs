use crate::{
    operation_support::{step, step_error, storage_error},
    registry_storage::RegistryStorage,
    *,
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

/// Withdraw the release owed by a cancelled move; it may wait until superseded.
pub struct HolderChangeCancelTask<S, D> {
    _config: RegistryConfig,
    clock: Arc<dyn Clock>,
    store: PhantomData<fn() -> (S, D)>,
}
impl<S, D> HolderChangeCancelTask<S, D> {
    pub const fn new(config: RegistryConfig, clock: Arc<dyn Clock>) -> Self {
        Self {
            _config: config,
            clock,
            store: PhantomData,
        }
    }
}
impl<S, D> Task for HolderChangeCancelTask<S, D>
where
    S: AtomicStore,
    D: Database,
    for<'t> S::Scope<'t>: TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    const KIND: &'static str = "fleet-sync.holder-change-cancel";
    const VERSION: u32 = 1;
    type Store = S;
    type Input = TransferId;
    type Output = HolderChangeAnswer;
    async fn run<C: OperationContext<Store = S>>(
        &self,
        ctx: &mut C,
        id: &TransferId,
    ) -> Result<Self::Output, OperationFailure> {
        let transfer = *id;
        let prepared: Result<(WithdrawRelease, PeerId), HolderChangeAnswer> = ctx
            .local(step("check")?, move |scope| {
                Box::pin(async move {
                    let mut storage = RegistryStorage(scope);
                    let record = storage
                        .transfer(transfer)
                        .await
                        .map_err(step_error)?
                        .ok_or_else(|| {
                            step_error(LeaseError::Corrupt {
                                field: "cancelled move missing".into(),
                            })
                        })?;
                    if let Some(answer) = record.outcome {
                        return Ok(Err(answer));
                    }
                    let HolderChangeRequest::Move {
                        database,
                        from_epoch,
                        ..
                    } = record.intent.request
                    else {
                        return Err(step_error(LeaseError::Corrupt {
                            field: "withdrawal is not a move".into(),
                        }));
                    };
                    let holder = storage
                        .lease(database)
                        .await
                        .map_err(step_error)?
                        .ok_or_else(|| step_error(LeaseError::NotHeld))?
                        .holder;
                    Ok(Ok((
                        WithdrawRelease {
                            database,
                            epoch: from_epoch,
                            transfer,
                        },
                        holder,
                    )))
                })
            })
            .await?;
        let (request, holder) = match prepared {
            Ok(prepared) => prepared,
            Err(answer) => return Ok(answer),
        };
        ctx.call(step("withdraw")?, HolderWithdrawal { holder }, request)
            .await?;
        let now = ctx.now();
        Ok(ctx
            .local(step("finish")?, move |scope| {
                Box::pin(async move {
                    let mut storage = RegistryStorage(scope);
                    let mut record = storage
                        .transfer(transfer)
                        .await
                        .map_err(step_error)?
                        .ok_or_else(|| {
                            step_error(LeaseError::Corrupt {
                                field: "cancelled move missing".into(),
                            })
                        })?;
                    if let Some(answer) = record.outcome {
                        return Ok(answer);
                    }
                    let cancel = storage
                        .cancel(transfer)
                        .await
                        .map_err(step_error)?
                        .ok_or_else(|| {
                            step_error(LeaseError::Corrupt {
                                field: "cancel record missing".into(),
                            })
                        })?;
                    let answer = HolderChangeAnswer::Cancelled { by: cancel.by };
                    record.outcome = Some(answer.clone());
                    record
                        .enter(HolderChangePhase::Ended, now)
                        .map_err(step_error)?;
                    storage.save_transfer(&record).await.map_err(step_error)?;
                    Ok(answer)
                })
            })
            .await?)
    }
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<Self::Output>, StoreError>> {
        Box::pin(async move {
            let _now = self.clock.now();
            let raw = key
                .as_str()
                .strip_prefix("holder-change-cancel:")
                .ok_or_else(|| StoreError::backend("invalid withdrawal task key"))?;
            let id = TransferId::new(uuid::Uuid::parse_str(raw).map_err(StoreError::backend)?)
                .map_err(StoreError::backend)?;
            let record = RegistryStorage(scope)
                .transfer(id)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| StoreError::backend("missing withdrawal transfer"))?;
            Ok(match record.outcome {
                Some(answer) => Resume::Finish(answer),
                None => Resume::Continue,
            })
        })
    }
}
