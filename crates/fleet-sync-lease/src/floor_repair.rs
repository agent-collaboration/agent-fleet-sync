//! Reserve above the authoritative copy's fence without reusing an earlier reservation.
use crate::{
    LeaseError, change_checks,
    operation_support::{step, step_error},
    registry_storage::RegistryStorage,
};
use durable_steps::{AtomicStore, OperationContext, storage::TransactionScope};
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

#[derive(Serialize, Deserialize)]
pub(crate) enum RepairedFence {
    Reserved {
        epoch: Epoch,
        expect: FenceExpectation,
    },
    Answer(HolderChangeAnswer),
}

pub(crate) async fn repair<C, D>(
    ctx: &mut C,
    request: &HolderChangeRequest,
    copy_fence: Epoch,
    acked: ReplicationPosition,
) -> Result<RepairedFence, LeaseError>
where
    C: OperationContext,
    D: Database,
    for<'t> <C::Store as AtomicStore>::Scope<'t>: TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    let request = request.clone();
    Ok(ctx
        .local(step("repair")?, move |scope| {
            Box::pin(async move {
                let mut storage = RegistryStorage(scope);
                let id = change_checks::attempt(&request);
                let mut record =
                    storage
                        .transfer(id)
                        .await
                        .map_err(step_error)?
                        .ok_or_else(|| {
                            step_error(LeaseError::Corrupt {
                                field: "floor repair transfer".into(),
                            })
                        })?;
                if record.intent.request != request {
                    return Err(step_error(LeaseError::InvalidInput {
                        field: "floor repair request identity".into(),
                    }));
                }
                if let Some(answer) = record.outcome {
                    return Ok(RepairedFence::Answer(answer));
                }
                let database = record.intent.database();
                let floor = storage
                    .floor(database)
                    .await
                    .map_err(step_error)?
                    .map_or(copy_fence, |floor| floor.max(copy_fence));
                let floor = record
                    .intent
                    .new_epoch
                    .map_or(floor, |reserved| floor.max(reserved));
                let Some(next) = floor
                    .value()
                    .checked_add(1)
                    .and_then(|value| Epoch::new(value).ok())
                else {
                    let answer = HolderChangeAnswer::Refused(HolderChangeRefusal::EpochUnprovable);
                    record.outcome = Some(answer.clone());
                    storage.save_transfer(&record).await.map_err(step_error)?;
                    return Ok(RepairedFence::Answer(answer));
                };
                storage
                    .save_floor(database, next)
                    .await
                    .map_err(step_error)?;
                storage
                    .repair_transfer_epoch(id, next)
                    .await
                    .map_err(step_error)?;
                Ok(RepairedFence::Reserved {
                    epoch: next,
                    expect: FenceExpectation {
                        epoch: copy_fence,
                        acked,
                    },
                })
            })
        })
        .await?)
}
