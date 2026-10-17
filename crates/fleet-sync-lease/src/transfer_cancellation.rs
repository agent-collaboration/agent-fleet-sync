use crate::{registry_records::*, registry_storage::RegistryStorage, *};
use durable_steps::{AtomicStore, JournalScope, RequestKey};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub(crate) async fn process_cancel<S, D>(
    scope: &mut S::Scope<'_>,
    id: TransferId,
    now: SyncTime,
) -> Result<Option<HolderChangeAnswer>, LeaseError>
where
    S: AtomicStore,
    D: Database,
    for<'t> S::Scope<'t>: durable_steps::storage::TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    let Some(cancel) = RegistryStorage(scope).cancel(id).await? else {
        return Ok(None);
    };
    let admission =
        RegistryStorage(scope)
            .admission(id)
            .await?
            .ok_or_else(|| LeaseError::Corrupt {
                field: "cancel admission".into(),
            })?;
    crate::transfer_admission::bound_operation(
        scope,
        admission.operation,
        "fleet-sync.holder-change",
        &holder_change_request_key(id)?,
    )
    .await?;
    let mut record = match RegistryStorage(scope).transfer(id).await? {
        Some(record) => record,
        None => TransferRecord::requested(
            TransferIntent {
                request: admission.request.clone(),
                new_epoch: None,
            },
            admission.admitted_at,
        ),
    };
    if let Some(answer) = record.outcome {
        return Ok(Some(answer));
    }
    if !record.phase.cancellable() {
        return Ok(None);
    }
    if record.phase == HolderChangePhase::Cancelling {
        return Ok(Some(HolderChangeAnswer::Pending { transfer: id }));
    }
    let move_owed = matches!(record.intent.request, HolderChangeRequest::Move { .. })
        && matches!(
            record.phase,
            HolderChangePhase::Releasing | HolderChangePhase::Released { .. }
        );
    let claim_owed = matches!(record.intent.request, HolderChangeRequest::Claim { .. })
        && record.intent.new_epoch.is_some();
    if move_owed {
        record.enter(HolderChangePhase::Cancelling, now)?;
        RegistryStorage(scope).save_transfer(&record).await?;
        let admitted = scope
            .admit::<HolderChangeCancelTask<S, D>>(&cancel_task_key(id)?, &id)
            .await
            .map_err(|error| LeaseError::InvalidInput {
                field: error.to_string(),
            })?;
        let mut admission = admission;
        admission.follow_up = Some(admitted.id());
        RegistryStorage(scope).save_admission(&admission).await?;
        return Ok(Some(HolderChangeAnswer::Pending { transfer: id }));
    }
    let answer = HolderChangeAnswer::Cancelled { by: cancel.by };
    record.outcome = Some(answer.clone());
    record.enter(HolderChangePhase::Ended, now)?;
    RegistryStorage(scope).save_transfer(&record).await?;
    if claim_owed {
        admit_settlement::<S, D>(scope, id, SettleOutcome::Cancelled).await?;
    }
    Ok(Some(answer))
}

pub(crate) fn cancel_task_key(id: TransferId) -> Result<RequestKey, LeaseError> {
    RequestKey::new(format!("holder-change-cancel:{}", id.as_uuid())).map_err(|error| {
        LeaseError::InvalidInput {
            field: error.to_string(),
        }
    })
}

pub(crate) async fn admit_settlement<S, D>(
    scope: &mut S::Scope<'_>,
    id: TransferId,
    outcome: SettleOutcome,
) -> Result<(), LeaseError>
where
    S: AtomicStore,
    D: Database,
    for<'t> S::Scope<'t>: durable_steps::storage::TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    let mut admission =
        RegistryStorage(scope)
            .admission(id)
            .await?
            .ok_or_else(|| LeaseError::Corrupt {
                field: "settlement admission".into(),
            })?;
    let key = RequestKey::new(format!("claim-settle:{}", id.as_uuid())).map_err(|error| {
        LeaseError::InvalidInput {
            field: error.to_string(),
        }
    })?;
    let admitted = scope
        .admit::<SettleReservationTask<S>>(
            &key,
            &fleet_sync_protocol::ClaimSettlement {
                change: admission.request.clone(),
                outcome,
            },
        )
        .await
        .map_err(|error| LeaseError::InvalidInput {
            field: error.to_string(),
        })?;
    admission.follow_up = Some(admitted.id());
    RegistryStorage(scope).save_admission(&admission).await?;
    Ok(())
}
