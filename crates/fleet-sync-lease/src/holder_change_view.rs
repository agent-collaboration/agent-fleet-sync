use crate::{registry_storage::RegistryStorage, transfer_admission::journal_error, *};
use durable_steps::storage::{JournalStorage, OperationExecution, StepState, TransactionScope};
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub async fn holder_change_view<S>(
    scope: &mut S,
    attempt: TransferId,
) -> Result<Option<HolderChangeView>, LeaseError>
where
    S: TransactionScope + JournalStorage,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let mut storage = RegistryStorage(scope);
    let Some(admission) = storage.admission(attempt).await? else {
        return Ok(None);
    };
    let record = storage.transfer(attempt).await?;
    let cancel = storage.cancel(attempt).await?;
    let (phase, history, outcome) = match record {
        Some(record) => (record.phase, record.phase_history, record.outcome),
        None => (
            HolderChangePhase::Requested,
            vec![PhaseEntry {
                phase: HolderChangePhase::Requested,
                entered_at: admission.admitted_at,
            }],
            None,
        ),
    };
    let (mut waiting, parent_running, reserved) = pending(
        scope,
        admission.operation,
        "fleet-sync.holder-change",
        &holder_change_request_key(attempt)?,
    )
    .await?;
    let mut follow_running = false;
    if let Some(id) = admission.follow_up {
        let (kind, key) = if matches!(admission.request, HolderChangeRequest::Move { .. }) {
            (
                "fleet-sync.holder-change-cancel",
                crate::transfer_cancellation::cancel_task_key(attempt)?,
            )
        } else {
            (
                "fleet-sync.settle-reservation",
                durable_steps::RequestKey::new(format!("claim-settle:{}", attempt.as_uuid()))
                    .map_err(|error| LeaseError::InvalidInput {
                        field: error.to_string(),
                    })?,
            )
        };
        let (follow_waiting, running, _) = pending(scope, id, kind, &key).await?;
        follow_running = running;
        if running {
            waiting = follow_waiting;
        }
    }
    let settlement_owed = matches!(admission.request, HolderChangeRequest::Claim { .. })
        && outcome.is_some()
        && (follow_running
            || (parent_running
                && (reserved || matches!(outcome, Some(HolderChangeAnswer::Granted { .. })))));
    Ok(Some(HolderChangeView {
        request: admission.request,
        requested_by: admission.requested_by,
        phase,
        history,
        waiting,
        settlement_owed,
        cancel,
        outcome,
    }))
}

async fn pending<S>(
    scope: &mut S,
    id: OperationId,
    kind: &str,
    key: &durable_steps::RequestKey,
) -> Result<(Option<Waiting>, bool, bool), LeaseError>
where
    S: TransactionScope + JournalStorage,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let operation = crate::transfer_admission::bound_operation(scope, id, kind, key).await?;
    let OperationExecution::Running {
        attempt,
        next_attempt_at,
        ..
    } = operation.execution
    else {
        return Ok((None, false, false));
    };
    let count = scope.step_count(id).await.map_err(journal_error)?;
    let mut waiting = None;
    let mut reserved = false;
    for position in 0..count {
        let step = scope
            .step(id, position)
            .await
            .map_err(journal_error)?
            .ok_or_else(|| LeaseError::Corrupt {
                field: "holder-change journal step".into(),
            })?;
        if step.name.as_str() == "reserve" {
            reserved = match &step.state {
                StepState::CallPending { .. } => true,
                StepState::CallDone { output, .. } => matches!(
                    serde_json::from_slice::<GateAnswer>(output),
                    Ok(GateAnswer::Reserved)
                ),
                StepState::Local { .. } => false,
            };
        }
        if let StepState::CallPending {
            intent,
            attempt: call,
        } = step.state
        {
            let (kind, target): (String, serde_json::Value) = serde_json::from_str(&intent.target)
                .map_err(|_| LeaseError::Corrupt {
                    field: "holder-change waiting target".into(),
                })?;
            let on = match kind.as_str() {
                "fleet-sync.holder-release" => WaitingOn::Holder(
                    serde_json::from_value::<HolderRelease>(target)
                        .map_err(|_| LeaseError::Corrupt {
                            field: "waiting holder".into(),
                        })?
                        .holder,
                ),
                "fleet-sync.holder-withdrawal" => WaitingOn::Holder(
                    serde_json::from_value::<HolderWithdrawal>(target)
                        .map_err(|_| LeaseError::Corrupt {
                            field: "waiting withdrawal".into(),
                        })?
                        .holder,
                ),
                "fleet-sync.copy-fence" => WaitingOn::Copy(
                    serde_json::from_value::<CopyFence>(target)
                        .map_err(|_| LeaseError::Corrupt {
                            field: "waiting fence".into(),
                        })?
                        .copy,
                ),
                "fleet-sync.copy-status" => WaitingOn::Copy(
                    serde_json::from_value::<CopyRead>(target)
                        .map_err(|_| LeaseError::Corrupt {
                            field: "waiting copy".into(),
                        })?
                        .copy,
                ),
                "fleet-sync.copy-acknowledged" => WaitingOn::Copy(
                    serde_json::from_value::<CopyAcknowledged>(target)
                        .map_err(|_| LeaseError::Corrupt {
                            field: "waiting acknowledgement".into(),
                        })?
                        .copy,
                ),
                "fleet-sync.claim-reservation" | "fleet-sync.claim-settlement" => WaitingOn::Gate,
                _ => {
                    return Err(LeaseError::Corrupt {
                        field: "unknown holder-change waiting target".into(),
                    });
                }
            };
            waiting = Some(Waiting {
                on,
                since: call.first_attempt_at,
                last_seen: call.last_seen,
                reach: call.reach,
                attempt,
                next_attempt_at,
            });
        }
    }
    Ok((waiting, true, reserved))
}
