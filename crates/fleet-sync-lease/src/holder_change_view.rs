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
    let parent = read_journal_progress(
        scope,
        admission.operation,
        "fleet-sync.holder-change",
        &holder_change_request_key(attempt)?,
    )
    .await?;
    let (mut waiting, parent_unresolved, parent_failed, reserved) = match parent {
        JournalProgress::Running { waiting, reserved } => (waiting, true, false, reserved),
        JournalProgress::Completed => (None, false, false, false),
        JournalProgress::Failed { .. } => (None, true, true, false),
    };
    let mut follow_unresolved = None;
    let mut follow_up_failure = None;
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
        match read_journal_progress(scope, id, kind, &key).await? {
            JournalProgress::Running {
                waiting: child_waiting,
                ..
            } => {
                follow_unresolved = Some(true);
                waiting = child_waiting;
            }
            JournalProgress::Completed => {
                follow_unresolved = Some(false);
                waiting = None;
            }
            JournalProgress::Failed { reason } => {
                follow_unresolved = Some(true);
                waiting = None;
                follow_up_failure = Some(reason);
            }
        }
    }
    let settlement_owed = matches!(admission.request, HolderChangeRequest::Claim { .. })
        && outcome.is_some()
        && follow_unresolved.unwrap_or(
            parent_unresolved
                && (parent_failed
                    || reserved
                    || matches!(outcome, Some(HolderChangeAnswer::Granted { .. }))),
        );
    Ok(Some(HolderChangeView {
        request: admission.request,
        requested_by: admission.requested_by,
        phase,
        history,
        waiting,
        settlement_owed,
        follow_up_failure,
        cancel,
        outcome,
    }))
}

async fn read_journal_progress<S>(
    scope: &mut S,
    id: OperationId,
    kind: &str,
    key: &durable_steps::RequestKey,
) -> Result<JournalProgress, LeaseError>
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
    let (attempt, next_attempt_at) = match operation.execution {
        OperationExecution::Running {
            attempt,
            next_attempt_at,
            ..
        } => (attempt, next_attempt_at),
        OperationExecution::Completed { .. } => return Ok(JournalProgress::Completed),
        OperationExecution::Failed { reason, .. } => {
            return Ok(JournalProgress::Failed {
                reason: crate::operation_support::failure_code(&reason).map_err(journal_error)?,
            });
        }
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
        if let StepState::CallDone { intent, output, .. } = &step.state {
            let (kind, _): (String, serde_json::Value) = serde_json::from_str(&intent.target)
                .map_err(|_| LeaseError::Corrupt {
                    field: "holder-change waiting target".into(),
                })?;
            let answered_reservation = validate_saved_answer(&kind, output)?;
            if step.name.as_str() == "reserve" {
                reserved = answered_reservation;
            }
        } else if step.name.as_str() == "reserve" {
            reserved = matches!(step.state, StepState::CallPending { .. });
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
            let copy_unavailable = call
                .retry_reason
                .as_ref()
                .map(|reason| {
                    if !matches!(on, WaitingOn::Copy(_)) {
                        return Err(LeaseError::Corrupt {
                            field: "retry reason on a non-copy call".into(),
                        });
                    }
                    CopyUnavailableReason::decode_retry(&reason.code, &reason.detail).map_err(
                        |_| LeaseError::Corrupt {
                            field: "copy retry reason".into(),
                        },
                    )
                })
                .transpose()?;
            waiting = Some(Waiting {
                on,
                since: call.first_attempt_at,
                last_seen: call.last_seen,
                reach: call.reach,
                copy_unavailable,
                attempt,
                next_attempt_at,
            });
        }
    }
    Ok(JournalProgress::Running { waiting, reserved })
}

enum JournalProgress {
    Running {
        waiting: Option<Waiting>,
        reserved: bool,
    },
    Completed,
    Failed {
        reason: FailureCode,
    },
}

fn saved_answer<A: serde::de::DeserializeOwned>(
    output: &[u8],
    allow_never_sent: bool,
) -> Result<Option<A>, LeaseError> {
    match serde_json::from_slice::<durable_steps::CallResult<A>>(output) {
        Ok(durable_steps::CallResult::Answered(answer)) => Ok(Some(answer)),
        Ok(durable_steps::CallResult::NeverSent { .. }) if allow_never_sent => Ok(None),
        _ => Err(LeaseError::Corrupt {
            field: "holder-change saved call answer".into(),
        }),
    }
}

/// Validate each exact call wrapper and report whether it reserved the claim.
fn validate_saved_answer(kind: &str, output: &[u8]) -> Result<bool, LeaseError> {
    match kind {
        "fleet-sync.claim-reservation" => {
            return Ok(matches!(
                saved_answer::<GateAnswer>(output, false)?,
                Some(GateAnswer::Reserved)
            ));
        }
        "fleet-sync.holder-release" => {
            saved_answer::<ReleaseAnswer>(output, true)?;
        }
        "fleet-sync.holder-withdrawal" => {
            saved_answer::<WithdrawAnswer>(output, false)?;
        }
        "fleet-sync.copy-fence" => {
            saved_answer::<FenceResult>(output, false)?;
        }
        "fleet-sync.copy-status" => {
            saved_answer::<CopyStatusAnswer>(output, false)?;
        }
        "fleet-sync.copy-acknowledged" => {
            saved_answer::<AwaitAckedAnswer>(output, false)?;
        }
        "fleet-sync.claim-settlement" => {
            saved_answer::<SettlementRecorded>(output, false)?;
        }
        _ => {
            return Err(LeaseError::Corrupt {
                field: "unknown holder-change saved target".into(),
            });
        }
    }
    Ok(false)
}
