use crate::{
    change_checks::{self, CheckedChange},
    change_completion,
    operation_support::step,
    *,
};
use durable_steps::{AtomicStore, CallResult, OperationContext, storage::TransactionScope};
use fleet_sync_protocol as wire;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// Reserve a unique epoch, complete the external fence, then grant under a fresh local check.
pub async fn holder_change<C, D>(
    ctx: &mut C,
    request: &HolderChangeRequest,
    config: &RegistryConfig,
) -> Result<HolderChangeAnswer, LeaseError>
where
    C: OperationContext,
    D: Database,
    for<'t> <C::Store as AtomicStore>::Scope<'t>: TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    let (intent, holder) = match change_checks::check::<C, D>(ctx, request).await? {
        CheckedChange::Answer(answer) => return Ok(answer),
        CheckedChange::Continue { intent, holder } => (intent, holder),
    };
    let mut new_epoch = intent.new_epoch.ok_or_else(|| LeaseError::Corrupt {
        field: "reserved transfer epoch".into(),
    })?;
    let database = change_checks::database(request);
    let mut expect = match request {
        HolderChangeRequest::Move {
            attempt,
            from_epoch,
            ..
        } => {
            let holder = holder.ok_or_else(|| LeaseError::Corrupt {
                field: "move holder".into(),
            })?;
            let released = ctx
                .call_or_give_up(
                    step("request_release")?,
                    HolderRelease { holder, database },
                    ReleaseRequest {
                        database,
                        epoch: *from_epoch,
                        transfer: *attempt,
                    },
                    config.release_unsent_for,
                )
                .await?;
            let released = match released {
                CallResult::Answered(answer) => answer,
                CallResult::NeverSent { last_seen } => {
                    return change_completion::refuse::<C, D>(
                        ctx,
                        request,
                        "holder_unreachable",
                        HolderChangeRefusal::HolderUnreachable { last_seen },
                    )
                    .await;
                }
            };
            if let Some(answer) =
                change_completion::outcome::<C, D>(ctx, request, "after_release").await?
            {
                return Ok(answer);
            }
            let final_position = match released {
                ReleaseAnswer::Released { final_position } => final_position,
                ReleaseAnswer::RequestConflict => {
                    return Err(LeaseError::InvalidInput {
                        field: "holder rejected the release call identity".into(),
                    });
                }
                ReleaseAnswer::ReleaseRefused(ReleaseRefusal::MigrationPending { .. }) => {
                    return change_completion::refuse::<C, D>(
                        ctx,
                        request,
                        "release_refused",
                        HolderChangeRefusal::MigrationPending,
                    )
                    .await;
                }
                ReleaseAnswer::ReleaseRefused(ReleaseRefusal::SchemaTooNew {
                    store,
                    supported,
                }) => {
                    return change_completion::refuse::<C, D>(
                        ctx,
                        request,
                        "release_refused",
                        HolderChangeRefusal::SchemaTooNew { store, supported },
                    )
                    .await;
                }
                ReleaseAnswer::ReleaseRefused(ReleaseRefusal::Superseded { .. }) => {
                    return change_completion::refuse::<C, D>(
                        ctx,
                        request,
                        "release_refused",
                        HolderChangeRefusal::PreviewStale,
                    )
                    .await;
                }
            };
            // An idle holder still ends at the position inherited from its predecessor.
            // The copy must acknowledge and fence at that exact position below.
            if final_position.epoch > *from_epoch {
                return Err(LeaseError::InvalidInput {
                    field: "release position belongs to another epoch".into(),
                });
            }
            let acked = ctx
                .call(
                    step("await_acked")?,
                    CopyAcknowledged {
                        copy: config.authoritative_receiver,
                        database,
                    },
                    AwaitAcked {
                        through: final_position,
                    },
                )
                .await?;
            match acked {
                AwaitAckedAnswer::Acked { through } if through >= final_position => {}
                AwaitAckedAnswer::Acked { .. } => {
                    return Err(LeaseError::InvalidInput {
                        field: "copy acknowledged less than the released position".into(),
                    });
                }
                AwaitAckedAnswer::NotInitialized => {
                    return change_completion::refuse::<C, D>(
                        ctx,
                        request,
                        "copy_not_initialized",
                        HolderChangeRefusal::Unavailable,
                    )
                    .await;
                }
                AwaitAckedAnswer::Retired => {
                    return change_completion::refuse::<C, D>(
                        ctx,
                        request,
                        "copy_retired",
                        HolderChangeRefusal::Retired,
                    )
                    .await;
                }
            }
            FenceExpectation {
                epoch: *from_epoch,
                acked: final_position,
            }
        }
        HolderChangeRequest::Takeover { preview, .. } => FenceExpectation {
            epoch: preview.fence,
            acked: preview.acked,
        },
        HolderChangeRequest::Claim { .. } => {
            let gate = ctx
                .call(
                    step("reserve")?,
                    crate::ClaimReservation,
                    wire::ClaimReservation {
                        change: request.clone(),
                    },
                )
                .await?;
            match gate {
                GateAnswer::Reserved => {}
                GateAnswer::Refused { reason } => {
                    return change_completion::refuse::<C, D>(
                        ctx,
                        request,
                        "reservation_refused",
                        HolderChangeRefusal::GateRefused { reason },
                    )
                    .await;
                }
                GateAnswer::Admitted => {
                    return Err(LeaseError::InvalidInput {
                        field: "claim gate returned Admitted".into(),
                    });
                }
            }
            if let Some(answer) =
                change_completion::outcome::<C, D>(ctx, request, "after_reserve").await?
            {
                return settle_claim::<C, D>(ctx, request, answer).await;
            }
            let status = ctx
                .call(
                    step("copy_status")?,
                    CopyRead {
                        copy: config.authoritative_receiver,
                        database,
                    },
                    (),
                )
                .await?;
            let status = match status {
                CopyStatusAnswer::Status(status) => status,
                CopyStatusAnswer::NotInitialized => {
                    let answer = change_completion::refuse::<C, D>(
                        ctx,
                        request,
                        "copy_not_initialized",
                        HolderChangeRefusal::Unavailable,
                    )
                    .await?;
                    return settle_claim::<C, D>(ctx, request, answer).await;
                }
                CopyStatusAnswer::Retired => {
                    let answer = change_completion::refuse::<C, D>(
                        ctx,
                        request,
                        "copy_retired",
                        HolderChangeRefusal::Retired,
                    )
                    .await?;
                    return settle_claim::<C, D>(ctx, request, answer).await;
                }
            };
            if status.database != database || status.role != CopyRole::Authoritative {
                return Err(LeaseError::InvalidInput {
                    field: "claim copy status identifies another copy".into(),
                });
            }
            FenceExpectation {
                epoch: status.fence,
                acked: status.acked,
            }
        }
    };
    if let Some(answer) = change_completion::outcome::<C, D>(ctx, request, "before_fence").await? {
        return settle_claim::<C, D>(ctx, request, answer).await;
    }
    let mut fenced = ctx
        .call(
            step("fence")?,
            CopyFence {
                copy: config.authoritative_receiver,
            },
            FenceRequest {
                database,
                transfer_id: change_checks::attempt(request),
                new_epoch,
                expect: expect.clone(),
                target_schema_max: config.target_schema_max,
                holder: change_checks::target(request),
            },
        )
        .await?;
    if let FenceResult::FenceNotAdvanced { current_epoch } = fenced {
        match crate::floor_repair::repair::<C, D>(ctx, request, current_epoch, expect.acked).await?
        {
            crate::floor_repair::RepairedFence::Answer(answer) => {
                return settle_claim::<C, D>(ctx, request, answer).await;
            }
            crate::floor_repair::RepairedFence::Reserved {
                epoch,
                expect: repaired,
            } => {
                new_epoch = epoch;
                expect = repaired;
            }
        }
        fenced = ctx
            .call(
                step("fence:2")?,
                CopyFence {
                    copy: config.authoritative_receiver,
                },
                FenceRequest {
                    database,
                    transfer_id: change_checks::attempt(request),
                    new_epoch,
                    expect: expect.clone(),
                    target_schema_max: config.target_schema_max,
                    holder: change_checks::target(request),
                },
            )
            .await?;
    }
    let refusal = match &fenced {
        FenceResult::Fenced {
            old_epoch,
            new_epoch: actual,
            start,
        } => {
            if *actual != new_epoch || *old_epoch != expect.epoch || *start != expect.acked {
                return Err(LeaseError::InvalidInput {
                    field: "fence answer differs from requested transition".into(),
                });
            }
            None
        }
        FenceResult::FenceNotAdvanced { .. } => Some(HolderChangeRefusal::EpochUnprovable),
        FenceResult::PreviewStale { .. } => Some(HolderChangeRefusal::PreviewStale),
        FenceResult::SchemaTooNew { store, supported } => Some(HolderChangeRefusal::SchemaTooNew {
            store: store.clone(),
            supported: Some(*supported),
        }),
        FenceResult::NotInitialized => Some(HolderChangeRefusal::Unavailable),
        FenceResult::Retired => Some(HolderChangeRefusal::Retired),
    };
    let answer = if let Some(reason) = refusal {
        change_completion::refuse::<C, D>(ctx, request, "fence_refused", reason).await?
    } else {
        change_completion::grant::<C, D>(ctx, request, fenced, expect, config).await?
    };
    settle_claim::<C, D>(ctx, request, answer).await
}

async fn settle_claim<C, D>(
    ctx: &mut C,
    request: &HolderChangeRequest,
    answer: HolderChangeAnswer,
) -> Result<HolderChangeAnswer, LeaseError>
where
    C: OperationContext,
    D: Database,
    for<'t> <C::Store as AtomicStore>::Scope<'t>: TransactionScope<Database = D>,
    D::Arguments: IntoArguments<D>,
    for<'value> String: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'value> Option<String>: Type<D> + Encode<'value, D> + Decode<'value, D>,
    for<'row> &'row str: ColumnIndex<D::Row>,
{
    if !matches!(request, HolderChangeRequest::Claim { .. }) {
        return Ok(answer);
    }
    let outcome = match &answer {
        HolderChangeAnswer::Granted { epoch, .. } => SettleOutcome::Granted { epoch: *epoch },
        HolderChangeAnswer::Refused(reason) => SettleOutcome::Refused {
            reason: reason.clone(),
        },
        _ => {
            return Err(LeaseError::Corrupt {
                field: "claim settlement requires a terminal grant decision".into(),
            });
        }
    };
    ctx.call(
        step("settle")?,
        crate::ClaimSettlement,
        wire::ClaimSettlement {
            change: request.clone(),
            outcome,
        },
    )
    .await?;
    Ok(answer)
}
