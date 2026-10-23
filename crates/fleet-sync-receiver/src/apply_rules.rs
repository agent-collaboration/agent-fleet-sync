use crate::{
    AppliedPiece, ApplyDecision, ApplyFailure, ReceiverError, ReceiverRules,
    copy_contract::CopyRecord, copy_storage::CopyStorage, part_assembly::assemble_transaction,
    row_transactions::RowTransactions,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::{
    ConsumerProtocol, PeerId, Piece, PieceRefusal, ReplicatedTransaction, SchemaStatementForm,
    SyncTime, TransactionKey,
};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// Apply within the host's transaction. Every error requires the host to roll back.
/// Admit returned follow-ups before committing; deliver them after the commit.
// Explicit Send is required at the AtomicStore GAT callback boundary.
#[allow(clippy::manual_async_fn)]
pub fn apply_piece<'scope, S, H>(
    scope: &'scope mut S,
    piece: &'scope Piece<<H::Protocol as ConsumerProtocol>::Annotation>,
    sender: &'scope PeerId,
    hook: &'scope H,
    now: SyncTime,
) -> impl std::future::Future<Output = Result<ApplyDecision<H::FollowUp>, ApplyFailure>> + Send + 'scope
where
    S: TransactionScope + 'scope,
    H: ReceiverRules<S>,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> f64: Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Vec<u8>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    async move {
        let mut status = match CopyStorage(scope).load().await? {
            Some(CopyRecord::Following(status)) if status.database == piece.database => status,
            Some(CopyRecord::Retired { .. }) => {
                return Err(ApplyFailure::Refused(PieceRefusal::Retired));
            }
            _ => return Err(ApplyFailure::Refused(PieceRefusal::NotInitialized)),
        };
        // Authorization precedes receipts: an old receipt never restores a writer's authority.
        if piece.epoch < status.fence {
            let Some((old_holder, ended_at)) = CopyStorage(scope).epoch_end(piece.epoch).await?
            else {
                return Err(ApplyFailure::Refused(PieceRefusal::NotWriter {
                    current: Some(status.fence),
                }));
            };
            if old_holder.as_ref() != Some(sender) {
                return Err(ApplyFailure::Refused(PieceRefusal::NotWriter {
                    current: Some(status.fence),
                }));
            }
            return Err(ApplyFailure::Refused(PieceRefusal::StaleEpoch {
                newer: status.fence,
                ended_at,
            }));
        }
        if piece.epoch != status.fence || status.holder.as_ref() != Some(sender) {
            return Err(ApplyFailure::Refused(PieceRefusal::NotWriter {
                current: Some(status.fence),
            }));
        }
        if piece.recomputed_digest().ok().as_ref() != Some(&piece.digest) {
            return Err(ApplyFailure::Refused(PieceRefusal::PieceConflict));
        }
        if let Some(digest) = CopyStorage(scope).receipt(piece).await? {
            return if digest == piece.digest {
                Ok(ApplyDecision::AlreadyApplied {
                    through: piece.through,
                })
            } else {
                Err(ApplyFailure::Refused(PieceRefusal::PieceConflict))
            };
        }
        if piece.through <= status.acked || piece.continues_from != status.acked {
            return Err(ApplyFailure::Refused(PieceRefusal::Gap {
                acked: status.acked,
            }));
        }
        admit_transactions(&piece.transactions)?;
        let assembled;
        let staged;
        let transactions = if let Some(part) = &piece.part {
            staged = CopyStorage(scope).parts(part.transaction).await?;
            if let Some(repeated) = staged.iter().find(|existing| {
                existing
                    .part
                    .as_ref()
                    .is_some_and(|existing| existing.index == part.index)
            }) {
                return if repeated.digest == piece.digest {
                    Ok(ApplyDecision::PartStaged {
                        transaction: part.transaction,
                        index: part.index,
                        acked: status.acked,
                    })
                } else {
                    Err(ApplyFailure::Refused(PieceRefusal::PieceConflict))
                };
            }
            if part.index + 1 < part.count {
                if staged.iter().any(|existing| {
                    existing.continues_from != piece.continues_from
                        || existing.through != piece.through
                        || existing.part.as_ref().is_none_or(|existing| {
                            existing.count != part.count
                                || existing.transaction_digest != part.transaction_digest
                        })
                }) {
                    return Err(ApplyFailure::Refused(PieceRefusal::PieceConflict));
                }
                CopyStorage(scope).save_part(piece, now).await?;
                return Ok(ApplyDecision::PartStaged {
                    transaction: part.transaction,
                    index: part.index,
                    acked: status.acked,
                });
            }
            let mut fragments = staged.iter().collect::<Vec<_>>();
            fragments.push(piece);
            assembled = [assemble_transaction(&fragments, piece).ok_or({
                ApplyFailure::RefusedDiscardingParts {
                    refusal: PieceRefusal::Gap {
                        acked: status.acked,
                    },
                    transaction: part.transaction,
                }
            })?];
            admit_transactions(&assembled)?;
            assembled.as_slice()
        } else {
            staged = Vec::new();
            piece.transactions.as_slice()
        };
        RowTransactions(scope)
            .apply(&mut status.schema, transactions)
            .await?;
        let applied = AppliedPiece {
            database: piece.database,
            epoch: piece.epoch,
            through: piece.through,
            transactions,
            annotation: piece.annotation.as_ref(),
        };
        let follow_ups = hook
            .on_applied(scope, &applied)
            .await
            .map_err(|error| match error {
                crate::HookError::Refused(refusal) => {
                    ApplyFailure::Refused(PieceRefusal::Hook(refusal))
                }
                crate::HookError::Storage(error) => crate::ReceiverError::Storage(error).into(),
            })?;
        for fragment in &staged {
            CopyStorage(scope).save_receipt(fragment).await?;
        }
        CopyStorage(scope).save_receipt(piece).await?;
        if let Some(part) = &piece.part {
            CopyStorage(scope).drop_parts(part.transaction).await?;
        }
        status.acked = piece.through;
        CopyStorage(scope)
            .save(&CopyRecord::Following(status))
            .await?;
        Ok(ApplyDecision::Applied {
            through: piece.through,
            follow_ups,
        })
    }
}

fn admit_transactions(transactions: &[ReplicatedTransaction]) -> Result<(), ApplyFailure> {
    if let Some(form) = unsupported_schema_form(transactions) {
        return Err(ApplyFailure::Refused(PieceRefusal::SchemaUnsupported {
            form,
        }));
    }
    Ok(())
}

fn unsupported_schema_form(transactions: &[ReplicatedTransaction]) -> Option<SchemaStatementForm> {
    transactions.iter().find_map(|transaction| {
        let ReplicatedTransaction::SchemaStep { statements, .. } = transaction else {
            return None;
        };
        statements.iter().find_map(|sql| {
            fleet_sync_protocol::admit_replicated_schema_statement(sql)
                .err()
                .map(|error| error.form)
        })
    })
}

/// After a final-part refusal, discard its staging in a separate host transaction.
// Explicit Send is required by borrowed-scope host dispatch.
#[allow(clippy::manual_async_fn)]
pub fn discard_staged_parts<'scope, S>(
    scope: &'scope mut S,
    transaction: &'scope TransactionKey,
) -> impl std::future::Future<Output = Result<(), ReceiverError>> + Send + 'scope
where
    S: TransactionScope + 'scope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'value> Option<String>:
        Type<S::Database> + Encode<'value, S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    async move {
        CopyStorage(scope).drop_parts(*transaction).await?;
        Ok(())
    }
}
