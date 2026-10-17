use crate::{CapturedTransaction, PieceAnnotation, SenderError, capture_stream::CapturedCommit};
use fleet_sync_protocol::{
    ConsumerProtocol, DatabaseId, Epoch, Piece, PieceContents, PieceDigest, ReplicatedTransaction,
    ReplicationPosition, TransactionKey, TransactionPart,
};
use std::num::NonZeroUsize;

pub(crate) fn cut_transaction<A: PieceAnnotation>(
    database: DatabaseId,
    epoch: Epoch,
    after: ReplicationPosition,
    captured: &CapturedCommit,
    max_items: NonZeroUsize,
    annotation: &A,
) -> Result<Vec<Piece<<A::Protocol as ConsumerProtocol>::Annotation>>, SenderError> {
    let count = match &captured.transaction {
        ReplicatedTransaction::Rows { rows, .. } => rows.len(),
        ReplicatedTransaction::SchemaStep { statements, .. } => statements.len(),
    };
    let part_count = count.div_ceil(max_items.get()).max(1);
    let mut pieces = Vec::new();
    let whole_digest =
        PieceDigest::of_transaction(&captured.transaction).map_err(|_| SenderError::Invalid {
            field: "transaction.digest",
        })?;
    for index in 0..part_count {
        let start = index
            .checked_mul(max_items.get())
            .ok_or(SenderError::Invalid {
                field: "part.index",
            })?;
        let end = start.saturating_add(max_items.get()).min(count);
        let transaction = match &captured.transaction {
            ReplicatedTransaction::Rows { commit_id, rows } => ReplicatedTransaction::Rows {
                commit_id: *commit_id,
                rows: rows
                    .get(start..end)
                    .ok_or(SenderError::Invalid { field: "part.rows" })?
                    .to_vec(),
            },
            ReplicatedTransaction::SchemaStep {
                commit_id,
                migration_version,
                statements,
            } => ReplicatedTransaction::SchemaStep {
                commit_id: *commit_id,
                migration_version: *migration_version,
                statements: statements
                    .get(start..end)
                    .ok_or(SenderError::Invalid {
                        field: "part.statements",
                    })?
                    .to_vec(),
            },
        };
        let part = if part_count > 1 {
            Some(
                TransactionPart::new(
                    TransactionKey {
                        epoch,
                        commit_id: captured.position.sequence,
                    },
                    u32::try_from(index).map_err(|_| SenderError::Invalid {
                        field: "part.index",
                    })?,
                    u32::try_from(part_count).map_err(|_| SenderError::Invalid {
                        field: "part.count",
                    })?,
                    whole_digest.clone(),
                )
                .map_err(|_| SenderError::Invalid {
                    field: "part.descriptor",
                })?,
            )
        } else {
            None
        };
        let annotation = if index + 1 == part_count {
            annotation.annotate(&[CapturedTransaction {
                position: captured.position,
                transaction: &captured.transaction,
            }])
        } else {
            None
        };
        pieces.push(Piece::new(PieceContents {
            database,
            epoch,
            continues_from: after,
            through: captured.position,
            transactions: vec![transaction],
            part,
            annotation,
        })?);
    }
    Ok(pieces)
}
