use fleet_sync_protocol::{Piece, PieceDigest, ReplicatedTransaction};
use serde::Serialize;

pub(crate) fn assemble_transaction<A: Serialize>(
    parts: &[&Piece<A>],
    incoming: &Piece<A>,
) -> Option<ReplicatedTransaction> {
    let descriptor = incoming.part.as_ref()?;
    if parts.len() != usize::try_from(descriptor.count).ok()? {
        return None;
    }
    let mut transaction = None;
    for (index, piece) in parts.iter().enumerate() {
        let part = piece.part.as_ref()?;
        if usize::try_from(part.index).ok()? != index
            || part.count != descriptor.count
            || part.transaction != descriptor.transaction
            || part.transaction_digest != descriptor.transaction_digest
            || piece.database != incoming.database
            || piece.epoch != incoming.epoch
            || piece.continues_from != incoming.continues_from
            || piece.through != incoming.through
        {
            return None;
        }
        let fragment = piece.transactions.first()?;
        if piece.transactions.len() != 1 || fragment.commit_id() != descriptor.transaction.commit_id
        {
            return None;
        }
        match (&mut transaction, fragment) {
            (None, fragment) => transaction = Some(fragment.clone()),
            (
                Some(ReplicatedTransaction::Rows { rows, .. }),
                ReplicatedTransaction::Rows { rows: more, .. },
            ) => rows.extend(more.iter().cloned()),
            (
                Some(ReplicatedTransaction::SchemaStep {
                    migration_version,
                    statements,
                    ..
                }),
                ReplicatedTransaction::SchemaStep {
                    migration_version: more_version,
                    statements: more,
                    ..
                },
            ) if migration_version == more_version => statements.extend(more.iter().cloned()),
            _ => return None,
        }
    }
    let transaction = transaction?;
    (PieceDigest::of_transaction(&transaction).ok()? == descriptor.transaction_digest)
        .then_some(transaction)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]
    use super::*;
    use fleet_sync_protocol::{
        CommitSequence, DatabaseId, Epoch, PieceContents, ReplicationPosition, RowChange, SqlValue,
        TransactionKey, TransactionPart,
    };

    fn fragments() -> Vec<Piece> {
        let epoch = Epoch::new(1).expect("epoch");
        let transaction = ReplicatedTransaction::Rows {
            commit_id: CommitSequence::new(4),
            rows: (0..3)
                .map(|index| {
                    RowChange::upsert(
                        "records",
                        vec![SqlValue::Integer(index)],
                        vec![
                            SqlValue::Integer(index),
                            SqlValue::Text(format!("row {index}")),
                        ],
                    )
                    .expect("row")
                })
                .collect(),
        };
        let ReplicatedTransaction::Rows { rows, .. } = &transaction else {
            return Vec::new();
        };
        rows.iter()
            .enumerate()
            .map(|(index, row)| {
                Piece::new(PieceContents {
                    database: DatabaseId::new(
                        "019f0000-0000-7000-8000-000000000001"
                            .parse()
                            .expect("UUID"),
                    )
                    .expect("database"),
                    epoch,
                    continues_from: ReplicationPosition::new(epoch, CommitSequence::new(1)),
                    through: ReplicationPosition::new(epoch, CommitSequence::new(4)),
                    transactions: vec![ReplicatedTransaction::Rows {
                        commit_id: CommitSequence::new(4),
                        rows: vec![row.clone()],
                    }],
                    part: Some(
                        TransactionPart::new(
                            TransactionKey {
                                epoch,
                                commit_id: CommitSequence::new(4),
                            },
                            u32::try_from(index).expect("index"),
                            3,
                            PieceDigest::of_transaction(&transaction).expect("transaction digest"),
                        )
                        .expect("part"),
                    ),
                    annotation: None,
                })
                .expect("piece")
            })
            .collect()
    }

    #[test]
    fn complete_fragments_preserve_row_order_and_whole_transaction_digest() {
        // Wrong behavior: dropping/reordering fragments while accepting the whole digest.
        let pieces = fragments();
        let incoming = pieces.last().expect("final part");
        let assembled = assemble_transaction(&pieces.iter().collect::<Vec<_>>(), incoming)
            .expect("complete transaction");
        let ReplicatedTransaction::Rows { commit_id, rows } = assembled else {
            panic!("row transaction expected");
        };
        assert_eq!(commit_id, CommitSequence::new(4));
        let keys = rows
            .iter()
            .map(|row| match row {
                RowChange::Upsert { key, .. } | RowChange::Delete { key, .. } => key.clone(),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                vec![SqlValue::Integer(0)],
                vec![SqlValue::Integer(1)],
                vec![SqlValue::Integer(2)]
            ]
        );
    }

    #[test]
    fn missing_or_duplicate_fragment_never_builds_a_short_transaction() {
        let pieces = fragments();
        let first = pieces.first().expect("first");
        let final_part = pieces.last().expect("last");
        assert!(assemble_transaction(&[first, final_part], final_part).is_none());
        assert!(assemble_transaction(&[first, first, final_part], final_part).is_none());
    }

    #[test]
    fn changed_transaction_content_is_rejected_even_with_valid_piece_digests() {
        let mut pieces = fragments();
        let first = pieces.first_mut().expect("first");
        first.transactions = vec![ReplicatedTransaction::Rows {
            commit_id: CommitSequence::new(4),
            rows: vec![RowChange::delete("records", vec![SqlValue::Integer(99)]).expect("delete")],
        }];
        first.digest = first.recomputed_digest().expect("piece digest");
        assert!(
            assemble_transaction(
                &pieces.iter().collect::<Vec<_>>(),
                pieces.last().expect("last")
            )
            .is_none()
        );
    }

    #[test]
    fn mismatched_count_and_transaction_identity_cannot_combine_fragments() {
        for mismatch_count in [false, true] {
            let mut pieces = fragments();
            let part = pieces
                .first_mut()
                .expect("first")
                .part
                .as_mut()
                .expect("descriptor");
            if mismatch_count {
                part.count = 4;
            } else {
                part.transaction.commit_id = CommitSequence::new(5);
            }
            assert!(
                assemble_transaction(
                    &pieces.iter().collect::<Vec<_>>(),
                    pieces.last().expect("last")
                )
                .is_none()
            );
        }
    }

    #[test]
    fn split_schema_steps_keep_statement_order_and_require_one_migration_version() {
        let mut pieces = fragments().into_iter().take(2).collect::<Vec<_>>();
        let statements = [
            "CREATE TABLE notes(id TEXT PRIMARY KEY)",
            "ALTER TABLE notes ADD COLUMN body TEXT",
        ];
        let whole = ReplicatedTransaction::SchemaStep {
            commit_id: CommitSequence::new(4),
            migration_version: 2,
            statements: statements.iter().map(|sql| (*sql).to_owned()).collect(),
        };
        let digest = PieceDigest::of_transaction(&whole).expect("digest");
        for (piece, sql) in pieces.iter_mut().zip(statements) {
            piece.transactions = vec![ReplicatedTransaction::SchemaStep {
                commit_id: CommitSequence::new(4),
                migration_version: 2,
                statements: vec![sql.into()],
            }];
            let part = piece.part.as_mut().expect("descriptor");
            part.count = 2;
            part.transaction_digest = digest.clone();
            piece.digest = piece.recomputed_digest().expect("piece digest");
        }
        assert_eq!(
            assemble_transaction(
                &pieces.iter().collect::<Vec<_>>(),
                pieces.last().expect("last")
            ),
            Some(whole)
        );
        if let Some(ReplicatedTransaction::SchemaStep {
            migration_version, ..
        }) = pieces.last_mut().expect("last").transactions.first_mut()
        {
            *migration_version = 3;
        }
        assert!(
            assemble_transaction(
                &pieces.iter().collect::<Vec<_>>(),
                pieces.last().expect("last")
            )
            .is_none()
        );
    }

    #[test]
    fn consumer_annotation_needs_serialization_without_requiring_clone() {
        #[derive(serde::Serialize)]
        struct Annotation {
            label: String,
        }
        let pieces = fragments()
            .into_iter()
            .map(|piece| {
                Piece::new(PieceContents {
                    database: piece.database,
                    epoch: piece.epoch,
                    continues_from: piece.continues_from,
                    through: piece.through,
                    transactions: piece.transactions,
                    annotation: piece
                        .part
                        .as_ref()
                        .filter(|part| part.index + 1 == part.count)
                        .map(|_| Annotation {
                            label: "consumer".into(),
                        }),
                    part: piece.part,
                })
                .expect("annotated piece")
            })
            .collect::<Vec<_>>();
        assert!(
            assemble_transaction(
                &pieces.iter().collect::<Vec<_>>(),
                pieces.last().expect("last")
            )
            .is_some()
        );
    }
}
