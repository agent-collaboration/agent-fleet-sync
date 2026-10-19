#![allow(clippy::panic_in_result_fn)]
use super::*;
use crate::sender_test_fixture::TestResult;
use sizing_oracle::cut_transaction_oracle;

struct Annotator(Option<usize>);
struct Protocol;
impl ConsumerProtocol for Protocol {
    type Delivery = fleet_sync_protocol::NoDelivery;
    type Request = ();
    type Answer = ();
    type ServerRequest = ();
    type ClientAnswer = ();
    type Notice = ();
    type Annotation = Annotation;
}
#[derive(serde::Serialize, serde::Deserialize)]
struct Annotation {
    text: String,
}
impl PieceAnnotation for Annotator {
    type Protocol = Protocol;
    fn annotate(&self, _: &[CapturedTransaction<'_>]) -> Option<Annotation> {
        self.0.map(|bytes| Annotation {
            text: "a".repeat(bytes),
        })
    }
}
fn compare(
    transaction: ReplicatedTransaction,
    max_items: usize,
    annotation: Option<usize>,
) -> TestResult {
    let database = DatabaseId::new("019eef00-0000-7000-8000-000000000001".parse()?)?;
    let epoch = Epoch::new(1)?;
    let after = ReplicationPosition::new(epoch, fleet_sync_protocol::CommitSequence::new(0));
    let captured = CapturedCommit {
        position: ReplicationPosition::new(epoch, transaction.commit_id()),
        changed_at: fleet_sync_protocol::SyncTime::from_unix_millis(1),
        transaction,
        layout_after: vec![],
    };
    let max_items = NonZeroUsize::new(max_items).ok_or("zero max_items")?;
    let expected = cut_transaction_oracle(
        database,
        epoch,
        after,
        &captured,
        max_items,
        &Annotator(annotation),
    );
    FULL_MEASURES.with(|count| count.set(0));
    let actual = cut_transaction(
        database,
        epoch,
        after,
        &captured,
        max_items,
        &Annotator(annotation),
    );
    let calls = FULL_MEASURES.with(std::cell::Cell::get);
    match (expected, actual) {
        (Ok(expected), Ok(actual)) => {
            assert!(
                calls <= actual.len() + 3,
                "{} fragments needed {calls} full measures/digests",
                actual.len()
            );
            assert_eq!(serde_json::to_vec(&actual)?, serde_json::to_vec(&expected)?);
            for piece in &actual {
                assert_eq!(piece.digest, piece.recomputed_digest()?);
            }
        }
        (Err(SenderError::UnitTooLarge(expected)), Err(SenderError::UnitTooLarge(actual))) => {
            assert_eq!(actual, expected)
        }
        (expected, actual) => {
            return Err(format!(
                "cutter/oracle diverged: {:?} / {:?}",
                expected.err(),
                actual.err()
            )
            .into());
        }
    }
    Ok(())
}
fn rows(count: usize, bytes: usize) -> TestResult<ReplicatedTransaction> {
    Ok(ReplicatedTransaction::Rows {
        commit_id: fleet_sync_protocol::CommitSequence::new(1),
        rows: (0..count)
            .map(|index| {
                RowChange::upsert(
                    "records",
                    vec![SqlValue::Text(format!(
                        "019eef00-0000-7000-8000-{index:012x}"
                    ))],
                    vec![
                        SqlValue::Text(format!("{index}:{}", "\"\\雪🦀\n".repeat(bytes / 12))),
                        SqlValue::Blob(vec![0, 128, 255]),
                        SqlValue::Integer(-9007199254740993),
                        SqlValue::Null,
                    ],
                )
            })
            .collect::<Result<_, _>>()?,
    })
}
#[test]
fn generated_piece_fragments_match_oracle_with_linear_full_measures() -> TestResult {
    for max in [1000, 7, 1] {
        for annotation in [None, Some(0), Some(512 * 1024)] {
            compare(rows(33, 48 * 1024)?, max, annotation)?;
        }
    }
    for bytes in [
        TARGET_UNIT_BYTES - 1024,
        TARGET_UNIT_BYTES + 1024,
        MAX_UNIT_BYTES - 1024,
        MAX_UNIT_BYTES + 1024,
    ] {
        compare(
            ReplicatedTransaction::Rows {
                commit_id: fleet_sync_protocol::CommitSequence::new(1),
                rows: vec![RowChange::upsert(
                    "records",
                    vec![SqlValue::Text(
                        "019eef00-0000-7000-8000-000000000001".into(),
                    )],
                    vec![SqlValue::Text("x".repeat(bytes))],
                )?],
            },
            1000,
            None,
        )?;
    }
    compare(rows(1, 900 * 1024)?, 1000, Some(900 * 1024))?;
    compare(rows(2, 32)?, 1, Some(2 * 1024 * 1024))?;
    compare(rows(0, 0)?, 1000, None)?;
    Ok(())
}
#[test]
fn generated_schema_fragments_match_oracle_with_linear_full_measures() -> TestResult {
    for max in [1000, 7, 1] {
        compare(
            ReplicatedTransaction::SchemaStep {
                commit_id: fleet_sync_protocol::CommitSequence::new(1),
                migration_version: 123456,
                statements: (0..33)
                    .map(|index| format!("-- {index} {}", "\"\\雪🦀\n".repeat(4096)))
                    .collect(),
            },
            max,
            Some(256 * 1024),
        )?;
    }
    compare(
        ReplicatedTransaction::SchemaStep {
            commit_id: fleet_sync_protocol::CommitSequence::new(1),
            migration_version: 9,
            statements: vec!["x".repeat(MAX_UNIT_BYTES + 1)],
        },
        1,
        None,
    )
}
