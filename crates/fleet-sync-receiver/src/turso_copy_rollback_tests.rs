//! W-C18: terminal refusals cross only a successful real AtomicStore rollback.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_copy_rollback_fixture::*, *};
use durable_steps::{AtomicStore, OperationFailure, StoreError as AtomicError};
use fleet_sync_protocol::*;

#[tokio::test]
async fn stale_epoch_refusal_crosses_atomic_store_with_its_exact_cutoff() -> TestResult {
    let (_directory, store) = initialized_copy_store().await?;
    let request = FenceRequest {
        database: database()?,
        transfer_id: TransferId::new("019f0000-0000-7000-8000-000000000015".parse()?)?,
        new_epoch: Epoch::new(2)?,
        expect: FenceExpectation {
            epoch: Epoch::new(1)?,
            acked: position(0)?,
        },
        target_schema: Some(SchemaSupport { max_version: 1 }),
        holder: PeerId::new("019f0000-0000-7000-8000-000000000014".parse()?)?,
    };
    let fenced = store
        .transaction(move |scope| {
            Box::pin(async move { fence(scope, &request).await.map_err(AtomicError::backend) })
        })
        .await?;
    assert!(matches!(fenced, FenceResult::Fenced { .. }));
    let before = read_copy_state(&store).await?;
    assert_eq!(
        apply_and_answer(&store, piece(1, 0)?, HookMode::Accept, BoundaryFault::None).await?,
        ReplicationAnswer::Refused(PieceRefusal::StaleEpoch {
            newer: Epoch::new(2)?,
            ended_at: position(0)?,
        })
    );
    assert_eq!(read_copy_state(&store).await?, before);
    Ok(())
}

#[tokio::test]
async fn receipt_conflict_crosses_atomic_store_without_changing_the_copy() -> TestResult {
    let (_directory, store) = initialized_copy_store().await?;
    assert_eq!(
        apply_and_answer(&store, piece(1, 0)?, HookMode::Accept, BoundaryFault::None).await?,
        ReplicationAnswer::Applied {
            through: position(1)?
        }
    );
    let before = read_copy_state(&store).await?;
    let mut conflicting = piece(1, 0)?;
    conflicting.transactions.clear();
    conflicting.digest = conflicting.recomputed_digest()?;
    assert_eq!(
        apply_and_answer(&store, conflicting, HookMode::Accept, BoundaryFault::None).await?,
        ReplicationAnswer::Refused(PieceRefusal::PieceConflict)
    );
    assert_eq!(read_copy_state(&store).await?, before);
    Ok(())
}

#[tokio::test]
async fn hook_refusal_rolls_back_business_hook_position_and_receipts_through_atomic_store()
-> TestResult {
    let (_directory, store) = initialized_copy_store().await?;
    let before = read_copy_state(&store).await?;
    assert_eq!(
        apply_and_answer(
            &store,
            piece(1, 0)?,
            HookMode::RefuseAfterWrite,
            BoundaryFault::None
        )
        .await?,
        ReplicationAnswer::Refused(PieceRefusal::Hook(hook_refusal()?))
    );
    assert_eq!(read_copy_state(&store).await?, before);
    assert_eq!(
        apply_and_answer(&store, piece(1, 0)?, HookMode::Accept, BoundaryFault::None).await?,
        ReplicationAnswer::Applied {
            through: position(1)?
        }
    );
    assert_eq!(
        read_copy_state(&store).await?,
        StoredState {
            records: "1".into(),
            hooks: "2".into(),
            receipts: "2".into(),
            parts: "0".into(),
            acked: position(1)?,
        }
    );
    Ok(())
}

#[tokio::test]
async fn discarding_refusal_cleans_committed_parts_before_answering() -> TestResult {
    let (_directory, store) = initialized_copy_store().await?;
    let (first, broken, transaction) = broken_parts()?;
    assert_eq!(
        apply_and_answer(&store, first, HookMode::Accept, BoundaryFault::None).await?,
        ReplicationAnswer::PartStaged {
            transaction,
            index: 0,
            acked: position(0)?
        }
    );
    let mut before = read_copy_state(&store).await?;
    assert_eq!(before.parts, "1");
    assert_eq!(
        apply_and_answer(&store, broken, HookMode::Accept, BoundaryFault::None).await?,
        ReplicationAnswer::Refused(PieceRefusal::Gap {
            acked: position(0)?
        })
    );
    before.parts = "0".into();
    assert_eq!(read_copy_state(&store).await?, before);
    Ok(())
}

fn unknown_detail(answer: ReplicationAnswer) -> TestResult<String> {
    match answer {
        ReplicationAnswer::OutcomeUnknown {
            reason: OutcomeUnknownReason::StoreBoundary { detail },
        } => Ok(detail.as_str().to_owned()),
        answer => Err(format!("boundary failure became {answer:?}").into()),
    }
}

#[tokio::test]
async fn refused_apply_rollback_failure_stays_unknown() -> TestResult {
    let (_directory, store) = initialized_copy_store().await?;
    let before = read_copy_state(&store).await?;
    let detail = unknown_detail(
        apply_and_answer(
            &store,
            piece(1, 0)?,
            HookMode::RefuseAfterWrite,
            BoundaryFault::RefusalRollback,
        )
        .await?,
    )?;
    assert!(
        detail.contains("rollback"),
        "unexpected rollback failure: {detail}"
    );
    assert_eq!(read_copy_state(&store).await?, before);
    Ok(())
}

#[tokio::test]
async fn refused_piece_cleanup_commit_failure_stays_unknown_and_keeps_staging() -> TestResult {
    let (_directory, store) = initialized_copy_store().await?;
    let (first, broken, _) = broken_parts()?;
    assert!(matches!(
        apply_and_answer(&store, first, HookMode::Accept, BoundaryFault::None).await?,
        ReplicationAnswer::PartStaged { .. }
    ));
    let before = read_copy_state(&store).await?;
    assert_eq!(before.parts, "1");
    let detail = unknown_detail(
        apply_and_answer(
            &store,
            broken,
            HookMode::Accept,
            BoundaryFault::CleanupCommit,
        )
        .await?,
    )?;
    assert!(
        detail.contains("commit"),
        "unexpected cleanup commit failure: {detail}"
    );
    assert_eq!(read_copy_state(&store).await?, before);
    Ok(())
}

#[test]
fn every_terminal_payload_and_discard_key_round_trips_without_a_store_escape() -> TestResult {
    let newer = Epoch::new(2)?;
    let transaction = TransactionKey {
        epoch: Epoch::new(1)?,
        commit_id: CommitSequence::new(u64::MAX),
    };
    let refusals = [
        PieceRefusal::StaleEpoch {
            newer,
            ended_at: position(7)?,
        },
        PieceRefusal::NotWriter {
            current: Some(newer),
        },
        PieceRefusal::NotWriter { current: None },
        PieceRefusal::Gap {
            acked: position(7)?,
        },
        PieceRefusal::PieceConflict,
        PieceRefusal::SchemaUnsupported {
            form: SchemaStatementForm::CreateTrigger,
        },
        PieceRefusal::NeedsSnapshot {
            acked: position(7)?,
        },
        PieceRefusal::Retired,
        PieceRefusal::Hook(hook_refusal()?),
    ];
    for refusal in refusals {
        assert_eq!(
            classify_copy_store_error(apply_rollback(ApplyFailure::Refused(refusal.clone()))),
            CopyTransactionFailure::Refused(refusal.clone())
        );
        let discarding =
            classify_copy_store_error(apply_rollback(ApplyFailure::RefusedDiscardingParts {
                refusal: refusal.clone(),
                transaction,
            }));
        assert_eq!(
            discarding,
            CopyTransactionFailure::RefusedDiscardingParts {
                refusal,
                transaction
            }
        );
        // A convenience conversion cannot claim the required cleanup has committed.
        assert!(matches!(
            discarding.answer(),
            ReplicationAnswer::OutcomeUnknown { .. }
        ));
    }
    for (refusal, reason) in [
        (
            PieceRefusal::NotInitialized,
            CopyUnavailableReason::Uninitialized,
        ),
        (PieceRefusal::CopyFull, CopyUnavailableReason::StorageFull),
    ] {
        assert_eq!(
            classify_copy_store_error(apply_rollback(ApplyFailure::Refused(refusal.clone()))),
            CopyTransactionFailure::Unavailable(reason)
        );
        assert_eq!(
            classify_copy_store_error(apply_rollback(ApplyFailure::RefusedDiscardingParts {
                refusal: refusal.clone(),
                transaction,
            })),
            CopyTransactionFailure::RefusedDiscardingParts {
                refusal,
                transaction
            }
        );
    }
    Ok(())
}

#[test]
fn malformed_unknown_and_lookalike_markers_never_become_terminal_refusals() -> TestResult {
    let valid = apply_rollback(ApplyFailure::Refused(PieceRefusal::PieceConflict));
    let AtomicError::StepRollback { failure } = valid else {
        return Err("terminal refusal was not encoded as StepRollback".into());
    };
    let OperationFailure::Refused { code, detail } = *failure else {
        return Err("missing reserved refusal code".into());
    };
    for error in [
        AtomicError::backend(format!("{code}: {detail}")),
        AtomicError::Corrupt {
            field: code.clone(),
            message: detail.clone(),
        },
        AtomicError::Aborted {
            reason: format!("{code}: {detail}"),
        },
    ] {
        assert!(matches!(
            classify_copy_store_error(error),
            CopyTransactionFailure::OutcomeUnknown(_)
        ));
    }
    for (code, detail) in [
        ("not_a_copy_code".to_owned(), detail),
        (
            code.clone(),
            "{\"kind\":\"pieceConflict\",\"extra\":true}".into(),
        ),
        (code.clone(), "{\"kind\":\"unknownRefusal\"}".into()),
        (code, "not JSON".into()),
    ] {
        let result = classify_copy_store_error(AtomicError::StepRollback {
            failure: Box::new(OperationFailure::Refused { code, detail }),
        });
        let detail = unknown_detail(result.answer())?;
        assert!(
            detail.starts_with("invalid persisted field copy rollback reason:"),
            "not identified as corrupt: {detail}"
        );
    }
    let AtomicError::StepRollback { failure } =
        apply_rollback(ApplyFailure::RefusedDiscardingParts {
            refusal: PieceRefusal::PieceConflict,
            transaction: TransactionKey {
                epoch: Epoch::new(1)?,
                commit_id: CommitSequence::new(9),
            },
        })
    else {
        return Err("discarding refusal was not encoded".into());
    };
    let OperationFailure::Refused { code, .. } = *failure else {
        return Err("missing discarding refusal code".into());
    };
    for detail in [
        r#"{"refusal":{"kind":"pieceConflict"}}"#,
        r#"{"refusal":{"kind":"pieceConflict"},"transaction":{"epoch":0,"commitId":9}}"#,
        r#"{"refusal":{"kind":"pieceConflict"},"transaction":{"epoch":1,"commitId":9,"extra":true}}"#,
        r#"{"refusal":{"kind":"pieceConflict"},"transaction":{"epoch":1,"commitId":9},"extra":true}"#,
    ] {
        let result = classify_copy_store_error(AtomicError::StepRollback {
            failure: Box::new(OperationFailure::Refused {
                code: code.clone(),
                detail: detail.into(),
            }),
        });
        assert!(
            unknown_detail(result.answer())?
                .starts_with("invalid persisted field copy rollback reason:")
        );
    }
    Ok(())
}

#[tokio::test]
async fn malformed_and_lookalike_errors_roll_back_real_callback_writes() -> TestResult {
    use durable_steps::storage::TransactionScope;
    use sqlx_turso::Turso;
    let (_directory, store) = initialized_copy_store().await?;
    let before = read_copy_state(&store).await?;
    let AtomicError::StepRollback { failure } =
        apply_rollback(ApplyFailure::Refused(PieceRefusal::PieceConflict))
    else {
        return Err("missing marker".into());
    };
    let OperationFailure::Refused { code, detail } = *failure else {
        return Err("missing reserved code".into());
    };
    for error in [
        AtomicError::backend(format!("{code}: {detail}")),
        AtomicError::Corrupt {
            field: code.clone(),
            message: detail.clone(),
        },
        AtomicError::StepRollback {
            failure: Box::new(OperationFailure::Refused {
                code: code.clone(),
                detail: "not JSON".into(),
            }),
        },
        AtomicError::StepRollback {
            failure: Box::new(OperationFailure::Refused {
                code: "foreign_code".into(),
                detail,
            }),
        },
    ] {
        let returned: Result<(), AtomicError> = store
            .transaction(move |scope| {
                Box::pin(async move {
                    sqlx_core::query::query::<Turso>(
                        "INSERT INTO hook_records(value) VALUES('rolled back marker')",
                    )
                    .execute(scope.executor())
                    .await?;
                    Err(error)
                })
            })
            .await;
        let error = returned.err().ok_or("callback unexpectedly committed")?;
        unknown_detail(classify_copy_store_error(error).answer())?;
        assert_eq!(read_copy_state(&store).await?, before);
    }
    Ok(())
}
