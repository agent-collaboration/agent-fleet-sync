//! W-C18 C15b: receiver decisions survive the real AtomicStore rollback.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    turso_follow_up_fixture::*,
    turso_seed_hook_tests::{seed_identity_state, staged_seed},
    *,
};
use durable_steps::{
    AtomicStore, OperationFailure, StoreError as AtomicError, storage::TransactionScope,
    turso_store::TursoStore,
};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::sync::{Arc, atomic::Ordering};

async fn complete_seed(
    store: &TursoStore<TestHost>,
    hook: Arc<Hook>,
    done: SeedComplete,
    fail_rollback: bool,
) -> ReplicationAnswer {
    let result = store
        .transaction(move |scope| {
            Box::pin(async move {
                let decision = seed_complete(scope, &done, &*hook, &SeedAuthority::Admin).await;
                let decision = match decision {
                    Ok(decision) => decision,
                    Err(error) => {
                        if fail_rollback {
                            // The real store must fail its required second ROLLBACK.
                            sqlx_core::query::query::<Turso>("ROLLBACK")
                                .execute(scope.executor())
                                .await?;
                        }
                        return Err(receiver_rollback(error));
                    }
                };
                Ok(match decision {
                    SeedDecision::Published {
                        position,
                        follow_ups,
                    } => {
                        admit_snapshot_follow_ups::<TursoStore<TestHost>, String>(
                            scope,
                            &InstalledSnapshot {
                                database: done.database,
                                position,
                                schema: done.schema,
                            },
                            follow_ups,
                        )
                        .await
                        .map_err(AtomicError::backend)?;
                        SeedAnswer::Published { position }
                    }
                    SeedDecision::Answer(answer) => answer,
                })
            })
        })
        .await;
    match result {
        Ok(answer) => ReplicationAnswer::SeedComplete(answer),
        Err(error) => classify_copy_store_error(error).answer(),
    }
}

fn snapshot_refusal() -> TestResult<HookRefusal> {
    Ok(HookRefusal {
        code: HookRefusalCode::new("snapshot_policy")?,
        detail: "snapshot projection refused".into(),
    })
}

#[tokio::test]
async fn seed_hook_refusal_preserves_answer_and_rolls_back_publication_through_atomic_store()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let host = host(&directory.path().join("consumer.db")).await?;
    let store =
        TursoStore::open_and_migrate(directory.path().join("copy.db"), Arc::clone(&host)).await?;
    let done = staged_seed(&store, Arc::clone(&host.hook)).await?;
    let before = seed_identity_state(&store).await?;
    host.hook.snapshot_refuse.store(true, Ordering::SeqCst);

    assert_eq!(
        complete_seed(&store, Arc::clone(&host.hook), done.clone(), false).await,
        ReplicationAnswer::SeedComplete(SeedAnswer::Hook(snapshot_refusal()?))
    );
    assert_eq!(seed_identity_state(&store).await?, before);
    assert_eq!(host.hook.snapshot_calls.load(Ordering::SeqCst), 1);
    host.hook.snapshot_refuse.store(false, Ordering::SeqCst);
    assert_eq!(
        complete_seed(&store, Arc::clone(&host.hook), done.clone(), false).await,
        ReplicationAnswer::SeedComplete(SeedAnswer::Published {
            position: done.position
        })
    );
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let CopyStatusAnswer::Status(status) =
                    copy_status(scope).await.map_err(AtomicError::backend)?
                else {
                    return Err(AtomicError::backend("seed did not publish"));
                };
                assert_eq!(status.acked, done.position);
                let row = sqlx_core::query::query::<Turso>("SELECT body FROM records")
                    .fetch_one(scope.executor())
                    .await?;
                use sqlx_core::row::Row;
                assert_eq!(row.try_get::<String, _>("body")?, "installed record");
                Ok(())
            })
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn refused_seed_rollback_failure_is_unknown_with_no_publication() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = host(&directory.path().join("consumer.db")).await?;
    let store =
        TursoStore::open_and_migrate(directory.path().join("copy.db"), Arc::clone(&host)).await?;
    let done = staged_seed(&store, Arc::clone(&host.hook)).await?;
    let before = seed_identity_state(&store).await?;
    host.hook.snapshot_refuse.store(true, Ordering::SeqCst);
    let answer = complete_seed(&store, Arc::clone(&host.hook), done, true).await;
    assert!(matches!(answer, ReplicationAnswer::OutcomeUnknown {
        reason: OutcomeUnknownReason::StoreBoundary { detail },
    } if detail.as_str().contains("rollback")));
    assert_eq!(seed_identity_state(&store).await?, before);
    Ok(())
}

#[test]
fn every_typed_receiver_outcome_keeps_the_native_answer() -> TestResult {
    let epoch = Epoch::new(2)?;
    for (error, answer) in [
        (
            ReceiverError::Retired,
            ReplicationAnswer::Refused(PieceRefusal::Retired),
        ),
        (
            ReceiverError::NotWriter { current: None },
            ReplicationAnswer::Refused(PieceRefusal::NotWriter { current: None }),
        ),
        (
            ReceiverError::NotWriter {
                current: Some(epoch),
            },
            ReplicationAnswer::Refused(PieceRefusal::NotWriter {
                current: Some(epoch),
            }),
        ),
        (
            ReceiverError::SchemaUnsupported {
                form: SchemaStatementForm::CreateTrigger,
            },
            ReplicationAnswer::Refused(PieceRefusal::SchemaUnsupported {
                form: SchemaStatementForm::CreateTrigger,
            }),
        ),
        (
            ReceiverError::NotInitialized,
            ReplicationAnswer::CopyUnavailable {
                reason: CopyUnavailableReason::Uninitialized,
            },
        ),
        (
            ReceiverError::Storage(StoreError::Full),
            ReplicationAnswer::CopyUnavailable {
                reason: CopyUnavailableReason::StorageFull,
            },
        ),
    ] {
        assert_eq!(
            classify_copy_store_error(receiver_rollback(error)).answer(),
            answer
        );
    }
    for error in [
        ReceiverError::InvalidInput {
            field: "fixture".into(),
        },
        ReceiverError::FenceConflict,
    ] {
        assert!(matches!(
            classify_copy_store_error(receiver_rollback(error)),
            CopyTransactionFailure::OutcomeUnknown(_)
        ));
    }
    let damaged = StoreError::Corrupt {
        field: "fixture copy".into(),
    };
    assert_eq!(
        classify_copy_store_error(receiver_rollback(ReceiverError::Storage(damaged.clone())))
            .answer(),
        ReplicationAnswer::CopyUnavailable {
            reason: CopyUnavailableReason::Damaged {
                detail: damaged.to_string()
            }
        }
    );
    Ok(())
}

#[test]
fn seed_answer_payloads_round_trip_without_an_error_side_channel() -> TestResult {
    let position = ReplicationPosition::new(Epoch::new(2)?, CommitSequence::new(u64::MAX));
    for answer in [
        SeedAnswer::Published { position },
        SeedAnswer::PositionMismatch {
            expected: position,
            offered: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0)),
        },
        SeedAnswer::Incomplete {
            missing: vec![0, 3, u32::MAX],
        },
        SeedAnswer::DigestMismatch {
            table: TableName::new("records")?,
        },
        SeedAnswer::SchemaMismatch,
        SeedAnswer::Retired,
        SeedAnswer::StaleSeed,
        SeedAnswer::Hook(snapshot_refusal()?),
    ] {
        assert_eq!(
            classify_copy_store_error(receiver_rollback(ReceiverError::SeedRefused {
                answer: answer.clone()
            }))
            .answer(),
            ReplicationAnswer::SeedComplete(answer)
        );
    }
    Ok(())
}

#[test]
fn malformed_seed_markers_and_backend_lookalikes_stay_unknown() -> TestResult {
    let marker = receiver_rollback(ReceiverError::SeedRefused {
        answer: SeedAnswer::Hook(snapshot_refusal()?),
    });
    let AtomicError::StepRollback { failure } = marker else {
        return Err("seed refusal was not carried by StepRollback".into());
    };
    let OperationFailure::Refused { code, detail } = *failure else {
        return Err("seed refusal code missing".into());
    };
    assert!(matches!(
        classify_copy_store_error(AtomicError::backend(format!("{code}: {detail}"))),
        CopyTransactionFailure::OutcomeUnknown(_)
    ));
    for detail in [
        r#"{"kind":"hook","value":{"code":"","detail":"invalid code"}}"#,
        r#"{"kind":"published","value":{"position":{"epoch":0,"sequence":1}}}"#,
        r#"{"kind":"schemaMismatch","extra":true}"#,
        r#"{"kind":"futureSeedOutcome"}"#,
        "not JSON",
    ] {
        let result = classify_copy_store_error(AtomicError::StepRollback {
            failure: Box::new(OperationFailure::Refused {
                code: code.clone(),
                detail: detail.into(),
            }),
        });
        assert!(
            matches!(result.answer(), ReplicationAnswer::OutcomeUnknown {
            reason: OutcomeUnknownReason::StoreBoundary { detail },
        } if detail.as_str().starts_with("invalid persisted field copy rollback reason:"))
        );
    }
    Ok(())
}
