//! Callback SQL failures cross actual receiver writes and owned rollback.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_hook_storage_fixture::*, *};
use durable_steps::{
    AtomicStore, StoreError as AtomicError, storage::TransactionScope, turso_store::TursoStore,
};
use fleet_sync_protocol::*;
use sqlx_core::query::query;
use sqlx_turso::Turso;
use std::sync::Arc;

fn unavailable_full() -> ReplicationAnswer {
    ReplicationAnswer::CopyUnavailable {
        reason: CopyUnavailableReason::StorageFull,
    }
}
fn valid_parts() -> TestResult<(Piece, Piece, TransactionKey)> {
    let mut first = crate::turso_rule_fixture::piece(1, 0)?;
    let transaction = TransactionKey {
        epoch: first.epoch,
        commit_id: first.through.sequence,
    };
    let second_row = RowChange::upsert(
        "records",
        vec![crate::turso_rule_fixture::uuid_key(2)],
        vec![
            crate::turso_rule_fixture::uuid_key(2),
            SqlValue::Text("second record".into()),
        ],
    )?;
    let mut whole = first
        .transactions
        .first()
        .ok_or("missing row transaction")?
        .clone();
    let ReplicatedTransaction::Rows { rows, .. } = &mut whole else {
        return Err("expected rows".into());
    };
    rows.push(second_row.clone());
    let digest = PieceDigest::of_transaction(&whole)?;
    first.part = Some(TransactionPart::new(transaction, 0, 2, digest.clone())?);
    first.digest = first.recomputed_digest()?;
    let mut final_part = first.clone();
    final_part.transactions = vec![ReplicatedTransaction::Rows {
        commit_id: transaction.commit_id,
        rows: vec![second_row],
    }];
    final_part.part = Some(TransactionPart::new(transaction, 1, 2, digest)?);
    final_part.digest = final_part.recomputed_digest()?;
    Ok((first, final_part, transaction))
}
fn count_prefix(state: &[String], prefix: &str) -> usize {
    state
        .iter()
        .filter(|value| value.starts_with(prefix))
        .count()
}

#[tokio::test]
async fn real_full_in_the_library_row_upsert_preserves_the_existing_storage_answer() -> TestResult {
    let (_directory, store) = crate::turso_copy_rollback_fixture::initialized_copy_store().await?;
    initialize_summary(&store).await?;
    let before = stored_state(&store).await?;
    let mut incoming = crate::turso_rule_fixture::piece(1, 0)?;
    let Some(ReplicatedTransaction::Rows { rows, .. }) = incoming.transactions.first_mut() else {
        return Err("missing rows".into());
    };
    *rows = vec![RowChange::upsert(
        "records",
        vec![crate::turso_rule_fixture::uuid_key(1)],
        vec![
            crate::turso_rule_fixture::uuid_key(1),
            SqlValue::Text("x".repeat(128 * 1024)),
        ],
    )?];
    incoming.digest = incoming.recomputed_digest()?;
    store
        .transaction(|scope| {
            Box::pin(async move {
                query::<Turso>("PRAGMA max_page_count=1")
                    .fetch_one(scope.executor())
                    .await?;
                Ok(())
            })
        })
        .await?;
    assert_eq!(
        apply_answer(&store, incoming.clone(), rule(RuleMode::Accept, 1), false).await?,
        unavailable_full()
    );
    assert_eq!(stored_state(&store).await?, before);
    restore_capacity(&store).await?;
    assert_eq!(
        apply_answer(&store, incoming.clone(), rule(RuleMode::Accept, 1), false).await?,
        ReplicationAnswer::Applied {
            through: incoming.through
        }
    );
    Ok(())
}

#[tokio::test]
async fn rule_sql_full_on_final_part_keeps_staging_then_identical_retry_commits_once() -> TestResult
{
    let (_directory, store) = crate::turso_copy_rollback_fixture::initialized_copy_store().await?;
    initialize_summary(&store).await?;
    let (first, final_part, transaction) = valid_parts()?;
    let initial = stored_state(&store).await?;
    assert_eq!(count_prefix(&initial, "row:"), 0);
    assert_eq!(count_prefix(&initial, "receipt:"), 1);
    assert_eq!(
        apply_answer(&store, first.clone(), rule(RuleMode::Accept, 2), false).await?,
        ReplicationAnswer::PartStaged {
            transaction,
            index: 0,
            acked: crate::turso_rule_fixture::position(0)?,
        }
    );
    let before = stored_state(&store).await?;
    assert_eq!(count_prefix(&before, "part:"), 1);
    assert_eq!(
        apply_answer(&store, final_part.clone(), rule(RuleMode::Full, 2), false).await?,
        unavailable_full()
    );
    assert_eq!(
        stored_state(&store).await?,
        before,
        "rows, summary, ack, receipts and earlier part roll back"
    );
    restore_capacity(&store).await?;
    assert_eq!(
        apply_answer(&store, final_part.clone(), rule(RuleMode::Accept, 2), false).await?,
        ReplicationAnswer::Applied {
            through: final_part.through
        }
    );
    let committed = stored_state(&store).await?;
    assert_eq!(count_prefix(&committed, "row:"), 2);
    assert!(
        committed
            .iter()
            .any(|value| value == "row:019eef00-0000-7000-8000-000000000001:commit 1")
    );
    assert!(
        committed
            .iter()
            .any(|value| value == "row:019eef00-0000-7000-8000-000000000002:second record")
    );
    assert!(
        committed
            .iter()
            .any(|value| value == "summary:2:commit 1|second record")
    );
    assert_eq!(count_prefix(&committed, "payload:"), 1);
    assert_eq!(count_prefix(&committed, "receipt:"), 3);
    assert_eq!(count_prefix(&committed, "part:"), 0);
    assert_eq!(count_prefix(&committed, "followup:"), 1);
    let expected_through = final_part.through;
    assert_eq!(
        apply_answer(&store, final_part, rule(RuleMode::Full, 2), false).await?,
        ReplicationAnswer::AlreadyApplied {
            through: expected_through
        }
    );
    assert_eq!(
        apply_answer(&store, first, rule(RuleMode::Full, 2), false).await?,
        ReplicationAnswer::AlreadyApplied {
            through: expected_through
        }
    );
    assert_eq!(stored_state(&store).await?, committed);
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let CopyStatusAnswer::Status(status) =
                    copy_status(scope).await.map_err(AtomicError::backend)?
                else {
                    return Err(AtomicError::backend("missing following copy"));
                };
                assert_eq!(status.acked, expected_through);
                Ok(())
            })
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn rule_sql_full_during_seed_keeps_pages_and_publication_pending_until_retry() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = crate::turso_follow_up_fixture::host(&directory.path().join("consumer.db")).await?;
    let store =
        TursoStore::open_and_migrate(directory.path().join("copy.db"), Arc::clone(&host)).await?;
    let done =
        crate::turso_seed_hook_tests::staged_seed(&store, Arc::clone(&host.receiver_rules)).await?;
    initialize_summary(&store).await?;
    let before = stored_state(&store).await?;
    assert_eq!(count_prefix(&before, "seedpage:"), 1);
    assert_eq!(count_prefix(&before, "row:"), 0);
    assert_eq!(count_prefix(&before, "followup:"), 1);
    assert_eq!(
        seed_answer(&store, done.clone(), rule(RuleMode::Full, 1)).await,
        unavailable_full()
    );
    assert_eq!(stored_state(&store).await?, before);
    restore_capacity(&store).await?;
    assert_eq!(
        seed_answer(&store, done.clone(), rule(RuleMode::Accept, 1)).await,
        ReplicationAnswer::SeedComplete(SeedAnswer::Published {
            position: done.position
        })
    );
    let committed = stored_state(&store).await?;
    assert!(
        committed
            .iter()
            .any(|value| value == "row:019eef00-0000-7000-8000-000000000001:installed record")
    );
    assert!(
        committed
            .iter()
            .any(|value| value == "summary:7:installed record")
    );
    assert_eq!(count_prefix(&committed, "seedpage:"), 0);
    assert_eq!(count_prefix(&committed, "payload:"), 1);
    assert_eq!(count_prefix(&committed, "followup:"), 2);
    assert_eq!(
        seed_answer(&store, done.clone(), rule(RuleMode::Full, 1)).await,
        ReplicationAnswer::SeedComplete(SeedAnswer::Published {
            position: done.position
        })
    );
    assert_eq!(stored_state(&store).await?, committed);
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let seed = crate::seed_storage::SeedStorage(scope)
                    .load(done.database)
                    .await
                    .map_err(AtomicError::backend)?
                    .ok_or_else(|| AtomicError::backend("missing published seed"))?;
                assert_eq!(seed.published, Some(done.clone()));
                let CopyStatusAnswer::Status(status) =
                    copy_status(scope).await.map_err(AtomicError::backend)?
                else {
                    return Err(AtomicError::backend("seed did not become following"));
                };
                assert_eq!(status.acked, done.position);
                Ok(())
            })
        })
        .await?;
    Ok(())
}

type InstallHost = crate::turso_copy_rollback_fixture::TestHost;
async fn staged_install()
-> TestResult<(tempfile::TempDir, TursoStore<InstallHost>, SnapshotInstall)> {
    let directory = tempfile::tempdir()?;
    let store = TursoStore::open_and_migrate(
        directory.path().join("copy.db"),
        Arc::new(crate::turso_copy_rollback_fixture::TestHost),
    )
    .await?;
    let (install, source, page, manifest) = crate::turso_snapshot_install_tests::source()?;
    let staged = install.clone();
    store
        .transaction(move |scope| {
            Box::pin(async move {
                RECEIVER_MIGRATOR
                    .run_direct(None, scope.executor(), false)
                    .await
                    .map_err(AtomicError::backend)?;
                let mut rules = SnapshotInstallRules(scope);
                rules
                    .prepare(&staged, &source)
                    .await
                    .map_err(AtomicError::backend)?;
                rules
                    .stage_page(&staged, 0, None, &page)
                    .await
                    .map_err(AtomicError::backend)?;
                rules
                    .verify(&staged, &manifest)
                    .await
                    .map_err(AtomicError::backend)?;
                Ok(())
            })
        })
        .await?;
    initialize_summary(&store).await?;
    Ok((directory, store, install))
}

async fn ready(
    store: &TursoStore<InstallHost>,
    install: SnapshotInstall,
    rule: Arc<StorageRule>,
) -> Result<bool, AtomicError> {
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let decision = SnapshotInstallRules(scope)
                    .mark_ready(&install, &*rule)
                    .await
                    .map_err(|error| match error {
                        SnapshotInstallError::Storage(error) => {
                            receiver_rollback(ReceiverError::Storage(error))
                        }
                        error => AtomicError::backend(error),
                    })?;
                match decision {
                    SnapshotReady::Ready {
                        installed,
                        follow_ups,
                    } => {
                        admit_snapshot_follow_ups::<TursoStore<InstallHost>, String>(
                            scope, &installed, follow_ups,
                        )
                        .await
                        .map_err(AtomicError::backend)?;
                        Ok(true)
                    }
                    SnapshotReady::AlreadyReady { .. } => Ok(false),
                }
            })
        })
        .await
}

#[tokio::test]
async fn rule_sql_full_during_install_ready_preserves_marker_then_retry_admits_once() -> TestResult
{
    let (_directory, store, install) = staged_install().await?;
    let before = stored_state(&store).await?;
    assert_eq!(count_prefix(&before, "row:"), 2);
    assert_eq!(count_prefix(&before, "installpage:"), 1);
    let failure = ready(&store, install.clone(), rule(RuleMode::Full, 2))
        .await
        .err()
        .ok_or("full ready unexpectedly committed")?;
    assert_eq!(
        classify_copy_store_error(failure).answer(),
        unavailable_full()
    );
    assert_eq!(stored_state(&store).await?, before);
    restore_capacity(&store).await?;
    assert!(ready(&store, install.clone(), rule(RuleMode::Accept, 2)).await?);
    let committed = stored_state(&store).await?;
    assert_eq!(count_prefix(&committed, "summary:"), 1);
    assert_eq!(count_prefix(&committed, "payload:"), 1);
    assert_eq!(count_prefix(&committed, "followup:"), 1);
    assert_eq!(count_prefix(&committed, "row:"), 2);
    assert!(!ready(&store, install.clone(), rule(RuleMode::Full, 2)).await?);
    assert_eq!(stored_state(&store).await?, committed);
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let marker = SnapshotInstallRules(scope)
                    .state()
                    .await
                    .map_err(AtomicError::backend)?
                    .ok_or_else(|| AtomicError::backend("missing install marker"))?;
                assert!(marker.ready && marker.verified);
                let CopyStatusAnswer::Status(status) =
                    copy_status(scope).await.map_err(AtomicError::backend)?
                else {
                    return Err(AtomicError::backend("ready copy is not following"));
                };
                assert_eq!(status.acked, install.position);
                Ok(())
            })
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn rule_policy_refusal_stays_terminal_and_rolls_back_its_real_writes() -> TestResult {
    let (_directory, store) = crate::turso_copy_rollback_fixture::initialized_copy_store().await?;
    initialize_summary(&store).await?;
    let before = stored_state(&store).await?;
    assert_eq!(
        apply_answer(
            &store,
            crate::turso_rule_fixture::piece(1, 0)?,
            rule(RuleMode::Refuse, 1),
            false
        )
        .await?,
        ReplicationAnswer::Refused(PieceRefusal::Hook(policy_refusal()))
    );
    assert_eq!(stored_state(&store).await?, before);
    Ok(())
}

#[tokio::test]
async fn rule_refusal_with_failed_store_rollback_stays_unknown() -> TestResult {
    let (_directory, store) = crate::turso_copy_rollback_fixture::initialized_copy_store().await?;
    initialize_summary(&store).await?;
    let before = stored_state(&store).await?;
    assert!(matches!(
        apply_answer(
            &store,
            crate::turso_rule_fixture::piece(1, 0)?,
            rule(RuleMode::Refuse, 1),
            true
        )
        .await?,
        ReplicationAnswer::OutcomeUnknown {
            reason: OutcomeUnknownReason::StoreBoundary { .. }
        }
    ));
    assert_eq!(stored_state(&store).await?, before);
    Ok(())
}
