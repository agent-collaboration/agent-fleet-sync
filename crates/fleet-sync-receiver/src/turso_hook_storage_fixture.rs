//! A rule's actual SQL exhausts only its fixture database's page allowance.
#![allow(clippy::panic_in_result_fn)]
use crate::*;
use durable_steps::{
    AtomicStore, Host, StoreError as AtomicError, storage::TransactionScope,
    turso_store::TursoStore,
};
use fleet_sync_protocol::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::Arc;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy)]
pub(super) enum RuleMode {
    Accept,
    Full,
    Refuse,
}
pub(super) struct StorageRule {
    pub(super) mode: RuleMode,
    pub(super) expected_rows: usize,
}
pub(super) fn policy_refusal() -> HookRefusal {
    HookRefusal {
        code: crate::turso_rule_fixture::fixture_refusal_code("summary_policy"),
        detail: "summary refused after local writes".into(),
    }
}
impl StorageRule {
    async fn project<S: TransactionScope<Database = Turso>>(
        &self,
        scope: &mut S,
        position: ReplicationPosition,
    ) -> Result<Vec<String>, HookError> {
        let items = self.write_summary(scope, position).await?;
        if matches!(self.mode, RuleMode::Refuse) {
            return Err(policy_refusal().into());
        }
        Ok(items)
    }

    async fn write_summary<S: TransactionScope<Database = Turso>>(
        &self,
        scope: &mut S,
        position: ReplicationPosition,
    ) -> Result<Vec<String>, StoreError> {
        let rows = query::<Turso>("SELECT id,body FROM records ORDER BY id")
            .fetch_all(scope.executor())
            .await
            .map_err(classify_sqlx_error)?;
        assert_eq!(
            rows.len(),
            self.expected_rows,
            "receiver rows must reach the rule"
        );
        let bodies = rows
            .iter()
            .map(|row| row.try_get::<String, _>("body"))
            .collect::<Result<Vec<_>, _>>()
            .map_err(classify_sqlx_error)?;
        let summary = bodies.join("|");
        query::<Turso>("INSERT INTO rule_summary(position,summary) VALUES(?1,?2)")
            .bind(position.sequence.value().to_string())
            .bind(summary.clone())
            .execute(scope.executor())
            .await
            .map_err(classify_sqlx_error)?;
        if matches!(self.mode, RuleMode::Full) {
            // The real pager clamps this to the current size, so the next
            // overflow-page allocation must fail without filling the host disk.
            let cap = query::<Turso>("PRAGMA max_page_count=1")
                .fetch_one(scope.executor())
                .await
                .map_err(classify_sqlx_error)?
                .try_get::<i64, _>(0)
                .map_err(classify_sqlx_error)?;
            assert!(cap > 1);
        }
        let insert = query::<Turso>("INSERT INTO rule_payload(value) VALUES(?1)")
            .bind(vec![7_u8; 128 * 1024])
            .execute(scope.executor())
            .await;
        match insert {
            Ok(_) => {
                assert!(
                    !matches!(self.mode, RuleMode::Full),
                    "page cap did not fail the rule SQL"
                );
                Ok(vec![summary])
            }
            Err(error) => {
                let classified = classify_sqlx_error(error);
                assert!(matches!(self.mode, RuleMode::Full));
                assert_eq!(
                    classified,
                    StoreError::Full,
                    "real DatabaseFull must preserve its SQL class"
                );
                Err(classified)
            }
        }
    }
}
impl<S: TransactionScope<Database = Turso>> ReceiverRules<S> for StorageRule {
    type Protocol = NoConsumer;
    type FollowUp = String;
    async fn on_applied(
        &self,
        scope: &mut S,
        applied: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<String>, HookError> {
        self.project(scope, applied.through).await
    }
    async fn on_snapshot_installed(
        &self,
        scope: &mut S,
        installed: &InstalledSnapshot,
    ) -> Result<Vec<String>, HookError> {
        self.project(scope, installed.position).await
    }
}
pub(super) fn rule(mode: RuleMode, expected_rows: usize) -> Arc<StorageRule> {
    Arc::new(StorageRule {
        mode,
        expected_rows,
    })
}
pub(super) async fn initialize_summary<H: Host>(store: &TursoStore<H>) -> TestResult {
    store
        .transaction(|scope| {
            Box::pin(async move {
                sqlx_core::raw_sql::raw_sql(
            "CREATE TABLE rule_summary(position TEXT NOT NULL PRIMARY KEY,summary TEXT NOT NULL); \
             CREATE TABLE rule_payload(value BLOB NOT NULL)"
        ).execute(scope.executor()).await?;
                Ok(())
            })
        })
        .await?;
    Ok(())
}
pub(super) async fn restore_capacity<H: Host>(store: &TursoStore<H>) -> TestResult {
    store
        .transaction(|scope| {
            Box::pin(async move {
                query::<Turso>("PRAGMA max_page_count=100000")
                    .fetch_one(scope.executor())
                    .await?;
                Ok(())
            })
        })
        .await?;
    Ok(())
}
pub(super) async fn apply_answer<H: Host>(
    store: &TursoStore<H>,
    piece: Piece,
    rule: Arc<StorageRule>,
    fail_rollback: bool,
) -> TestResult<ReplicationAnswer> {
    let sender = crate::turso_rule_fixture::writer()?;
    let result = store
        .transaction(move |scope| {
            Box::pin(async move {
                let decision = match apply_piece(
                    scope,
                    &piece,
                    &sender,
                    &*rule,
                    SyncTime::from_unix_millis(1),
                )
                .await
                {
                    Ok(decision) => decision,
                    Err(error) => {
                        if fail_rollback {
                            query::<Turso>("ROLLBACK").execute(scope.executor()).await?;
                        }
                        return Err(apply_rollback(error));
                    }
                };
                Ok(match decision {
                    ApplyDecision::Applied {
                        through,
                        follow_ups,
                    } => {
                        admit_follow_ups::<TursoStore<H>, String>(
                            scope,
                            piece.database,
                            piece.epoch,
                            through,
                            follow_ups,
                        )
                        .await
                        .map_err(AtomicError::backend)?;
                        ReplicationAnswer::Applied { through }
                    }
                    ApplyDecision::AlreadyApplied { through } => {
                        ReplicationAnswer::AlreadyApplied { through }
                    }
                    ApplyDecision::PartStaged {
                        transaction,
                        index,
                        acked,
                    } => ReplicationAnswer::PartStaged {
                        transaction,
                        index,
                        acked,
                    },
                })
            })
        })
        .await;
    Ok(result.unwrap_or_else(|error| classify_copy_store_error(error).answer()))
}
pub(super) async fn seed_answer<H: Host>(
    store: &TursoStore<H>,
    done: SeedComplete,
    rule: Arc<StorageRule>,
) -> ReplicationAnswer {
    let result = store
        .transaction(move |scope| {
            Box::pin(async move {
                let decision = seed_complete(scope, &done, &*rule, &SeedAuthority::Admin)
                    .await
                    .map_err(receiver_rollback)?;
                let answer = match decision {
                    SeedDecision::Published {
                        position,
                        follow_ups,
                    } => {
                        admit_snapshot_follow_ups::<TursoStore<H>, String>(
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
                };
                Ok(ReplicationAnswer::SeedComplete(answer))
            })
        })
        .await;
    result.unwrap_or_else(|error| classify_copy_store_error(error).answer())
}
pub(super) async fn stored_state<H: Host>(store: &TursoStore<H>) -> TestResult<Vec<String>> {
    Ok(store.transaction(|scope| Box::pin(async move {
        let mut state = query::<Turso>(
            "SELECT 'copy:'||record AS value FROM _fleet_sync_copy \
             UNION ALL SELECT 'receipt:'||identity||':'||digest FROM _fleet_sync_receipts \
             UNION ALL SELECT 'part:'||transaction_key||':'||CAST(part_index AS TEXT)||':'||staged_at||':'||payload FROM _fleet_sync_staged_parts \
             UNION ALL SELECT 'seed:'||record FROM _fleet_sync_seed \
             UNION ALL SELECT 'seedpage:'||seed||':'||CAST(page AS TEXT)||':'||digest||':'||payload FROM _fleet_sync_seed_pages \
             UNION ALL SELECT 'seedattempt:'||attempt||':'||record FROM _fleet_sync_seed_attempts \
             UNION ALL SELECT 'install:'||record FROM _fleet_sync_install \
             UNION ALL SELECT 'installpage:'||record FROM _fleet_sync_install_pages \
             UNION ALL SELECT 'summary:'||position||':'||summary FROM rule_summary \
             UNION ALL SELECT 'payload:'||CAST(length(value) AS TEXT) FROM rule_payload \
             UNION ALL SELECT 'followup:'||operation_id||':'||state||':'||hex(input_digest)||':'||hex(input) FROM _fleet_sync_operations \
             UNION ALL SELECT 'schema:'||type||':'||name||':'||coalesce(sql,'') FROM sqlite_schema \
             ORDER BY value"
        ).fetch_all(scope.executor()).await?.into_iter()
            .map(|row| row.try_get::<String, _>("value")).collect::<Result<Vec<_>, _>>()?;
        if query::<Turso>("SELECT name FROM sqlite_schema WHERE type='table' AND name='records'")
            .fetch_optional(scope.executor()).await?.is_some() {
            state.extend(query::<Turso>("SELECT 'row:'||id||':'||body AS value FROM records ORDER BY id")
                .fetch_all(scope.executor()).await?.into_iter()
                .map(|row| row.try_get::<String, _>("value")).collect::<Result<Vec<_>, _>>()?);
        }
        state.sort();
        Ok(state)
    })).await?)
}
