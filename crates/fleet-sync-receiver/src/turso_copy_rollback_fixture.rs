//! Public AtomicStore callbacks exercise the receiver's rollback carrier.
#![allow(clippy::panic_in_result_fn)]
use crate::*;
use durable_steps::{
    AtomicStore, CallFailure, CallId, CallTarget, Host, StoreError as AtomicError,
    storage::TransactionScope,
    turso_store::{TursoScope, TursoStore},
};
use fleet_sync_protocol::*;
use runtime_primitives::{Clock, IdSource, WakeFailure, WakeScheduler};
use sqlx_core::{row::Row, sql_str::SqlSafeStr};
use sqlx_turso::Turso;
use std::sync::Arc;

pub(super) use crate::turso_rule_fixture::{database, piece, position, writer};
pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
pub(super) struct TestHost;
impl Clock for TestHost {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(0)
    }
}
impl IdSource for TestHost {
    fn new_id(&self) -> uuid::Uuid {
        uuid::Uuid::from_u128(0x019eef00000070008000000000000001)
    }
}
impl WakeScheduler for TestHost {
    async fn wake_at(&self, _: SyncTime) -> Result<(), WakeFailure> {
        Ok(())
    }
}
impl Host for TestHost {
    async fn dispatch<C: CallTarget>(
        &self,
        _: C,
        _: C::Request,
        _: CallId,
    ) -> Result<C::Answer, CallFailure> {
        Err(CallFailure::AnswerLost)
    }
}

#[derive(Clone, Copy)]
pub(super) enum HookMode {
    Accept,
    RefuseAfterWrite,
}
pub(super) fn hook_refusal() -> TestResult<HookRefusal> {
    Ok(HookRefusal {
        code: HookRefusalCode::new("policy")?,
        detail: "refused after business and hook writes".into(),
    })
}
impl ReceiverRules<TursoScope<'_, TestHost>> for HookMode {
    type Protocol = NoConsumer;
    type FollowUp = ();
    fn local_migrations(&self) -> &'static [sqlx_core::migrate::Migration] {
        static MIGRATIONS: std::sync::LazyLock<Vec<sqlx_core::migrate::Migration>> =
            std::sync::LazyLock::new(|| {
                vec![sqlx_core::migrate::Migration::new(
                    1,
                    "hook writes".into(),
                    sqlx_core::migrate::MigrationType::Simple,
                    "CREATE TABLE hook_records(value TEXT PRIMARY KEY)".into_sql_str(),
                    false,
                )]
            });
        &MIGRATIONS
    }
    async fn on_applied(
        &self,
        scope: &mut TursoScope<'_, TestHost>,
        applied: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<()>, HookError> {
        sqlx_core::query::query::<Turso>("INSERT INTO hook_records(value) VALUES(?)")
            .bind(applied.through.sequence.value().to_string())
            .execute(scope.executor())
            .await
            .map_err(|error| HookRefusal {
                code: crate::turso_rule_fixture::fixture_refusal_code("storage"),
                detail: error.to_string(),
            })?;
        match self {
            Self::Accept => Ok(Vec::new()),
            Self::RefuseAfterWrite => Err(HookRefusal {
                code: crate::turso_rule_fixture::fixture_refusal_code("policy"),
                detail: "refused after business and hook writes".into(),
            }
            .into()),
        }
    }
}
impl FollowUpHandler for HookMode {
    type Item = ();
    async fn follow_up(&self, _: &(), _: CallId) -> Result<FollowUpDone, FollowUpFailure> {
        Ok(FollowUpDone)
    }
}

pub(super) async fn initialized_copy_store() -> TestResult<(tempfile::TempDir, TursoStore<TestHost>)>
{
    let directory = tempfile::tempdir()?;
    let store =
        TursoStore::open_and_migrate(directory.path().join("copy.db"), Arc::new(TestHost)).await?;
    let request = CreateCopy {
        database: database()?,
        epoch: Epoch::new(1)?,
        holder: Some(writer()?),
        origin: CopyOrigin::Empty,
    };
    store
        .transaction(move |scope| {
            Box::pin(async move {
                RECEIVER_MIGRATOR
                    .run_direct(None, scope.executor(), false)
                    .await
                    .map_err(AtomicError::backend)?;
                initialize_hook(scope, &HookMode::Accept)
                    .await
                    .map_err(AtomicError::backend)?;
                create_copy(scope, &request)
                    .await
                    .map_err(AtomicError::backend)?;
                let schema = Piece::<()>::new(PieceContents {
                    database: request.database,
                    epoch: request.epoch,
                    continues_from: ReplicationPosition::new(request.epoch, CommitSequence::new(0)),
                    through: position(0).map_err(AtomicError::backend)?,
                    transactions: vec![ReplicatedTransaction::SchemaStep {
                        commit_id: CommitSequence::new(1),
                        migration_version: 1,
                        statements: vec![
                            "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,body TEXT)".into(),
                        ],
                    }],
                    part: None,
                    annotation: None,
                })
                .map_err(AtomicError::backend)?;
                apply_piece(
                    scope,
                    &schema,
                    &writer().map_err(AtomicError::backend)?,
                    &HookMode::Accept,
                    SyncTime::from_unix_millis(0),
                )
                .await
                .map_err(apply_rollback)?;
                Ok(())
            })
        })
        .await?;
    Ok((directory, store))
}

#[derive(Clone, Copy)]
pub(super) enum BoundaryFault {
    None,
    RefusalRollback,
    CleanupCommit,
}

pub(super) async fn apply_and_answer(
    store: &TursoStore<TestHost>,
    incoming: Piece,
    hook: HookMode,
    fault: BoundaryFault,
) -> TestResult<ReplicationAnswer> {
    let sender = writer()?;
    let result = store
        .transaction(move |scope| {
            Box::pin(async move {
                let decision = apply_piece(
                    scope,
                    &incoming,
                    &sender,
                    &hook,
                    SyncTime::from_unix_millis(100),
                )
                .await;
                let decision = match decision {
                    Ok(decision) => decision,
                    Err(error) => {
                        if matches!(fault, BoundaryFault::RefusalRollback) {
                            // End the actual transaction first, making the store's
                            // required rollback fail rather than retaining the marker.
                            sqlx_core::query::query::<Turso>("ROLLBACK")
                                .execute(scope.executor())
                                .await?;
                        }
                        return Err(apply_rollback(error));
                    }
                };
                Ok(match decision {
                    ApplyDecision::Applied { through, .. } => {
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
    let failure = match result {
        Ok(answer) => return Ok(answer),
        Err(error) => classify_copy_store_error(error),
    };
    let CopyTransactionFailure::RefusedDiscardingParts {
        refusal,
        transaction,
    } = failure
    else {
        return Ok(failure.answer());
    };
    let cleanup = store
        .transaction(move |scope| {
            Box::pin(async move {
                discard_staged_parts(scope, &transaction)
                    .await
                    .map_err(AtomicError::backend)?;
                if matches!(fault, BoundaryFault::CleanupCommit) {
                    // The delete really ran. Roll it back, then require the actual
                    // store commit to fail instead of emitting the saved refusal.
                    sqlx_core::query::query::<Turso>("ROLLBACK")
                        .execute(scope.executor())
                        .await?;
                }
                Ok(())
            })
        })
        .await;
    Ok(match cleanup {
        Ok(()) => ReplicationAnswer::Refused(refusal),
        Err(error) => classify_copy_store_error(error).answer(),
    })
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct StoredState {
    pub(super) records: String,
    pub(super) hooks: String,
    pub(super) receipts: String,
    pub(super) parts: String,
    pub(super) acked: ReplicationPosition,
}
pub(super) async fn read_copy_state(store: &TursoStore<TestHost>) -> TestResult<StoredState> {
    Ok(store.transaction(|scope| Box::pin(async move {
        let row = sqlx_core::query::query::<Turso>(
            "SELECT CAST((SELECT count(*) FROM records) AS TEXT) AS records, CAST((SELECT count(*) FROM hook_records) AS TEXT) AS hooks, CAST((SELECT count(*) FROM _fleet_sync_receipts) AS TEXT) AS receipts, CAST((SELECT count(*) FROM _fleet_sync_staged_parts) AS TEXT) AS parts",
        ).fetch_one(scope.executor()).await?;
        let CopyStatusAnswer::Status(status) = copy_status(scope).await.map_err(AtomicError::backend)? else {
            return Err(AtomicError::backend("missing following copy"));
        };
        Ok(StoredState {
            records: row.try_get("records")?, hooks: row.try_get("hooks")?,
            receipts: row.try_get("receipts")?, parts: row.try_get("parts")?, acked: status.acked,
        })
    })).await?)
}

pub(super) fn broken_parts() -> TestResult<(Piece, Piece, TransactionKey)> {
    let base = piece(1, 0)?;
    let mut whole = base.transactions.first().ok_or("row transaction")?.clone();
    let ReplicatedTransaction::Rows { rows, .. } = &mut whole else {
        return Err("expected rows".into());
    };
    rows.push(RowChange::upsert(
        "records",
        vec![crate::turso_rule_fixture::uuid_key(2)],
        vec![
            crate::turso_rule_fixture::uuid_key(2),
            SqlValue::Text("second".into()),
        ],
    )?);
    let digest = PieceDigest::of_transaction(&whole)?;
    let transaction = TransactionKey {
        epoch: base.epoch,
        commit_id: base.through.sequence,
    };
    let mut first = base.clone();
    first.part = Some(TransactionPart::new(transaction, 0, 2, digest.clone())?);
    first.digest = first.recomputed_digest()?;
    let mut broken = base;
    broken.transactions = vec![ReplicatedTransaction::Rows {
        commit_id: transaction.commit_id,
        rows: Vec::new(),
    }];
    broken.part = Some(TransactionPart::new(transaction, 1, 2, digest)?);
    broken.digest = broken.recomputed_digest()?;
    Ok((first, broken, transaction))
}
