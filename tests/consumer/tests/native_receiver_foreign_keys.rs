//! Real writer CDC is replayed through the receiver's real Turso transaction.
use durable_steps::storage::TransactionScope;
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::*;
use fleet_sync_receiver::{AppliedPiece, ApplyDecision, ApplyFailure, HookError, ReceiverRules};
use fleet_sync_test_consumer::TestResult;
use fleet_sync_writer::*;
use runtime_primitives::{Clock, SystemClock};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::sync::Mutex;

static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./fk-integrity-migrations");
static TABLES: LazyLock<Result<Vec<TableName>, TextValueError>> = LazyLock::new(|| {
    ["a_child", "z_parent"]
        .into_iter()
        .map(TableName::new)
        .collect()
});
struct Schema {
    tables: &'static [TableName],
}
impl ReplicatedSchema for Schema {
    fn migrations(&self) -> &'static [Migration] {
        MIGRATIONS.iter().as_slice()
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}
struct Scope<'scope>(&'scope mut TursoConnection);
impl TransactionScope for Scope<'_> {
    type Database = Turso;
    type Executor<'query>
        = &'query mut TursoConnection
    where
        Self: 'query;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}
struct Rules;
impl ReceiverRules<Scope<'_>> for Rules {
    type Protocol = NoConsumer;
    type FollowUp = ();
    async fn on_applied(
        &self,
        _: &mut Scope<'_>,
        _: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<()>, HookError> {
        Ok(Vec::new())
    }
}
struct CopyState {
    connection: TursoConnection,
    pieces: Vec<Piece>,
}
struct Transport {
    state: Mutex<CopyState>,
    sender: PeerId,
}
fn unavailable(error: impl std::fmt::Display) -> SendFailure {
    SendFailure::Unavailable {
        message: error.to_string(),
    }
}
impl ReplicationTransport<NoConsumer> for Transport {
    async fn send_piece(
        &self,
        _: ReceiverId,
        piece: Piece,
    ) -> Result<ReplicationAnswer, SendFailure> {
        let mut state = self.state.lock().await;
        state.pieces.push(piece.clone());
        let mut transaction = state
            .connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(unavailable)?;
        let result = fleet_sync_receiver::apply_piece(
            &mut Scope(&mut transaction),
            &piece,
            &self.sender,
            &Rules,
            SyncTime::from_unix_millis(0),
        )
        .await;
        match result {
            Ok(decision) => {
                transaction.commit().await.map_err(unavailable)?;
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
            }
            Err(error) => {
                transaction.rollback().await.map_err(unavailable)?;
                match error {
                    ApplyFailure::Refused(refusal) => Ok(ReplicationAnswer::Refused(refusal)),
                    error => Err(unavailable(error)),
                }
            }
        }
    }
}
#[tokio::test]
async fn captured_child_before_parent_and_parent_first_delete_replay_without_reordering()
-> TestResult {
    let root = tempfile::tempdir()?;
    let peer = PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000002,
    ))?;
    let copy = ReceiverId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000003,
    ))?;
    let writer = Arc::new(
        ReplicatedDatabase::create(
            &root.path().join("writer.db"),
            Schema {
                tables: TABLES.as_ref().map_err(Clone::clone)?,
            },
            peer,
            LeasePolicy {
                lease_duration: Duration::from_secs(600),
                renew_interval: Duration::from_secs(30),
            },
            WriterOptions::default(),
        )
        .await?,
    );
    let now = SystemClock.now();
    writer
        .observe_registration(
            LeaseStatus {
                database: writer.database(),
                holder: peer,
                epoch: Epoch::new(1)?,
                state: LeaseState::Held,
                expires_at: now.after(Duration::from_secs(600)),
                remaining_ms: 600_000,
            },
            now,
        )
        .await?;
    let mut connection = TursoConnectOptions::new()
        .foreign_keys(false)
        .connect()
        .await?;
    fleet_sync_receiver::RECEIVER_MIGRATOR
        .run_direct(None, &mut connection, false)
        .await?;
    let start = ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0));
    let create = CreateCopy {
        database: writer.database(),
        epoch: Epoch::new(1)?,
        holder: Some(peer),
        origin: CopyOrigin::Empty,
    };
    fleet_sync_receiver::create_copy(&mut Scope(&mut connection), &create).await?;
    let transport = Arc::new(Transport {
        state: Mutex::new(CopyState {
            connection,
            pieces: Vec::new(),
        }),
        sender: peer,
    });
    let sender = ReplicationSender::new(
        writer.clone(),
        Arc::new(()),
        Arc::new(()),
        transport.clone(),
        SenderOptions::default(),
    );
    sender
        .add_copy(copy, CopyRole::Authoritative, start)
        .await?;
    for sql in [
        "INSERT INTO a_child VALUES('019eef00-0000-7000-8000-000000000001','019eef00-0000-7000-8000-000000000002'); INSERT INTO z_parent VALUES('019eef00-0000-7000-8000-000000000002')",
        "DELETE FROM z_parent; DELETE FROM a_child",
    ] {
        let mut write = writer.begin_write().await?;
        (&mut write)
            .execute(sqlx::AssertSqlSafe(sql))
            .await
            .map_err(|error| {
                format!("captured writer transaction failed before receiver replay: {error}")
            })?;
        write.commit().await?;
        let snapshot = writer.snapshot().await?;
        let through = snapshot.position();
        snapshot.close().await?;
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                sender.replicate_once(copy).await?;
                if sender
                    .copies()
                    .await?
                    .first()
                    .is_some_and(|status| status.acked == through)
                {
                    break;
                }
            }
            Ok::<(), SenderError>(())
        })
        .await??;
    }
    {
        let mut state = transport.state.lock().await;
        let captured = state
            .pieces
            .iter()
            .flat_map(|piece| &piece.transactions)
            .filter_map(|transaction| match transaction {
                ReplicatedTransaction::Rows { rows, .. } => Some(
                    rows.iter()
                        .map(|row| match row {
                            RowChange::Upsert { table, .. } | RowChange::Delete { table, .. } => {
                                table.as_str()
                            }
                        })
                        .collect::<Vec<_>>(),
                ),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            captured,
            vec![vec!["a_child", "z_parent"], vec!["z_parent", "a_child"]]
        );
        let remaining: i64 = sqlx_core::query_scalar::query_scalar::<Turso, i64>(
            "SELECT (SELECT COUNT(*) FROM a_child)+(SELECT COUNT(*) FROM z_parent)",
        )
        .fetch_one(&mut state.connection)
        .await?;
        assert_eq!(remaining, 0);
        if let Some(path) = std::env::var_os("FK_CAPTURE_FIXTURE") {
            std::fs::write(
                path,
                serde_json::to_vec_pretty(
                    &serde_json::json!({"create":create,"sender":peer,"pieces":state.pieces}),
                )?,
            )?;
        }
    }
    writer.close().await;
    Ok(())
}
