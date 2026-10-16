#![allow(clippy::panic_in_result_fn)]
use crate::writer_scope::WriterScope;
use crate::*;
use durable_steps::{CallId, Uuid};
use fleet_sync_lease::{LeasePolicy, WriterState};
use fleet_sync_protocol::{
    CopyOrigin, CopyRole, CreateCopy, Epoch, HookRefusalCode, LeaseState, LeaseStatus, NoConsumer,
    PeerId, Piece, ReceiverId, ReplicationAnswer, TableName,
};
use fleet_sync_receiver::{
    AppliedPiece, ApplyDecision, ApplyFailure, FollowUpDone, FollowUpFailure, ReceiverHook,
};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    migrate::Migrator,
    query::query,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static MIGRATIONS: Migrator = sqlx::migrate!("./tests/fixtures/regular");
static LOCAL: Migrator = sqlx::migrate!("./tests/fixtures/sender_local");
static HOOK: Migrator = sqlx::migrate!("./tests/fixtures/copy_hook");
static TABLES: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
    LazyLock::new(|| {
        ["text_records", "composite_records", "integer_records"]
            .into_iter()
            .map(TableName::new)
            .collect()
    });
pub(super) struct SenderSchema {
    tables: &'static [TableName],
}
impl ReplicatedSchema for SenderSchema {
    fn migrations(&self) -> &'static [Migration] {
        MIGRATIONS.iter().as_slice()
    }
    fn local_migrations(&self) -> &'static [Migration] {
        LOCAL.iter().as_slice()
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}

pub(super) struct Marks {
    pub refuse: AtomicBool,
}
impl CaptureBookkeeping for Marks {
    async fn record(
        &self,
        scope: &mut MaintenanceTransaction<'_>,
        captured: &CapturedTransaction<'_>,
    ) -> Result<(), BookkeepingError> {
        let id = format!(
            "{}:{}",
            captured.position.epoch.value(),
            captured.position.sequence.value()
        );
        query::<Turso>("INSERT INTO local_marks(id) VALUES(?)")
            .bind(id)
            .execute(scope)
            .await
            .map_err(|error| BookkeepingError {
                message: error.to_string(),
            })?;
        if self.refuse.load(Ordering::SeqCst) {
            return Err(BookkeepingError {
                message: "refused after local write".into(),
            });
        }
        Ok(())
    }
}

pub(super) struct CopyHook {
    pub refuse: AtomicBool,
    storage_code: HookRefusalCode,
    policy_code: HookRefusalCode,
}
impl ReceiverHook<WriterScope<'_>> for CopyHook {
    type Protocol = NoConsumer;
    type FollowUp = ();
    fn local_migrations(&self) -> &'static [Migration] {
        HOOK.iter().as_slice()
    }
    async fn on_applied(
        &self,
        scope: &mut WriterScope<'_>,
        applied: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<()>, fleet_sync_protocol::HookRefusal> {
        query::<Turso>("INSERT INTO hook_calls(position) VALUES(?)")
            .bind(applied.through.sequence.value().to_string())
            .execute(&mut *scope.0)
            .await
            .map_err(|error| fleet_sync_protocol::HookRefusal {
                code: self.storage_code.clone(),
                detail: error.to_string(),
            })?;
        if self.refuse.load(Ordering::SeqCst) {
            return Err(fleet_sync_protocol::HookRefusal {
                code: self.policy_code.clone(),
                detail: "refused after hook write".into(),
            });
        }
        Ok(Vec::new())
    }
    async fn follow_up(&self, _: &(), _: CallId) -> Result<FollowUpDone, FollowUpFailure> {
        Ok(FollowUpDone)
    }
}

pub(super) struct CopyFile {
    pub connection: Mutex<Option<TursoConnection>>,
    pub hook: CopyHook,
}
pub(super) struct NativeTransport {
    pub writer: Arc<ReplicatedDatabase<SenderSchema>>,
    pub files: BTreeMap<ReceiverId, CopyFile>,
    pub lose_answer: Mutex<Option<ReceiverId>>,
    pub calls: AtomicUsize,
    pub blocked: Mutex<Option<BlockedSend>>,
    peer: PeerId,
}
pub(super) struct BlockedSend {
    pub copy: ReceiverId,
    pub entered: tokio::sync::oneshot::Sender<()>,
    pub resume: tokio::sync::oneshot::Receiver<()>,
}
impl ReplicationTransport<NoConsumer> for NativeTransport {
    async fn send_piece(
        &self,
        copy: ReceiverId,
        piece: Piece,
    ) -> Result<ReplicationAnswer, SendFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let key = format!("{}:{}", piece.epoch.value(), piece.through.sequence.value());
        // This read happens after entering the transport, through a different
        // real connection: assignment and consumer bookkeeping must be visible.
        for statement in [
            "SELECT record FROM _fleet_sync_capture_bookkeeping WHERE commit_id=?",
            "SELECT id FROM local_marks WHERE id=?",
        ] {
            let record = query::<Turso>(statement)
                .bind(&key)
                .fetch_optional(self.writer.reader())
                .await
                .map_err(unavailable)?;
            assert!(record.is_some(), "send preceded committed bookkeeping");
        }
        let blocked = {
            let mut blocked = self.blocked.lock().await;
            if blocked.as_ref().is_some_and(|blocked| blocked.copy == copy) {
                blocked.take()
            } else {
                None
            }
        };
        if let Some(blocked) = blocked {
            let _ = blocked.entered.send(());
            blocked.resume.await.map_err(unavailable)?;
        }
        let file = self
            .files
            .get(&copy)
            .ok_or_else(|| SendFailure::Unavailable {
                message: "unknown fixture copy".into(),
            })?;
        let mut connection = file.connection.lock().await;
        let connection = connection
            .as_mut()
            .ok_or_else(|| SendFailure::Unavailable {
                message: "closed fixture copy".into(),
            })?;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(unavailable)?;
        let result = fleet_sync_receiver::apply_piece(
            &mut WriterScope(&mut transaction),
            &piece,
            &self.peer,
            &file.hook,
            runtime_primitives::Clock::now(&runtime_primitives::SystemClock),
        )
        .await;
        let answer = match result {
            Ok(decision) => {
                transaction.commit().await.map_err(unavailable)?;
                match decision {
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
                }
            }
            Err(error) => {
                transaction.rollback().await.map_err(unavailable)?;
                match error {
                    ApplyFailure::Refused(reason) => ReplicationAnswer::Refused(reason),
                    ApplyFailure::RefusedDiscardingParts {
                        refusal,
                        transaction: staged,
                    } => {
                        let mut transaction = connection
                            .begin_with("BEGIN IMMEDIATE")
                            .await
                            .map_err(unavailable)?;
                        fleet_sync_receiver::discard_staged_parts(
                            &mut WriterScope(&mut transaction),
                            &staged,
                        )
                        .await
                        .map_err(unavailable)?;
                        transaction.commit().await.map_err(unavailable)?;
                        ReplicationAnswer::Refused(refusal)
                    }
                    ApplyFailure::Storage(error) => return Err(unavailable(error)),
                }
            }
        };
        if matches!(
            answer,
            ReplicationAnswer::Applied { .. } | ReplicationAnswer::AlreadyApplied { .. }
        ) {
            let mut lose = self.lose_answer.lock().await;
            if *lose == Some(copy) {
                *lose = None;
                return Err(SendFailure::AnswerLost);
            }
        }
        Ok(answer)
    }
}
fn unavailable(error: impl std::fmt::Display) -> SendFailure {
    SendFailure::Unavailable {
        message: error.to_string(),
    }
}

pub(super) type TestSender = ReplicationSender<SenderSchema, (), Marks, NativeTransport>;
pub(super) struct SenderFixture {
    pub _directory: tempfile::TempDir,
    pub database: Arc<ReplicatedDatabase<SenderSchema>>,
    pub sender: Arc<TestSender>,
    pub transport: Arc<NativeTransport>,
    pub marks: Arc<Marks>,
    pub first: ReceiverId,
    pub second: ReceiverId,
}
impl SenderFixture {
    pub async fn open() -> TestResult<Self> {
        let directory = tempfile::tempdir()?;
        let peer = PeerId::new(Uuid::from_u128(2))?;
        let database = Arc::new(
            ReplicatedDatabase::create(
                &directory.path().join("writer.db"),
                SenderSchema {
                    tables: TABLES.as_ref().map_err(Clone::clone)?,
                },
                peer,
                LeasePolicy {
                    lease_duration: Duration::from_secs(600),
                    renew_interval: Duration::from_secs(10),
                },
                WriterOptions::default(),
            )
            .await?,
        );
        let now = database.clock.now();
        database.control.lock().await.holder.observe_registration(
            LeaseStatus {
                database: database.database(),
                holder: peer,
                epoch: Epoch::new(1)?,
                state: LeaseState::Held,
                expires_at: now.after(Duration::from_secs(600)),
                remaining_ms: 600_000,
            },
            now,
        )?;
        assert!(matches!(
            database.writer_state().await?,
            WriterState::Writing { .. }
        ));
        let first = ReceiverId::new(Uuid::from_u128(3))?;
        let second = ReceiverId::new(Uuid::from_u128(4))?;
        let mut files = BTreeMap::new();
        for copy in [first, second] {
            let mut connection = TursoConnectOptions::new()
                .filename(directory.path().join(format!("{}.db", copy.as_uuid())))
                .create_if_missing(true)
                .foreign_keys(false)
                .connect()
                .await?;
            let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
            let mut migrations = sqlx::migrate!("../fleet-sync-receiver/migrations");
            migrations.dangerous_set_table_name("_fleet_sync_receiver_history");
            migrations
                .run_direct(None, &mut *transaction, false)
                .await?;
            let hook = CopyHook {
                refuse: AtomicBool::new(false),
                storage_code: HookRefusalCode::new("fixture_storage")?,
                policy_code: HookRefusalCode::new("fixture_refused")?,
            };
            fleet_sync_receiver::initialize_hook(&mut WriterScope(&mut transaction), &hook).await?;
            fleet_sync_receiver::create_copy(
                &mut WriterScope(&mut transaction),
                &CreateCopy {
                    database: database.database(),
                    epoch: Epoch::new(1)?,
                    holder: Some(peer),
                    origin: CopyOrigin::Empty,
                },
            )
            .await?;
            transaction.commit().await?;
            files.insert(
                copy,
                CopyFile {
                    connection: Mutex::new(Some(connection)),
                    hook,
                },
            );
        }
        let transport = Arc::new(NativeTransport {
            writer: database.clone(),
            files,
            lose_answer: Mutex::new(None),
            calls: AtomicUsize::new(0),
            blocked: Mutex::new(None),
            peer,
        });
        let marks = Arc::new(Marks {
            refuse: AtomicBool::new(false),
        });
        let sender = Arc::new(ReplicationSender::new(
            database.clone(),
            Arc::new(()),
            marks.clone(),
            transport.clone(),
            SenderOptions {
                max_part_items: std::num::NonZeroUsize::MIN.saturating_add(1),
                ..SenderOptions::default()
            },
        ));
        let start = {
            let mut connection = database.maintenance.acquire().await?;
            crate::writer_file_record::read_file_record(&mut connection)
                .await?
                .ok_or("missing writer")?
                .start
        };
        sender
            .add_copy(first, CopyRole::Authoritative, start)
            .await?;
        sender.add_copy(second, CopyRole::Follower, start).await?;
        Ok(Self {
            _directory: directory,
            database,
            sender,
            transport,
            marks,
            first,
            second,
        })
    }
    pub async fn migrate_copies(&self) -> TestResult {
        for _ in 0..2 {
            for copy in [self.first, self.second] {
                self.sender.replicate_once(copy).await?;
            }
        }
        Ok(())
    }
    pub async fn close(self) -> TestResult {
        self.database.close().await;
        for file in self.transport.files.values() {
            if let Some(connection) = file.connection.lock().await.take() {
                connection.close().await?;
            }
        }
        Ok(())
    }
}
