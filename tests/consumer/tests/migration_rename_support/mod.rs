use super::support::*;
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::{ControlAnswer, ControlRequest, SampleProtocol};
use fleet_sync_transport_websocket::WebSocketConnection;
use fleet_sync_writer::*;
use runtime_primitives::{Clock, SystemClock};
use sqlx_core::{
    executor::Executor,
    migrate::MigrationType,
    query::query,
    row::Row,
    sql_str::{AssertSqlSafe, SqlSafeStr},
};
use sqlx_turso::Turso;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex;
pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const KEY: &str = "019eef00-0000-7000-8000-000000000001";
mod column_journeys;
mod local_history;
mod scenarios;
pub use column_journeys::{ColumnJourney, CopyAvailability, run_column_journey};
pub use scenarios::{Case, dropped_local_case, rename_chain_case, rename_table_case};
use scenarios::{RenameSchema, peer, policy, receiver, schema};
struct Annotation;
impl PieceAnnotation for Annotation {
    type Protocol = SampleProtocol;
    fn annotate(&self, _: &[CapturedTransaction<'_>]) -> Option<String> {
        None
    }
}
struct Transport {
    socket: Mutex<WebSocketConnection>,
    root: PathBuf,
    pieces: AtomicUsize,
}
impl ReplicationTransport<SampleProtocol> for Transport {
    async fn send_piece(
        &self,
        _: ReceiverId,
        piece: Piece<String>,
    ) -> Result<ReplicationAnswer, SendFailure> {
        let index = self.pieces.fetch_add(1, Ordering::SeqCst);
        tokio::fs::write(
            self.root.join(format!("piece-{index:04}.json")),
            serde_json::to_vec(&piece).map_err(send_error)?,
        )
        .await
        .map_err(send_error)?;
        println!("PIECE_SAVED {index} {:?}", piece.through);
        match request(
            &mut *self.socket.lock().await,
            OperationRequest::Replication(ReplicationRequest::Piece(piece)),
        )
        .await
        .map_err(send_error)?
        {
            OperationAnswer::Replication(answer) => Ok(answer),
            _ => Err(SendFailure::AnswerLost),
        }
    }
}
fn send_error(e: impl std::fmt::Display) -> SendFailure {
    SendFailure::Unavailable {
        message: e.to_string(),
    }
}
type Sender = ReplicationSender<RenameSchema, Annotation, (), Transport>;
fn sender(writer: Arc<ReplicatedDatabase<RenameSchema>>, transport: Arc<Transport>) -> Sender {
    ReplicationSender::new(
        writer,
        Arc::new(Annotation),
        Arc::new(()),
        transport,
        SenderOptions {
            max_part_items: std::num::NonZeroUsize::new(100).unwrap_or(std::num::NonZeroUsize::MIN),
            ..Default::default()
        },
    )
}
async fn register(
    socket: &mut WebSocketConnection,
    database: DatabaseId,
) -> TestResult<LeaseStatus> {
    match request(
        socket,
        OperationRequest::Lease(LeaseRequest::Register(Registration {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database,
            writer: peer()?,
            origin: Origin::Empty,
        })),
    )
    .await?
    {
        OperationAnswer::Lease(LeaseAnswer::Register(RegistrationAnswer::Registered {
            lease,
            ..
        })) => Ok(lease),
        _ => Err("registration failed".into()),
    }
}
async fn head(writer: &ReplicatedDatabase<RenameSchema>) -> TestResult<ReplicationPosition> {
    let snapshot = writer.snapshot().await?;
    let pos = snapshot.position();
    snapshot.close().await?;
    Ok(pos)
}
async fn drain(sender: &Sender, id: ReceiverId, through: ReplicationPosition) -> TestResult {
    loop {
        let before = sender
            .copies()
            .await?
            .into_iter()
            .find(|s| s.copy == id)
            .ok_or("registration missing")?
            .acked;
        if before == through {
            return Ok(());
        }
        sender.replicate_once(id).await?;
        let after = sender
            .copies()
            .await?
            .into_iter()
            .find(|s| s.copy == id)
            .ok_or("registration missing")?
            .acked;
        assert!(after > before, "sender made no forward progress");
    }
}
async fn writer_snapshot(
    writer: &ReplicatedDatabase<RenameSchema>,
) -> TestResult<(
    SnapshotManifest,
    Vec<SnapshotPage>,
    BTreeMap<String, Vec<Vec<SqlValue>>>,
)> {
    let mut snap = writer.snapshot().await?;
    let manifest = snap.manifest().await?;
    let mut cursor = None;
    let mut pages = Vec::new();
    let mut rows = BTreeMap::<String, Vec<Vec<SqlValue>>>::new();
    loop {
        let page = snap.page(cursor).await?;
        cursor = page.next.clone();
        rows.entry(page.table.clone())
            .or_default()
            .extend(page.rows.clone());
        pages.push(page);
        if cursor.is_none() {
            break;
        }
    }
    snap.close().await?;
    Ok((manifest, pages, rows))
}
async fn checkpoint(
    root: &Path,
    name: &str,
    writer: &ReplicatedDatabase<RenameSchema>,
    transport: &Transport,
    reader: &mut WebSocketConnection,
) -> TestResult {
    let (manifest, pages, rows) = writer_snapshot(writer).await?;
    let native = copy_rows(reader, writer.database()).await?;
    if name.ends_with("-write") {
        assert_eq!(rows.len(), 1);
        let values = rows
            .values()
            .next()
            .and_then(|rows| rows.first())
            .ok_or("literal row missing")?;
        assert!(matches!(values.first(), Some(SqlValue::Text(id)) if id == KEY));
        assert!(
            matches!(values.get(2), Some(SqlValue::Integer(value)) if *value == if name.starts_with("v1-") { 9_007_199_254_740_993 } else { -9_007_199_254_740_993 })
        );
        assert!(matches!(values.get(3), Some(SqlValue::Blob(value)) if value == &[0,255,128]));
        assert!(matches!(values.get(5), Some(SqlValue::Null)));
    }
    assert_eq!(native, rows, "typed rows diverged at {name}");
    let native_manifest = match request(
        reader,
        OperationRequest::Replication(ReplicationRequest::SnapshotManifest {
            database: writer.database(),
            position: manifest.position,
        }),
    )
    .await?
    {
        OperationAnswer::Replication(ReplicationAnswer::SnapshotManifest(
            SnapshotManifestAnswer::Manifest(manifest),
        )) => manifest,
        _ => return Err("native manifest missing".into()),
    };
    assert_eq!(
        native_manifest.schema, manifest.schema,
        "schema identity diverged {name}"
    );
    assert_eq!(native_manifest.position, manifest.position);
    let mut columns = BTreeMap::<String, Vec<String>>::new();
    for table in rows.keys() {
        let sql = format!("PRAGMA table_info(\"{}\")", table.replace('"', "\"\""));
        let names = query::<Turso>(AssertSqlSafe(sql))
            .fetch_all(writer.reader())
            .await?
            .into_iter()
            .map(|r| r.try_get("name"))
            .collect::<Result<Vec<String>, _>>()?;
        columns.insert(table.clone(), names);
    }
    let count = transport.pieces.load(Ordering::SeqCst);
    let record = serde_json::json!({"name":name,"pieceCount":count,"columns":columns,"typedRows":rows,"schemaIdentity":manifest.schema,"position":manifest.position,"schemaSteps":pages.first().map(|p|&p.schema_steps),"nativeTypedRows":native,"nativeSchemaIdentity":native_manifest.schema});
    tokio::fs::write(
        root.join(format!("checkpoint-{name}.json")),
        serde_json::to_vec_pretty(&record)?,
    )
    .await?;
    println!("CHECKPOINT_PASS {name} pieces={count}");
    Ok(())
}
async fn normal_write(
    writer: &ReplicatedDatabase<RenameSchema>,
    sql: &str,
    ordinal: usize,
) -> TestResult {
    let mut write = writer.begin_write().await?;
    if ordinal == 0 && sql.starts_with("INSERT") {
        query::<Turso>(AssertSqlSafe(sql.to_owned()))
            .bind(KEY)
            .bind("before")
            .bind(9_007_199_254_740_993_i64)
            .execute(&mut write)
            .await?;
    } else {
        query::<Turso>(AssertSqlSafe(sql.to_owned()))
            .bind(format!("after step {}", ordinal + 1))
            .bind(-9_007_199_254_740_993_i64)
            .bind(KEY)
            .execute(&mut write)
            .await?;
    }
    write.commit().await?;
    Ok(())
}
pub async fn run_case(name: &str, case: Case) -> TestResult {
    let temporary = tempfile::tempdir()?;
    let root = std::env::var_os("NORMAL_RUN_ROOT")
        .map_or_else(|| temporary.path().join("runs"), PathBuf::from)
        .join(name);
    tokio::fs::create_dir_all(&root).await?;
    let fixtures = std::env::var_os("NORMAL_FIXTURE_ROOT")
        .map_or_else(|| temporary.path().join("fixtures"), PathBuf::from)
        .join(name);
    tokio::fs::create_dir_all(&fixtures).await?;
    tokio::fs::write(
        fixtures.join("case-status.json"),
        br#"{"status":"started"}"#,
    )
    .await?;
    let process = ServiceProcess::start_with_lease(&root.join("service"), 3, 3_600_000).await?;
    let schema = schema(&case)?;
    let count = schema.count.clone();
    let mut writer = Arc::new(
        ReplicatedDatabase::create(
            &root.join("writer.db"),
            schema.clone(),
            peer()?,
            policy(),
            WriterOptions::default(),
        )
        .await?,
    );
    let mut socket = process
        .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let started = SystemClock.now();
    let lease = register(&mut socket, writer.database()).await?;
    writer.observe_registration(lease.clone(), started).await?;
    tokio::fs::write(
        fixtures.join("create-copy.json"),
        serde_json::to_vec_pretty(&CreateCopy {
            database: writer.database(),
            epoch: lease.epoch,
            holder: Some(peer()?),
            origin: CopyOrigin::Empty,
        })?,
    )
    .await?;
    tokio::fs::write(fixtures.join("sender.json"), serde_json::to_vec(&peer()?)?).await?;
    let transport = Arc::new(Transport {
        socket: Mutex::new(socket),
        root: fixtures.clone(),
        pieces: AtomicUsize::new(0),
    });
    let mut sender = sender(writer.clone(), transport.clone());
    sender
        .add_copy(
            receiver(3)?,
            CopyRole::Authoritative,
            ReplicationPosition::new(lease.epoch, CommitSequence::new(0)),
        )
        .await?;
    let mut reader = process
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let mut control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    if case.local_history {
        control_request(&mut control, ControlRequest::CheckPrivateHistoryOnApply).await?;
    }
    for (index, step) in case.steps.iter().enumerate() {
        if case.fresh && index + 1 < case.steps.len() {
            continue;
        }
        if index > 0 && !case.fresh {
            count.store(index + 1, Ordering::SeqCst);
            tokio::fs::write(
                fixtures.join("migration-attempt.json"),
                serde_json::to_vec_pretty(
                    &serde_json::json!({"version":index+1,"sql":step.sql,"coldWriter":case.cold}),
                )?,
            )
            .await?;
            println!("WRITER_MIGRATE_START {}", index + 1);
            if case.cold {
                drop(sender);
                let database = writer.database();
                writer.close().await;
                let path = root.join("writer.db");
                let schema = schema.clone();
                writer = Arc::new(
                    tokio::spawn(async move {
                        ReplicatedDatabase::open_with(
                            &path,
                            database,
                            schema,
                            LeaseHolder::provisional(
                                database,
                                peer().map_err(|e| e.to_string())?,
                                policy(),
                            ),
                            Arc::new(SystemClock),
                            WriterOptions::default(),
                        )
                        .await
                        .map_err(|e| e.to_string())
                    })
                    .await
                    .map_err(|e| format!("writer reopen panic: {e}"))??,
                );
                sender = self::sender(writer.clone(), transport.clone());
            } else {
                let target = writer.clone();
                match tokio::spawn(async move { target.migrate().await }).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        tokio::fs::write(fixtures.join("case-status.json"),serde_json::to_vec_pretty(&serde_json::json!({"status":"writer_refused","version":index+1,"error":e.to_string()}))?).await?;
                        return Err(e.into());
                    }
                    Err(e) => {
                        tokio::fs::write(fixtures.join("case-status.json"),serde_json::to_vec_pretty(&serde_json::json!({"status":"writer_panicked","version":index+1,"error":e.to_string()}))?).await?;
                        return Err(e.into());
                    }
                }
            }
            println!("WRITER_MIGRATE_PASS {}", index + 1);
        }
        if case.local_history {
            if index < 2 {
                let row = query::<Turso>("SELECT value FROM private_history")
                    .fetch_one(writer.reader())
                    .await?;
                assert_eq!(row.try_get::<String, _>("value")?, "private-local-only");
            } else {
                assert!(
                    query::<Turso>("SELECT name FROM sqlite_schema WHERE name='private_history'")
                        .fetch_all(writer.reader())
                        .await?
                        .is_empty()
                );
            }
        }
        save_writer_checkpoint(
            &fixtures,
            &format!("v{}-migration", index + 1),
            &writer,
            &transport,
        )
        .await?;
        println!("RECEIVER_MIGRATION_START {}", index + 1);
        if let Err(error) = drain(&sender, receiver(3)?, head(&writer).await?).await {
            tokio::fs::write(fixtures.join("case-status.json"),serde_json::to_vec_pretty(&serde_json::json!({"status":"native_receiver_failed","version":index+1,"error":error.to_string()}))?).await?;
            return Err(error);
        }
        checkpoint(
            &fixtures,
            &format!("v{}-migration", index + 1),
            &writer,
            &transport,
            &mut reader,
        )
        .await?;
        if case.local_history {
            local_history::check_copy_and_record(
                &mut control,
                &fixtures,
                &format!("v{}-migration", index + 1),
            )
            .await?;
        }
        normal_write(&writer, step.write, index).await?;
        save_writer_checkpoint(
            &fixtures,
            &format!("v{}-write", index + 1),
            &writer,
            &transport,
        )
        .await?;
        drain(&sender, receiver(3)?, head(&writer).await?).await?;
        checkpoint(
            &fixtures,
            &format!("v{}-write", index + 1),
            &writer,
            &transport,
            &mut reader,
        )
        .await?;
        if case.local_history {
            local_history::check_copy_and_record(
                &mut control,
                &fixtures,
                &format!("v{}-write", index + 1),
            )
            .await?;
        }
    }
    if case.local_history {
        local_history::check_received_pieces(&fixtures).await?;
    }
    tokio::fs::write(fixtures.join("case-status.json"), br#"{"status":"passed"}"#).await?;
    process.stop(&mut control).await?;
    writer.close().await;
    Ok(())
}
async fn save_writer_checkpoint(
    root: &Path,
    name: &str,
    writer: &ReplicatedDatabase<RenameSchema>,
    transport: &Transport,
) -> TestResult {
    let (manifest, pages, rows) = writer_snapshot(writer).await?;
    let mut columns = BTreeMap::<String, Vec<String>>::new();
    for table in rows.keys() {
        let sql = format!("PRAGMA table_info(\"{}\")", table.replace('"', "\"\""));
        let names = query::<Turso>(AssertSqlSafe(sql))
            .fetch_all(writer.reader())
            .await?
            .into_iter()
            .map(|r| r.try_get("name"))
            .collect::<Result<Vec<String>, _>>()?;
        columns.insert(table.clone(), names);
    }
    let record = serde_json::json!({"name":name,"pieceCountBeforeDelivery":transport.pieces.load(Ordering::SeqCst),"columns":columns,"typedRows":rows,"schemaIdentity":manifest.schema,"position":manifest.position,"schemaSteps":pages.first().map(|p|&p.schema_steps)});
    tokio::fs::write(
        root.join(format!("writer-checkpoint-{name}.json")),
        serde_json::to_vec_pretty(&record)?,
    )
    .await?;
    Ok(())
}
