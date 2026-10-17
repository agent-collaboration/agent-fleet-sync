use super::scenarios::Step;
use super::*;

#[derive(Clone, Copy, Debug)]
pub enum ColumnJourney {
    Replace,
    Release,
}
#[derive(Clone, Copy, Debug)]
pub enum CopyAvailability {
    Live,
    Offline,
}
const SECOND_KEY: &str = "019eef00-0000-7000-8000-000000000002";
const CREATE: &str = "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, body TEXT, amount INTEGER, payload BLOB, ratio REAL, missing TEXT, c1 TEXT); CREATE INDEX records_body ON records(body)";
const BEFORE_WRITE: &str = "INSERT INTO records(id,body,amount,payload,ratio,missing,c1) VALUES(?,?,?,X'00FF80',1.25,NULL,'before column')";
const AFTER_WRITE: &str = "UPDATE records SET body=?,amount=? WHERE id=?";
const REPLACE: &str = "ALTER TABLE records ADD COLUMN c2 TEXT; UPDATE records SET c2=upper(c1) WHERE id='019eef00-0000-7000-8000-000000000001'; INSERT INTO records(id,body,amount,payload,ratio,missing,c1,c2) VALUES('019eef00-0000-7000-8000-000000000002','inserted migration',17,X'1020',2.5,NULL,'added','ADDED'); ALTER TABLE records DROP COLUMN c1";
const ADD: &str = "ALTER TABLE records ADD COLUMN c2 TEXT; UPDATE records SET c2=upper(c1) WHERE id='019eef00-0000-7000-8000-000000000001'";
const DROP: &str = "ALTER TABLE records DROP COLUMN c1";

fn scenario(journey: ColumnJourney) -> Case {
    let statements = match journey {
        ColumnJourney::Replace => vec![CREATE, REPLACE],
        ColumnJourney::Release => vec![CREATE, ADD, DROP],
    };
    Case {
        steps: statements
            .into_iter()
            .map(|sql| Step {
                sql,
                tables: vec!["records"],
                write: AFTER_WRITE,
            })
            .collect(),
        cold: false,
        fresh: false,
        local_history: false,
    }
}

fn expected_rows(
    journey: ColumnJourney,
    after_write: bool,
    intermediate: bool,
) -> BTreeMap<String, Vec<Vec<SqlValue>>> {
    let mut first = vec![
        SqlValue::Text(KEY.into()),
        SqlValue::Text(
            if after_write {
                "after step 2"
            } else {
                "before"
            }
            .into(),
        ),
        SqlValue::Integer(if after_write {
            -9_007_199_254_740_993
        } else {
            9_007_199_254_740_993
        }),
        SqlValue::Blob(vec![0, 255, 128]),
        SqlValue::Real(1.25),
        SqlValue::Null,
    ];
    if intermediate {
        first.push(SqlValue::Text("before column".into()));
    }
    first.push(SqlValue::Text("BEFORE COLUMN".into()));
    let mut rows = vec![first];
    if matches!(journey, ColumnJourney::Replace) {
        rows.push(vec![
            SqlValue::Text(SECOND_KEY.into()),
            SqlValue::Text("inserted migration".into()),
            SqlValue::Integer(17),
            SqlValue::Blob(vec![16, 32]),
            SqlValue::Real(2.5),
            SqlValue::Null,
            SqlValue::Text("ADDED".into()),
        ]);
    }
    BTreeMap::from([("records".into(), rows)])
}

async fn assert_columns_and_index(writer: &ReplicatedDatabase<RenameSchema>) -> TestResult {
    let columns = query::<Turso>("PRAGMA table_info(records)")
        .fetch_all(writer.reader())
        .await?
        .into_iter()
        .map(|row| row.try_get::<String, _>("name"))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        columns,
        ["id", "body", "amount", "payload", "ratio", "missing", "c2"]
    );
    let index_columns = query::<Turso>("PRAGMA index_info(records_body)")
        .fetch_all(writer.reader())
        .await?
        .into_iter()
        .map(|row| row.try_get::<String, _>("name"))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(index_columns, ["body"]);
    Ok(())
}

async fn assert_piece(
    fixtures: &Path,
    index: usize,
    version: u64,
    previous: ReplicationPosition,
    journey: ColumnJourney,
) -> TestResult<ReplicationPosition> {
    let piece: Piece<String> = serde_json::from_slice(
        &tokio::fs::read(fixtures.join(format!("piece-{index:04}.json"))).await?,
    )?;
    assert_eq!(piece.continues_from, previous);
    assert!(piece.part.is_none());
    let [
        ReplicatedTransaction::SchemaStep {
            migration_version,
            statements,
            ..
        },
    ] = piece.transactions.as_slice()
    else {
        return Err("expected exactly one SchemaStep in the emitted piece".into());
    };
    assert_eq!(*migration_version, version);
    assert_eq!(
        statements.len(),
        match (journey, version) {
            (ColumnJourney::Replace, 2) => 4,
            (ColumnJourney::Release, 2) => 2,
            (ColumnJourney::Release, 3) => 1,
            _ => return Err("unexpected migration version".into()),
        }
    );
    if version == 2 {
        assert!(
            statements
                .first()
                .is_some_and(|sql| sql.contains("ADD COLUMN c2"))
        );
    }
    println!(
        "COLUMN_PIECE_PASS version={version} transactions=1 statements={} index={index}",
        statements.len()
    );
    Ok(piece.through)
}

pub async fn run_column_journey(
    name: &str,
    journey: ColumnJourney,
    availability: CopyAvailability,
) -> TestResult {
    let temporary = tempfile::tempdir()?;
    let root = std::env::var_os("NORMAL_RUN_ROOT")
        .map_or_else(|| temporary.path().join("runs"), PathBuf::from)
        .join(name);
    let fixtures = std::env::var_os("NORMAL_FIXTURE_ROOT")
        .map_or_else(|| temporary.path().join("fixtures"), PathBuf::from)
        .join(name);
    tokio::fs::create_dir_all(&root).await?;
    tokio::fs::create_dir_all(&fixtures).await?;
    let process = ServiceProcess::start_with_lease(&root.join("service"), 3, 3_600_000).await?;
    let case = scenario(journey);
    let schema = schema(&case)?;
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
    normal_write(&writer, BEFORE_WRITE, 0).await?;
    let before = head(&writer).await?;
    drain(&sender, receiver(3)?, before).await?;
    checkpoint(&fixtures, "v1-write", &writer, &transport, &mut reader).await?;
    let before_rows = copy_rows(&mut reader, writer.database()).await?;
    let first_piece = transport.pieces.load(Ordering::SeqCst);
    let process = match availability {
        CopyAvailability::Live => Some(process),
        CopyAvailability::Offline => {
            process.stop(&mut control).await?;
            println!("COLUMN_COPY_OFFLINE {name}");
            None
        }
    };
    println!("COLUMN_WRITER_START {name} {journey:?}");
    schema.count.store(case.steps.len(), Ordering::SeqCst);
    match journey {
        ColumnJourney::Replace => writer.migrate().await?,
        ColumnJourney::Release => {
            drop(sender);
            let database = writer.database();
            writer.close().await;
            writer = Arc::new(
                ReplicatedDatabase::open_with(
                    &root.join("writer.db"),
                    database,
                    schema.clone(),
                    LeaseHolder::provisional(database, peer()?, policy()),
                    Arc::new(SystemClock),
                    WriterOptions::default(),
                )
                .await?,
            );
            sender = super::sender(writer.clone(), transport.clone());
        }
    }
    assert_columns_and_index(&writer).await?;
    assert_eq!(
        writer_snapshot(&writer).await?.2,
        expected_rows(journey, false, false)
    );
    println!(
        "COLUMN_WRITER_PASS {name} versions=2..={}",
        case.steps.len()
    );
    save_writer_checkpoint(&fixtures, "after-migrations", &writer, &transport).await?;
    let process = match process {
        Some(process) => process,
        None => {
            let process =
                ServiceProcess::start_with_lease(&root.join("service"), 3, 3_600_000).await?;
            *transport.socket.lock().await = process
                .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
                .await?;
            reader = process
                .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
                .await?;
            control = process
                .connect("control", "Bearer consumer-control")
                .await?;
            assert_eq!(
                copy_rows(&mut reader, writer.database()).await?,
                before_rows
            );
            println!("COLUMN_COPY_REOPENED_AT_V1 {name}");
            process
        }
    };
    let mut previous = before;
    for (offset, version) in (2..=u64::try_from(case.steps.len())?).enumerate() {
        sender.replicate_once(receiver(3)?).await?;
        assert_eq!(
            transport.pieces.load(Ordering::SeqCst),
            first_piece + offset + 1
        );
        previous =
            assert_piece(&fixtures, first_piece + offset, version, previous, journey).await?;
        if matches!(journey, ColumnJourney::Release) && version == 2 {
            assert_eq!(
                copy_rows(&mut reader, writer.database()).await?,
                expected_rows(journey, false, true)
            );
            println!("COLUMN_INTERMEDIATE_PASS {name} version=2 typed_rows=1");
        }
    }
    assert_eq!(previous, head(&writer).await?);
    checkpoint(
        &fixtures,
        "after-migrations",
        &writer,
        &transport,
        &mut reader,
    )
    .await?;
    normal_write(&writer, AFTER_WRITE, 1).await?;
    drain(&sender, receiver(3)?, head(&writer).await?).await?;
    checkpoint(&fixtures, "v2-write", &writer, &transport, &mut reader).await?;
    assert_eq!(
        copy_rows(&mut reader, writer.database()).await?,
        expected_rows(journey, true, false)
    );
    println!(
        "COLUMN_JOURNEY_PASS {name} copy={availability:?} rows={} schema={}",
        if matches!(journey, ColumnJourney::Replace) {
            2
        } else {
            1
        },
        case.steps.len()
    );
    process.stop(&mut control).await?;
    writer.close().await;
    Ok(())
}
