#![allow(clippy::panic_in_result_fn)] // Assertions decide behavior; Result propagates fixture failures.

use crate::{
    ReadPool, StatementForm, StatementGuard, TableLayout, WriteRefusal, WriteTransaction,
    write_transaction::WriterControl,
};
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::{DatabaseId, Epoch, LeaseState, LeaseStatus, PeerId, SyncTime};
use runtime_primitives::Clock;
use sqlx_core::{
    executor::Executor,
    pool::{Pool, PoolOptions},
    query::query,
    row::Row,
    sql_str::SqlSafeStr,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct TestClock(AtomicI64, AtomicBool);
impl Clock for TestClock {
    fn now(&self) -> SyncTime {
        let now = SyncTime::from_unix_millis(self.0.load(Ordering::SeqCst));
        if self.1.swap(false, Ordering::SeqCst) {
            self.0.store(1000, Ordering::SeqCst);
        }
        now
    }
}
struct WriterFixture {
    _directory: tempfile::TempDir,
    writer: Pool<Turso>,
    reader: ReadPool,
    control: Arc<Mutex<WriterControl>>,
    clock: Arc<TestClock>,
}

impl WriterFixture {
    async fn open() -> TestResult<Self> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("writer.db");
        let writer = PoolOptions::<Turso>::new()
            .max_connections(1)
            .after_connect(|connection, _| {
                Box::pin(async move {
                    connection
                        .execute("PRAGMA capture_data_changes_conn('full')")
                        .await?;
                    Ok(())
                })
            })
            .connect_with(
                TursoConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true)
                    .foreign_keys(true),
            )
            .await?;
        writer.execute("CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,slot TEXT UNIQUE,amount INTEGER); INSERT INTO records VALUES('019eef00-0000-7000-8000-000000000001','same',1)").await?;
        let reader = ReadPool::new(
            PoolOptions::<Turso>::new()
                .max_connections(2)
                .connect_with(
                    TursoConnectOptions::new()
                        .filename(&path)
                        .foreign_keys(true),
                )
                .await?,
        );
        let database = DatabaseId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000001,
        ))?;
        let peer = PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000002,
        ))?;
        let mut holder = LeaseHolder::provisional(
            database,
            peer,
            LeasePolicy {
                lease_duration: Duration::from_secs(1),
                renew_interval: Duration::from_millis(100),
            },
        );
        holder.observe_registration(
            LeaseStatus {
                database,
                holder: peer,
                epoch: Epoch::new(1)?,
                state: LeaseState::Held,
                expires_at: SyncTime::from_unix_millis(1000),
                remaining_ms: 1000,
            },
            SyncTime::from_unix_millis(0),
        )?;
        let layout = TableLayout::new(
            "records".into(),
            ["id", "slot", "amount"].map(str::to_owned).to_vec(),
            vec![0],
        )?;
        Ok(Self {
            _directory: directory,
            writer,
            reader,
            control: Arc::new(Mutex::new(WriterControl {
                holder,
                statements: StatementGuard::new(vec![layout]),
                provisional_writes: false,
            })),
            clock: Arc::new(TestClock(AtomicI64::new(10), AtomicBool::new(false))),
        })
    }
    async fn begin(&self) -> Result<WriteTransaction, WriteRefusal> {
        WriteTransaction::begin(
            &self.writer,
            self.control.clone().lock_owned().await,
            self.clock.clone(),
        )
        .await
    }
}

#[tokio::test]
async fn reader_checks_fetch_prepare_and_batches_before_any_mutation() -> TestResult {
    let fixture = WriterFixture::open().await?;
    for statement in [
        "INSERT INTO records VALUES('b','second',2)",
        "UPDATE records SET amount=2",
        "DELETE FROM records",
        "CREATE TABLE unexpected(id INTEGER PRIMARY KEY)",
        "PRAGMA capture_data_changes_conn('off')",
        "PRAGMA foreign_keys=OFF",
        "ATTACH ':memory:' AS extra",
        "BEGIN",
        "COMMIT",
        "SAVEPOINT external",
        "SELECT 1; DELETE FROM records",
        "EXPLAIN SELECT * FROM records",
        "EXPLAIN UPDATE records SET amount=2",
    ] {
        let error = fixture
            .reader
            .execute(statement)
            .await
            .err()
            .ok_or("reader allowed mutation")?;
        assert_eq!(
            WriteRefusal::from_sqlx(error),
            WriteRefusal::StatementRefused {
                form: StatementForm::WriteOnReader
            },
            "{statement}"
        );
    }
    let prepare_error = fixture
        .reader
        .prepare("DELETE FROM records".into_sql_str())
        .await
        .err()
        .ok_or("reader prepared mutation")?;
    assert!(matches!(
        WriteRefusal::from_sqlx(prepare_error),
        WriteRefusal::StatementRefused {
            form: StatementForm::WriteOnReader
        }
    ));
    let row = query::<Turso>("SELECT amount FROM records WHERE id=?")
        .bind("019eef00-0000-7000-8000-000000000001")
        .fetch_one(&fixture.reader)
        .await?;
    assert_eq!(row.try_get::<i64, _>("amount")?, 1);
    fixture
        .reader
        .fetch_all("WITH rows AS (SELECT id FROM records) SELECT * FROM rows")
        .await?;
    assert_eq!(
        fixture
            .reader
            .fetch_all("PRAGMA table_info(records)")
            .await?
            .len(),
        3
    );
    fixture
        .reader
        .fetch_all("PRAGMA foreign_key_list(records)")
        .await?;
    fixture
        .reader
        .fetch_all("PRAGMA index_list(records)")
        .await?;
    Ok(())
}

#[tokio::test]
async fn writer_refuses_escape_classes_before_delegating_but_keeps_dml_and_arguments() -> TestResult
{
    let fixture = WriterFixture::open().await?;
    let mut transaction = fixture.begin().await?;
    for (statement, form) in [
        (
            "CREATE TABLE unexpected(id INTEGER PRIMARY KEY)",
            StatementForm::SchemaOutsideMigrate,
        ),
        (
            "ALTER TABLE records ADD COLUMN extra TEXT",
            StatementForm::SchemaOutsideMigrate,
        ),
        ("DROP TABLE records", StatementForm::SchemaOutsideMigrate),
        (
            "PRAGMA capture_data_changes_conn('off')",
            StatementForm::PragmaWrite,
        ),
        ("PRAGMA foreign_keys=OFF", StatementForm::PragmaWrite),
        ("ATTACH ':memory:' AS extra", StatementForm::Attachment),
        ("DETACH extra", StatementForm::Attachment),
        ("BEGIN", StatementForm::TransactionControl),
        ("COMMIT", StatementForm::TransactionControl),
        ("SAVEPOINT extra", StatementForm::TransactionControl),
        ("RELEASE extra", StatementForm::TransactionControl),
        ("ROLLBACK", StatementForm::TransactionControl),
        (
            "UPDATE records SET amount=999; PRAGMA foreign_keys=OFF",
            StatementForm::PragmaWrite,
        ),
    ] {
        let error = (&mut transaction)
            .execute(statement)
            .await
            .err()
            .ok_or("writer admitted escape")?;
        assert_eq!(
            WriteRefusal::from_sqlx(error),
            WriteRefusal::StatementRefused { form },
            "{statement}"
        );
    }
    let row = query::<Turso>(
        "SELECT amount FROM records WHERE id='019eef00-0000-7000-8000-000000000001'",
    )
    .fetch_one(&mut transaction)
    .await?;
    assert_eq!(
        row.try_get::<i64, _>("amount")?,
        1,
        "a later refusal must prevent the whole SQL batch"
    );
    query::<Turso>("UPDATE records SET amount=? WHERE id=?")
        .bind(9_i64)
        .bind("019eef00-0000-7000-8000-000000000001")
        .persistent(false)
        .execute(&mut transaction)
        .await?;
    (&mut transaction)
        .fetch_all("PRAGMA table_info(records)")
        .await?;
    transaction.commit().await?;
    let row = query::<Turso>(
        "SELECT amount FROM records WHERE id='019eef00-0000-7000-8000-000000000001'",
    )
    .fetch_one(&fixture.reader)
    .await?;
    assert_eq!(row.try_get::<i64, _>("amount")?, 9);
    Ok(())
}

#[tokio::test]
async fn key_assignments_are_refused_while_replace_and_payload_updates_work() -> TestResult {
    let fixture = WriterFixture::open().await?;
    let mut transaction = fixture.begin().await?;
    let error = query::<Turso>(
        "INSERT INTO records VALUES(?,?,?) ON CONFLICT(slot) DO UPDATE SET id=excluded.id",
    )
    .bind("019eef00-0000-7000-8000-000000000003")
    .bind("same")
    .bind(2_i64)
    .execute(&mut transaction)
    .await
    .err()
    .ok_or("key-changing UPSERT was admitted")?;
    assert_eq!(
        WriteRefusal::from_sqlx(error),
        WriteRefusal::KeyChange {
            table: "records".into()
        }
    );
    let row = query::<Turso>("SELECT id,amount FROM records")
        .fetch_one(&mut transaction)
        .await?;
    assert_eq!(
        row.try_get::<String, _>("id")?,
        "019eef00-0000-7000-8000-000000000001"
    );
    (&mut transaction)
        .execute("REPLACE INTO records VALUES('019eef00-0000-7000-8000-000000000003','same',2)")
        .await?;
    (&mut transaction)
        .execute("INSERT OR REPLACE INTO records VALUES('019eef00-0000-7000-8000-000000000004','same',3)")
        .await?;
    (&mut transaction)
        .execute("UPDATE records SET amount=3")
        .await?;
    (&mut transaction).execute("INSERT INTO records VALUES('019eef00-0000-7000-8000-000000000006','same',4) ON CONFLICT(slot) DO UPDATE SET amount=excluded.amount").await?;
    transaction.commit().await?;
    let row = query::<Turso>("SELECT id,amount FROM records")
        .fetch_one(&fixture.reader)
        .await?;
    assert_eq!(
        row.try_get::<String, _>("id")?,
        "019eef00-0000-7000-8000-000000000004"
    );
    assert_eq!(row.try_get::<i64, _>("amount")?, 4);
    Ok(())
}

#[tokio::test]
async fn a_lease_expiring_during_the_transaction_rolls_back_instead_of_committing() -> TestResult {
    let fixture = WriterFixture::open().await?;
    let mut transaction = fixture.begin().await?;
    (&mut transaction)
        .execute("UPDATE records SET amount=2")
        .await?;
    fixture.clock.0.store(1000, Ordering::SeqCst);
    assert!(matches!(
        transaction.commit().await,
        Err(WriteRefusal::LeaseExpired { .. })
    ));
    let row = query::<Turso>("SELECT amount FROM records")
        .fetch_one(&fixture.reader)
        .await?;
    assert_eq!(row.try_get::<i64, _>("amount")?, 1);
    assert!(matches!(
        fixture.begin().await,
        Err(WriteRefusal::LeaseExpired { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn lease_expiry_after_commit_validation_starts_still_rolls_back_rows_and_capture()
-> TestResult {
    let fixture = WriterFixture::open().await?;
    let mut transaction = fixture.begin().await?;
    transaction.execute("UPDATE records SET amount=2").await?;
    // The next read returns the still-valid time, then the clock crosses the
    // lease deadline. Validation must not turn that first check into a grant.
    fixture.clock.1.store(true, Ordering::SeqCst);
    assert!(matches!(
        transaction.commit().await,
        Err(WriteRefusal::LeaseExpired { .. })
    ));
    assert_eq!(
        query::<Turso>("SELECT amount FROM records")
            .fetch_one(&fixture.reader)
            .await?
            .try_get::<i64, _>("amount")?,
        1
    );
    assert!(
        query::<Turso>("SELECT change_id FROM turso_cdc WHERE change_type=0")
            .fetch_all(&fixture.reader)
            .await?
            .is_empty()
    );
    Ok(())
}
