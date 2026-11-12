//! SQLx migrations inside an already-open store transaction, on any SQL store.
//!
//! The native suite and the Durable Object worker both run [`run`]; each case
//! uses its own history and data tables, so one object holds them all. A case
//! name is pushed only after its assertions pass, and both runners compare the
//! names with this file's `passed.push` calls.
use durable_steps::{
    AtomicStore, StoreError, SyncTime,
    scope_migrations::{
        HistoryFault, MigrationPreflight, MigrationProgress, MigrationRunError,
        MigrationVersionCause, ReceiptRepresentation, SourceFault, history_migrator,
        run_migrations,
    },
    storage::TransactionScope,
};
use runtime_primitives::Clock;
use sqlx_core::{
    arguments::IntoArguments,
    column::ColumnIndex,
    database::Database,
    decode::Decode,
    encode::Encode,
    executor::Executor,
    migrate::{MigrateError, Migration, MigrationType, Migrator},
    query::query,
    row::Row,
    sql_str::{AssertSqlSafe, SqlSafeStr},
    types::Type,
};
use std::{
    borrow::Cow,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
};

/// Starts at a fixed instant and advances 5 ms on every reading, so a receipt's
/// duration is the number of readings between its statement and its insert.
pub(crate) struct SteppingClock(AtomicI64);
impl SteppingClock {
    pub(crate) fn starting_at(unix_millis: i64) -> Arc<Self> {
        Arc::new(Self(AtomicI64::new(unix_millis)))
    }
}
impl Clock for SteppingClock {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(self.0.fetch_add(5, Ordering::SeqCst))
    }
}

/// 2026-11-11 00:00:00 UTC.
const START_MILLIS: i64 = 1_762_819_200_000;

fn migration(version: i64, sql: &'static str) -> Migration {
    Migration::new(
        version,
        Cow::Owned(format!("case migration {version}")),
        MigrationType::Simple,
        sql.into_sql_str(),
        false,
    )
}

fn migrator(table: &'static str, migrations: Vec<Migration>) -> Arc<Migrator> {
    Arc::new(history_migrator(table, migrations))
}

/// A pending-version check the case controls: it records each version SQLx
/// asked to apply and refuses the one named.
struct RecordingPreflight {
    seen: Arc<Mutex<Vec<i64>>>,
    refuse: Option<i64>,
    wait_for: Option<EntryBarrier>,
}
impl<S: Send> MigrationPreflight<S> for RecordingPreflight {
    fn before_apply<'a>(
        &'a mut self,
        _scope: &'a mut S,
        version: i64,
    ) -> durable_steps::BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            if let Some(barrier) = &self.wait_for {
                barrier.wait().await;
            }
            if let Ok(mut seen) = self.seen.lock() {
                seen.push(version);
            }
            if self.refuse == Some(version) {
                return Err(StoreError::Corrupt {
                    field: "case.legacy_rows".into(),
                    message: format!("legacy row refused before {version}"),
                });
            }
            Ok(())
        })
    }
}

/// What one run returned, kept after its transaction rolled back.
#[derive(Debug)]
enum Observed {
    Progress(MigrationProgress),
    Refused(String),
}

fn label(error: &MigrationRunError) -> String {
    match error {
        MigrationRunError::Policy(MigrateError::VersionMismatch(version)) => {
            format!("policy:checksum:{version}")
        }
        MigrationRunError::Policy(MigrateError::VersionMissing(version)) => {
            format!("policy:missing:{version}")
        }
        MigrationRunError::Policy(MigrateError::Dirty(version)) => {
            format!("policy:dirty:{version}")
        }
        MigrationRunError::Policy(other) => format!("policy:other:{other}"),
        MigrationRunError::History(HistoryFault::Record { version, field }) => {
            format!("history:record:{version:?}:{}", field.column())
        }
        MigrationRunError::History(HistoryFault::DuplicateVersion(version)) => {
            format!("history:duplicate:{version}")
        }
        MigrationRunError::History(HistoryFault::LowerHole { version, highest }) => {
            format!("history:lower-hole:{version}:{highest}")
        }
        MigrationRunError::History(HistoryFault::TableName) => "history:table-name".into(),
        MigrationRunError::History(HistoryFault::Storage(_)) => "history:storage".into(),
        MigrationRunError::History(HistoryFault::Source(SourceFault::Empty)) => {
            "source:empty".into()
        }
        MigrationRunError::History(HistoryFault::Source(SourceFault::Unordered { version })) => {
            format!("source:unordered:{version}")
        }
        MigrationRunError::History(HistoryFault::Source(SourceFault::NoTransaction {
            version,
        })) => format!("source:no-tx:{version}"),
        MigrationRunError::AtVersion { version, cause } => match cause {
            MigrationVersionCause::Preflight(StoreError::Corrupt { field, message }) => {
                format!("at:{version}:preflight:{field}:{message}")
            }
            MigrationVersionCause::Preflight(other) => format!("at:{version}:preflight:{other}"),
            MigrationVersionCause::Statement(_) => format!("at:{version}:statement"),
            MigrationVersionCause::Receipt(_) => format!("at:{version}:receipt"),
        },
    }
}

/// The columns a case reads back from one history table.
#[derive(Debug, PartialEq, Eq)]
struct Receipt {
    version: i64,
    installed_on: String,
    success: i64,
    checksum: Vec<u8>,
    execution_time: i64,
}

fn failed(case: &str, detail: impl std::fmt::Debug) -> StoreError {
    StoreError::Backend {
        message: format!("{case}: {detail:?}"),
    }
}

pub(crate) async fn run<St, Db>(store: &St) -> Result<Vec<&'static str>, StoreError>
where
    St: AtomicStore,
    Db: Database,
    for<'transaction> St::Scope<'transaction>: TransactionScope<Database = Db>,
    Db::Arguments: IntoArguments<Db>,
    for<'value> i64: Type<Db> + Encode<'value, Db> + Decode<'value, Db>,
    for<'value> Option<i64>: Type<Db> + Decode<'value, Db>,
    for<'value> String: Type<Db> + Encode<'value, Db> + Decode<'value, Db>,
    for<'value> Option<String>: Type<Db> + Decode<'value, Db>,
    for<'value> Vec<u8>: Type<Db> + Encode<'value, Db> + Decode<'value, Db>,
    for<'value> Option<Vec<u8>>: Type<Db> + Decode<'value, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    let mut passed = Vec::new();
    let clock = SteppingClock::starting_at(START_MILLIS);

    // --- Success, no-op and a later pending version, central receipts.
    {
        let table = "_sqlx_migrations_case_apply";
        let first = migrator(
            table,
            vec![
                migration(1, "CREATE TABLE case_apply_a (id INTEGER PRIMARY KEY)"),
                migration(2, "INSERT INTO case_apply_a (id) VALUES (7)"),
            ],
        );
        let seen = Arc::new(Mutex::new(Vec::new()));
        let outcome = migrate(store, &first, clock.clone(), Clocked, &seen, None).await?;
        let progress = expect_progress("apply", outcome)?;
        check(
            "apply",
            (progress.count, progress.from, progress.to),
            (2, None, 2),
        )?;
        check("apply", take(&seen), vec![1, 2])?;
        let receipts = receipts(store, table).await?;
        // Two readings per version: started, then installed; 5 ms apart.
        check(
            "apply",
            receipts
                .iter()
                .map(|receipt| {
                    (
                        receipt.version,
                        receipt.installed_on.as_str(),
                        receipt.success,
                        receipt.execution_time,
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                (1, "2025-11-11 00:00:00", 1, 5),
                (2, "2025-11-11 00:00:00", 1, 5),
            ],
        )?;
        check(
            "apply",
            receipts
                .iter()
                .map(|receipt| receipt.checksum.clone())
                .collect::<Vec<_>>(),
            first
                .iter()
                .map(|migration| migration.checksum.to_vec())
                .collect::<Vec<_>>(),
        )?;
        check("apply", count(store, "case_apply_a").await?, 1)?;
        // A history created here stores the same schema text as one the
        // journal's or central's own DDL created before this operation existed.
        // The oracle is the backend's own storage of the journal's historical
        // statement (query-schema/99990000_journal_receipts.sql, no IF NOT
        // EXISTS), run under another name on the same store.
        let reference = "_sqlx_migrations_case_reference";
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    scope
                        .executor()
                        .execute(AssertSqlSafe(format!(
                            "CREATE TABLE {reference} (\n    version BIGINT PRIMARY KEY,\n    description TEXT NOT NULL,\n    installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n    success BOOLEAN NOT NULL,\n    checksum BLOB NOT NULL,\n    execution_time BIGINT NOT NULL\n);\n"
                        )))
                        .await?;
                    Ok(())
                })
            })
            .await?;
        check(
            "apply",
            schema_text(store, table).await?,
            schema_text(store, reference)
                .await?
                .replace(reference, table),
        )?;
        passed.push("pending versions apply with clocked receipts");

        let outcome = migrate(store, &first, clock.clone(), Clocked, &seen, None).await?;
        let progress = expect_progress("no-op", outcome)?;
        check(
            "no-op",
            (progress.count, progress.from, progress.to),
            (0, Some(2), 2),
        )?;
        check("no-op", take(&seen), Vec::<i64>::new())?;
        check("no-op", receipts_versions(store, table).await?, vec![1, 2])?;
        passed.push("unchanged history is a no-op");

        let later = migrator(
            table,
            vec![
                migration(1, "CREATE TABLE case_apply_a (id INTEGER PRIMARY KEY)"),
                migration(2, "INSERT INTO case_apply_a (id) VALUES (7)"),
                migration(3, "INSERT INTO case_apply_a (id) VALUES (8)"),
            ],
        );
        let outcome = migrate(store, &later, clock.clone(), Clocked, &seen, None).await?;
        let progress = expect_progress("later", outcome)?;
        check(
            "later",
            (progress.count, progress.from, progress.to),
            (1, Some(2), 3),
        )?;
        check("later", take(&seen), vec![3])?;
        check("later", count(store, "case_apply_a").await?, 2)?;
        passed.push("a later pending version applies alone");
    }

    // --- Two sets each with version 1 keep separate histories.
    {
        let left = migrator(
            "_sqlx_migrations_case_left",
            vec![migration(1, "CREATE TABLE case_left (id INTEGER)")],
        );
        let right = migrator(
            "_sqlx_migrations_case_right",
            vec![migration(1, "CREATE TABLE case_right (id INTEGER)")],
        );
        let seen = Arc::new(Mutex::new(Vec::new()));
        for (name, set) in [("left", &left), ("right", &right)] {
            let progress = expect_progress(
                name,
                migrate(store, set, clock.clone(), Clocked, &seen, None).await?,
            )?;
            check(name, (progress.count, progress.to), (1, 1))?;
        }
        for (name, set) in [("left", &left), ("right", &right)] {
            let progress = expect_progress(
                name,
                migrate(store, set, clock.clone(), Clocked, &seen, None).await?,
            )?;
            check(name, progress.count, 0)?;
        }
        check(
            "left",
            receipts_versions(store, "_sqlx_migrations_case_left").await?,
            vec![1],
        )?;
        check(
            "right",
            receipts_versions(store, "_sqlx_migrations_case_right").await?,
            vec![1],
        )?;
        passed.push("equal versions in two sets keep separate histories");
    }

    // --- Journal receipts: SQL-default time and the -1 unknown duration.
    {
        let table = "_sqlx_migrations_case_journal";
        let set = migrator(
            table,
            vec![migration(1, "CREATE TABLE case_journal (id INTEGER)")],
        );
        let seen = Arc::new(Mutex::new(Vec::new()));
        let progress = expect_progress(
            "journal",
            migrate(store, &set, clock.clone(), StorageDefault, &seen, None).await?,
        )?;
        check("journal", progress.count, 1)?;
        let receipt = receipts(store, table).await?;
        check(
            "journal",
            receipt
                .iter()
                .map(|row| (row.version, row.success, row.execution_time))
                .collect::<Vec<_>>(),
            vec![(1, 1, -1)],
        )?;
        let progress = expect_progress(
            "journal",
            migrate(store, &set, clock.clone(), StorageDefault, &seen, None).await?,
        )?;
        check("journal", progress.count, 0)?;
        passed.push("journal receipts keep the unknown-duration sentinel");
    }

    // --- Refused histories: nothing applies and the stored history is unchanged.
    for (case, table, history, mode, expected) in HISTORY_REFUSALS {
        let set = migrator(
            table,
            vec![
                migration(1, "SELECT 1"),
                migration(2, "SELECT 2"),
                migration(3, "SELECT 3"),
            ],
        );
        let set_checksum = set
            .iter()
            .next()
            .map(|migration| migration.checksum.to_vec())
            .unwrap_or_default();
        seed_history(store, table, history, set_checksum).await?;
        let before = receipts(store, table).await?;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let outcome = migrate(store, &set, clock.clone(), *mode, &seen, None).await?;
        check(case, refused(case, outcome)?, (*expected).to_string())?;
        check(case, take(&seen), Vec::<i64>::new())?;
        check(case, receipts(store, table).await?, before)?;
        passed.push(case);
    }

    // --- Refused sources never reach SQLx.
    for (case, migrations, expected) in [
        (
            "unordered source is refused",
            vec![migration(2, "SELECT 2"), migration(1, "SELECT 1")],
            "source:unordered:1",
        ),
        (
            "duplicate source version is refused",
            vec![migration(1, "SELECT 1"), migration(1, "SELECT 11")],
            "source:unordered:1",
        ),
        (
            "non-transactional source is refused",
            vec![Migration::new(
                1,
                Cow::Borrowed("outside a transaction"),
                MigrationType::Simple,
                "SELECT 1".into_sql_str(),
                true,
            )],
            "source:no-tx:1",
        ),
        ("empty source is refused", Vec::new(), "source:empty"),
    ] {
        let set = migrator("_sqlx_migrations_case_source", migrations);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let outcome = migrate(store, &set, clock.clone(), Clocked, &seen, None).await?;
        check(case, refused(case, outcome)?, expected.to_string())?;
        check(case, take(&seen), Vec::<i64>::new())?;
        passed.push(case);
    }

    // --- A history table outside the fixed identifier form is refused.
    {
        let set = migrator(
            "_sqlx_migrations_case; DROP TABLE case_apply_a",
            vec![migration(1, "SELECT 1")],
        );
        let seen = Arc::new(Mutex::new(Vec::new()));
        let outcome = migrate(store, &set, clock.clone(), Clocked, &seen, None).await?;
        check(
            "table",
            refused("table", outcome)?,
            "history:table-name".to_string(),
        )?;
        check("table", count(store, "case_apply_a").await?, 2)?;
        passed.push("history table name must be a plain identifier");
    }

    // --- Failures at a pending version roll back SQL and receipt together.
    {
        let table = "_sqlx_migrations_case_fault";
        let base = migration(
            1,
            "CREATE TABLE case_fault_base (id INTEGER); INSERT INTO case_fault_base VALUES (1)",
        );
        let seen = Arc::new(Mutex::new(Vec::new()));
        let set = migrator(table, vec![base.clone()]);
        expect_progress(
            "fault base",
            migrate(store, &set, clock.clone(), Clocked, &seen, None).await?,
        )?;
        take(&seen);

        // Preflight refusal: the original typed cause survives, no SQL ran.
        let set = migrator(
            table,
            vec![
                base.clone(),
                migration(2, "CREATE TABLE case_fault_preflight (id INTEGER)"),
            ],
        );
        let outcome = migrate(store, &set, clock.clone(), Clocked, &seen, Some(2)).await?;
        check(
            "preflight",
            refused("preflight", outcome)?,
            "at:2:preflight:case.legacy_rows:legacy row refused before 2".to_string(),
        )?;
        check("preflight", take(&seen), vec![2])?;
        check(
            "preflight",
            table_exists(store, "case_fault_preflight").await?,
            false,
        )?;
        check("preflight", receipts_versions(store, table).await?, vec![1])?;
        passed.push("preflight refusal keeps its original cause and runs no SQL");

        // Statement failure after earlier statements in the same migration.
        let set = migrator(
            table,
            vec![
                base.clone(),
                migration(
                    2,
                    "CREATE TABLE case_fault_statement (id INTEGER); INSERT INTO case_fault_base VALUES (2); INSERT INTO case_fault_missing VALUES (1)",
                ),
            ],
        );
        let outcome = migrate(store, &set, clock.clone(), Clocked, &seen, None).await?;
        check(
            "statement",
            refused("statement", outcome)?,
            "at:2:statement".to_string(),
        )?;
        check(
            "statement",
            table_exists(store, "case_fault_statement").await?,
            false,
        )?;
        check("statement", count(store, "case_fault_base").await?, 1)?;
        check("statement", receipts_versions(store, table).await?, vec![1])?;
        passed.push("statement failure rolls back the migration's earlier writes");

        // Receipt failure after the migration's SQL succeeded: the SQL drops
        // the history table, so its receipt cannot be written.
        let set = migrator(
            table,
            vec![
                base,
                migration(
                    2,
                    "CREATE TABLE case_fault_receipt (id INTEGER); INSERT INTO case_fault_base VALUES (3); DROP TABLE _sqlx_migrations_case_fault",
                ),
            ],
        );
        let outcome = migrate(store, &set, clock.clone(), Clocked, &seen, None).await?;
        check(
            "receipt",
            refused("receipt", outcome)?,
            "at:2:receipt".to_string(),
        )?;
        check(
            "receipt",
            table_exists(store, "case_fault_receipt").await?,
            false,
        )?;
        check("receipt", count(store, "case_fault_base").await?, 1)?;
        check("receipt", receipts_versions(store, table).await?, vec![1])?;
        passed.push("receipt failure rolls back the migration's SQL");
    }

    // --- Concurrent entry: the second run is attempted while the first holds
    // its transaction open (the first's preflight waits for that attempt), and
    // the object still migrates once. The DDL has no IF NOT EXISTS, so a second
    // application would fail rather than pass silently.
    {
        let table = "_sqlx_migrations_case_concurrent";
        let set = migrator(
            table,
            vec![migration(1, "CREATE TABLE case_concurrent (id INTEGER)")],
        );
        let barrier = EntryBarrier::new();
        let first_seen = Arc::new(Mutex::new(Vec::new()));
        let second_seen = Arc::new(Mutex::new(Vec::new()));
        let first = migrate_with(
            store,
            &set,
            clock.clone(),
            Clocked,
            &first_seen,
            None,
            Some(barrier.clone()),
        );
        let second = async {
            barrier.attempted();
            migrate(store, &set, clock.clone(), Clocked, &second_seen, None).await
        };
        let (first, second) = futures_util::future::join(first, second).await;
        let first = expect_progress("concurrent first", first?)?;
        let second = expect_progress("concurrent second", second?)?;
        check("concurrent", (first.count, second.count), (1, 0))?;
        check(
            "concurrent",
            (take(&first_seen), take(&second_seen)),
            (vec![1], Vec::new()),
        )?;
        check(
            "concurrent",
            receipts_versions(store, table).await?,
            vec![1],
        )?;
        passed.push("concurrent entry migrates the object once");
    }

    check("declared cases", passed.as_slice(), CASES)?;
    Ok(passed)
}

/// Every case [`run`] passes, in order. The native test and the workerd runner
/// both compare the returned names with this list.
pub(crate) const CASES: &[&str] = &[
    "pending versions apply with clocked receipts",
    "unchanged history is a no-op",
    "a later pending version applies alone",
    "equal versions in two sets keep separate histories",
    "journal receipts keep the unknown-duration sentinel",
    "changed checksum is refused",
    "unknown applied version is refused",
    "dirty history is refused",
    "a missing version below an applied one is refused",
    "text version storage class is refused",
    "clocked history refuses an unknown duration",
    "clocked history refuses a malformed install time",
    "journal history refuses a text success flag",
    "unordered source is refused",
    "duplicate source version is refused",
    "non-transactional source is refused",
    "empty source is refused",
    "history table name must be a plain identifier",
    "preflight refusal keeps its original cause and runs no SQL",
    "statement failure rolls back the migration's earlier writes",
    "receipt failure rolls back the migration's SQL",
    "concurrent entry migrates the object once",
];

/// A causal barrier: the waiting side resumes only after the other side has
/// attempted its entry. No clock or sleep decides the order.
#[derive(Clone)]
struct EntryBarrier(Arc<Mutex<(bool, Option<std::task::Waker>)>>);
impl EntryBarrier {
    fn new() -> Self {
        Self(Arc::new(Mutex::new((false, None))))
    }
    fn attempted(&self) {
        if let Ok(mut state) = self.0.lock() {
            state.0 = true;
            if let Some(waker) = state.1.take() {
                waker.wake();
            }
        }
    }
    async fn wait(&self) {
        futures_util::future::poll_fn(|context| match self.0.lock() {
            Ok(state) if state.0 => std::task::Poll::Ready(()),
            Ok(mut state) => {
                state.1 = Some(context.waker().clone());
                std::task::Poll::Pending
            }
            Err(_) => std::task::Poll::Ready(()),
        })
        .await;
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Clocked,
    StorageDefault,
}
use Mode::{Clocked, StorageDefault};

/// Rows a refusal case writes into its history before running.
/// `checksum` NULL means "the source's version 1 checksum".
struct SeedRow {
    version: &'static str,
    success: &'static str,
    checksum: Option<&'static str>,
    installed_on: &'static str,
    execution_time: &'static str,
}
const fn seed(version: &'static str) -> SeedRow {
    SeedRow {
        version,
        success: "1",
        checksum: None,
        installed_on: "'2025-11-10 12:00:00'",
        execution_time: "3",
    }
}

#[allow(clippy::type_complexity)]
const HISTORY_REFUSALS: &[(&str, &str, &[SeedRow], Mode, &str)] = &[
    (
        "changed checksum is refused",
        "_sqlx_migrations_case_checksum",
        &[SeedRow {
            checksum: Some("X'00'"),
            ..seed("1")
        }],
        Clocked,
        "policy:checksum:1",
    ),
    (
        "unknown applied version is refused",
        "_sqlx_migrations_case_unknown",
        &[seed("1"), seed("9")],
        Clocked,
        "policy:missing:9",
    ),
    (
        "dirty history is refused",
        "_sqlx_migrations_case_dirty",
        &[SeedRow {
            success: "0",
            ..seed("1")
        }],
        Clocked,
        "policy:dirty:1",
    ),
    (
        "a missing version below an applied one is refused",
        "_sqlx_migrations_case_hole",
        &[seed("1"), seed("3")],
        Clocked,
        "history:lower-hole:2:3",
    ),
    (
        "text version storage class is refused",
        "_sqlx_migrations_case_class",
        &[seed("'one'")],
        Clocked,
        "history:record:None:version",
    ),
    (
        "clocked history refuses an unknown duration",
        "_sqlx_migrations_case_duration",
        &[SeedRow {
            execution_time: "-1",
            ..seed("1")
        }],
        Clocked,
        "history:record:Some(1):execution_time",
    ),
    (
        "clocked history refuses a malformed install time",
        "_sqlx_migrations_case_installed",
        &[SeedRow {
            installed_on: "'yesterday'",
            ..seed("1")
        }],
        Clocked,
        "history:record:Some(1):installed_on",
    ),
    (
        "journal history refuses a text success flag",
        "_sqlx_migrations_case_journal_success",
        &[SeedRow {
            success: "'yes'",
            ..seed("1")
        }],
        StorageDefault,
        "history:record:Some(1):success",
    ),
];

async fn migrate<St, Db>(
    store: &St,
    migrator: &Arc<Migrator>,
    clock: Arc<SteppingClock>,
    mode: Mode,
    seen: &Arc<Mutex<Vec<i64>>>,
    refuse: Option<i64>,
) -> Result<Observed, StoreError>
where
    St: AtomicStore,
    Db: Database,
    for<'transaction> St::Scope<'transaction>: TransactionScope<Database = Db>,
    Db::Arguments: IntoArguments<Db>,
    for<'value> i64: Type<Db> + Encode<'value, Db> + Decode<'value, Db>,
    for<'value> Option<i64>: Type<Db> + Decode<'value, Db>,
    for<'value> String: Type<Db> + Encode<'value, Db> + Decode<'value, Db>,
    for<'value> Option<String>: Type<Db> + Decode<'value, Db>,
    for<'value> Vec<u8>: Type<Db> + Encode<'value, Db> + Decode<'value, Db>,
    for<'value> Option<Vec<u8>>: Type<Db> + Decode<'value, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    migrate_with(store, migrator, clock, mode, seen, refuse, None).await
}

/// Runs `migrator` in one store transaction. The transaction rolls back on a
/// refusal, whose label is returned after the rollback.
async fn migrate_with<St, Db>(
    store: &St,
    migrator: &Arc<Migrator>,
    clock: Arc<SteppingClock>,
    mode: Mode,
    seen: &Arc<Mutex<Vec<i64>>>,
    refuse: Option<i64>,
    wait_for: Option<EntryBarrier>,
) -> Result<Observed, StoreError>
where
    St: AtomicStore,
    Db: Database,
    for<'transaction> St::Scope<'transaction>: TransactionScope<Database = Db>,
    Db::Arguments: IntoArguments<Db>,
    for<'value> i64: Type<Db> + Encode<'value, Db> + Decode<'value, Db>,
    for<'value> Option<i64>: Type<Db> + Decode<'value, Db>,
    for<'value> String: Type<Db> + Encode<'value, Db> + Decode<'value, Db>,
    for<'value> Option<String>: Type<Db> + Decode<'value, Db>,
    for<'value> Vec<u8>: Type<Db> + Encode<'value, Db> + Decode<'value, Db>,
    for<'value> Option<Vec<u8>>: Type<Db> + Decode<'value, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    let refusal = Arc::new(Mutex::new(None));
    let slot = Arc::clone(&refusal);
    let migrator = Arc::clone(migrator);
    let mut preflight = RecordingPreflight {
        seen: Arc::clone(seen),
        refuse,
        wait_for,
    };
    let result = store
        .transaction(move |scope| {
            Box::pin(async move {
                let receipts = match mode {
                    Clocked => ReceiptRepresentation::Clocked(clock.as_ref()),
                    StorageDefault => ReceiptRepresentation::StorageDefault,
                };
                match run_migrations(scope, &migrator, &mut preflight, receipts).await {
                    Ok(progress) => Ok(progress),
                    Err(error) => {
                        if let Ok(mut slot) = slot.lock() {
                            *slot = Some(label(&error));
                        }
                        Err(StoreError::Aborted {
                            reason: "case migration refused".into(),
                        })
                    }
                }
            })
        })
        .await;
    match result {
        Ok(progress) => Ok(Observed::Progress(progress)),
        Err(error) => match refusal.lock().ok().and_then(|mut slot| slot.take()) {
            Some(label) => Ok(Observed::Refused(label)),
            None => Err(error),
        },
    }
}

fn expect_progress(case: &str, observed: Observed) -> Result<MigrationProgress, StoreError> {
    match observed {
        Observed::Progress(progress) => Ok(progress),
        Observed::Refused(label) => Err(failed(case, format!("refused {label}"))),
    }
}

fn refused(case: &str, observed: Observed) -> Result<String, StoreError> {
    match observed {
        Observed::Refused(label) => Ok(label),
        Observed::Progress(progress) => Err(failed(case, format!("applied {progress:?}"))),
    }
}

fn check<T: PartialEq + std::fmt::Debug>(
    case: &str,
    actual: T,
    expected: T,
) -> Result<(), StoreError> {
    if actual == expected {
        Ok(())
    } else {
        Err(failed(
            case,
            format!("expected {expected:?}, got {actual:?}"),
        ))
    }
}

fn take(seen: &Arc<Mutex<Vec<i64>>>) -> Vec<i64> {
    seen.lock()
        .map(|mut seen| std::mem::take(&mut *seen))
        .unwrap_or_default()
}

async fn seed_history<St, Db>(
    store: &St,
    table: &'static str,
    rows: &'static [SeedRow],
    source_checksum: Vec<u8>,
) -> Result<(), StoreError>
where
    St: AtomicStore,
    Db: Database,
    for<'transaction> St::Scope<'transaction>: TransactionScope<Database = Db>,
    Db::Arguments: IntoArguments<Db>,
    for<'value> Vec<u8>: Type<Db> + Encode<'value, Db>,
{
    store
        .transaction(move |scope| {
            Box::pin(async move {
                scope
                    .executor()
                    .execute(AssertSqlSafe(format!(
                        "CREATE TABLE {table} (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, success BOOLEAN NOT NULL, checksum BLOB NOT NULL, execution_time BIGINT NOT NULL)"
                    )))
                    .await?;
                for row in rows {
                    let insert = format!(
                        "INSERT INTO {table}(version,description,installed_on,success,checksum,execution_time) VALUES({},'seeded',{},{},{},{})",
                        row.version,
                        row.installed_on,
                        row.success,
                        row.checksum.unwrap_or("?1"),
                        row.execution_time,
                    );
                    let statement = query::<Db>(AssertSqlSafe(insert));
                    let statement = if row.checksum.is_none() {
                        statement.bind(source_checksum.clone())
                    } else {
                        statement
                    };
                    statement.execute(scope.executor()).await?;
                }
                Ok(())
            })
        })
        .await
}

async fn receipts<St, Db>(store: &St, table: &'static str) -> Result<Vec<Receipt>, StoreError>
where
    St: AtomicStore,
    Db: Database,
    for<'transaction> St::Scope<'transaction>: TransactionScope<Database = Db>,
    Db::Arguments: IntoArguments<Db>,
    for<'value> i64: Type<Db> + Decode<'value, Db>,
    for<'value> String: Type<Db> + Decode<'value, Db>,
    for<'value> Vec<u8>: Type<Db> + Decode<'value, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let rows = query::<Db>(AssertSqlSafe(format!(
                    "SELECT CAST(version AS INTEGER) AS version, CAST(installed_on AS TEXT) AS installed_on, CAST(success AS INTEGER) AS success, checksum, CAST(execution_time AS INTEGER) AS execution_time FROM {table} ORDER BY version"
                )))
                .fetch_all(scope.executor())
                .await?;
                rows.iter()
                    .map(|row| {
                        Ok(Receipt {
                            version: row.try_get("version")?,
                            installed_on: row.try_get("installed_on")?,
                            success: row.try_get("success")?,
                            checksum: row.try_get("checksum")?,
                            execution_time: row.try_get("execution_time")?,
                        })
                    })
                    .collect::<Result<Vec<_>, sqlx_core::Error>>()
                    .map_err(StoreError::from)
            })
        })
        .await
}

async fn receipts_versions<St, Db>(store: &St, table: &'static str) -> Result<Vec<i64>, StoreError>
where
    St: AtomicStore,
    Db: Database,
    for<'transaction> St::Scope<'transaction>: TransactionScope<Database = Db>,
    Db::Arguments: IntoArguments<Db>,
    for<'value> i64: Type<Db> + Decode<'value, Db>,
    for<'value> String: Type<Db> + Decode<'value, Db>,
    for<'value> Vec<u8>: Type<Db> + Decode<'value, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    Ok(receipts(store, table)
        .await?
        .into_iter()
        .map(|receipt| receipt.version)
        .collect())
}

async fn count<St, Db>(store: &St, table: &'static str) -> Result<i64, StoreError>
where
    St: AtomicStore,
    Db: Database,
    for<'transaction> St::Scope<'transaction>: TransactionScope<Database = Db>,
    Db::Arguments: IntoArguments<Db>,
    for<'value> i64: Type<Db> + Decode<'value, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let row = query::<Db>(AssertSqlSafe(format!(
                    "SELECT COUNT(*) AS total FROM {table}"
                )))
                .fetch_one(scope.executor())
                .await?;
                Ok(row.try_get::<i64, _>("total")?)
            })
        })
        .await
}

async fn schema_text<St, Db>(store: &St, table: &'static str) -> Result<String, StoreError>
where
    St: AtomicStore,
    Db: Database,
    for<'transaction> St::Scope<'transaction>: TransactionScope<Database = Db>,
    Db::Arguments: IntoArguments<Db>,
    for<'value> String: Type<Db> + Encode<'value, Db> + Decode<'value, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let row =
                    query::<Db>("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1")
                        .bind(table.to_owned())
                        .fetch_one(scope.executor())
                        .await?;
                Ok(row.try_get::<String, _>("sql")?)
            })
        })
        .await
}

async fn table_exists<St, Db>(store: &St, table: &'static str) -> Result<bool, StoreError>
where
    St: AtomicStore,
    Db: Database,
    for<'transaction> St::Scope<'transaction>: TransactionScope<Database = Db>,
    Db::Arguments: IntoArguments<Db>,
    for<'value> i64: Type<Db> + Decode<'value, Db>,
    for<'value> String: Type<Db> + Encode<'value, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let row = query::<Db>("SELECT COUNT(*) AS total FROM sqlite_master WHERE type = 'table' AND name = ?1")
                    .bind(table.to_owned())
                    .fetch_one(scope.executor())
                    .await?;
                Ok(row.try_get::<i64, _>("total")? == 1)
            })
        })
        .await
}
