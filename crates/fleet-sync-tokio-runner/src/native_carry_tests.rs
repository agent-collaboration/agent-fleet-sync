#![allow(clippy::panic_in_result_fn)]
use crate::{LocalStateCarryError, LocalStateSelection, carry_local_state};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::collections::BTreeSet;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn selection() -> LocalStateSelection {
    LocalStateSelection {
        replicated_before: BTreeSet::from(["records".into()]),
        replicated_after: BTreeSet::from(["records".into()]),
        replaced_tables: BTreeSet::from(["_replace_me".into()]),
    }
}
#[tokio::test]
async fn carry_preserves_local_rows_indexes_rowids_and_deleted_autoincrement_high_water()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let mut source = TursoConnectOptions::new()
        .filename(directory.path().join("source.db"))
        .create_if_missing(true)
        .foreign_keys(false)
        .connect()
        .await?;
    let destination = directory.path().join("staging.db");
    let mut staging = TursoConnectOptions::new()
        .filename(&destination)
        .create_if_missing(true)
        .foreign_keys(false)
        .connect()
        .await?;
    source.execute("CREATE TABLE records(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO records VALUES(1,'old replica'); CREATE TABLE _replace_me(value TEXT); INSERT INTO _replace_me VALUES('old metadata'); CREATE TABLE hook_auto(id INTEGER PRIMARY KEY AUTOINCREMENT,value TEXT); INSERT INTO hook_auto VALUES(100,'deleted'); DELETE FROM hook_auto; CREATE TABLE hook_values(id TEXT NOT NULL PRIMARY KEY,exact_value INTEGER,payload BLOB,missing TEXT); INSERT INTO hook_values(rowid,id,exact_value,payload,missing) VALUES(700,'owned',9007199254740993,x'00ff80',NULL); CREATE INDEX local_value_index ON hook_values(exact_value); CREATE TABLE hook_rows(note TEXT); INSERT INTO hook_rows(rowid,note) VALUES(100,'implicit identity'); CREATE TABLE local_pairs(a TEXT NOT NULL,b INTEGER NOT NULL,payload BLOB,PRIMARY KEY(a,b)); INSERT INTO local_pairs VALUES('pair',9223372036854775807,x'80ff')").await?;
    staging.execute("CREATE TABLE records(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO records VALUES(2,'verified snapshot'); CREATE TABLE _replace_me(value TEXT); INSERT INTO _replace_me VALUES('new metadata'); CREATE TABLE obsolete_local(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO obsolete_local VALUES(1,'previous carry')").await?;
    let mut transaction = staging.begin_with("BEGIN IMMEDIATE").await?;
    carry_local_state(&mut source, &mut transaction, &selection()).await?;
    transaction.commit().await?;
    let row = query::<Turso>("SELECT rowid,id,exact_value,payload,missing FROM hook_values")
        .fetch_one(&mut staging)
        .await?;
    assert_eq!(row.try_get::<i64, _>("rowid")?, 700);
    assert_eq!(row.try_get::<String, _>("id")?, "owned");
    assert_eq!(row.try_get::<i64, _>("exact_value")?, 9_007_199_254_740_993);
    assert_eq!(row.try_get::<Vec<u8>, _>("payload")?, [0, 255, 128]);
    assert_eq!(row.try_get::<Option<String>, _>("missing")?, None);
    assert_eq!(
        query::<Turso>("SELECT rowid FROM hook_rows")
            .fetch_one(&mut staging)
            .await?
            .try_get::<i64, _>("rowid")?,
        100
    );
    assert!(
        query::<Turso>("SELECT name FROM sqlite_schema WHERE name='local_value_index'")
            .fetch_optional(&mut staging)
            .await?
            .is_some()
    );
    assert!(
        query::<Turso>("SELECT name FROM sqlite_schema WHERE name='obsolete_local'")
            .fetch_optional(&mut staging)
            .await?
            .is_none()
    );
    assert_eq!(
        query::<Turso>("SELECT value FROM records")
            .fetch_one(&mut staging)
            .await?
            .try_get::<String, _>("value")?,
        "verified snapshot"
    );
    assert_eq!(
        query::<Turso>("SELECT value FROM _replace_me")
            .fetch_one(&mut staging)
            .await?
            .try_get::<String, _>("value")?,
        "new metadata"
    );
    staging.close().await?;
    let mut staging = TursoConnectOptions::new()
        .filename(&destination)
        .foreign_keys(false)
        .connect()
        .await?;
    staging
        .execute("INSERT INTO hook_auto(value) VALUES('next')")
        .await?;
    assert_eq!(
        query::<Turso>("SELECT id FROM hook_auto")
            .fetch_one(&mut staging)
            .await?
            .try_get::<i64, _>("id")?,
        101
    );
    source.execute("DELETE FROM hook_values; INSERT INTO hook_values(id,exact_value,payload,missing) VALUES('latest',42,x'01','new'); DROP TABLE hook_rows; INSERT INTO hook_auto VALUES(200,'deleted later'); DELETE FROM hook_auto").await?;
    let mut transaction = staging.begin_with("BEGIN IMMEDIATE").await?;
    carry_local_state(&mut source, &mut transaction, &selection()).await?;
    transaction.commit().await?;
    assert_eq!(
        query::<Turso>("SELECT id FROM hook_values")
            .fetch_one(&mut staging)
            .await?
            .try_get::<String, _>("id")?,
        "latest"
    );
    assert!(
        query::<Turso>("SELECT name FROM sqlite_schema WHERE name='hook_rows'")
            .fetch_optional(&mut staging)
            .await?
            .is_none(),
        "repeated carry resurrected a dropped local table"
    );
    staging
        .execute("INSERT INTO hook_auto(value) VALUES('new next')")
        .await?;
    assert_eq!(
        query::<Turso>("SELECT id FROM hook_auto")
            .fetch_one(&mut staging)
            .await?
            .try_get::<i64, _>("id")?,
        201
    );
    source.close().await?;
    staging.close().await?;
    Ok(())
}

#[tokio::test]
async fn fully_shadowed_rowids_keep_named_content_while_addressable_aliases_keep_identity()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let mut source = TursoConnectOptions::new()
        .filename(directory.path().join("source.db"))
        .create_if_missing(true)
        .connect()
        .await?;
    let mut staging = TursoConnectOptions::new()
        .filename(directory.path().join("staging.db"))
        .create_if_missing(true)
        .connect()
        .await?;
    // Insert while the hidden alias is addressable, then shadow it. This gives
    // the source an observable allocation history without altering the schema
    // during carry (decision 46).
    source.execute("CREATE TABLE shadowed(rowid TEXT,_rowid_ TEXT,pending_alias TEXT,payload BLOB); INSERT INTO shadowed(oid,rowid,_rowid_,pending_alias,payload) VALUES(100,'named rowid','named underscore','named oid',x'00ff'); ALTER TABLE shadowed RENAME COLUMN pending_alias TO oid; CREATE TABLE partially_shadowed(rowid TEXT,_rowid_ TEXT); INSERT INTO partially_shadowed(oid,rowid,_rowid_) VALUES(700,'one','two')").await?;
    let mut transaction = staging.begin_with("BEGIN IMMEDIATE").await?;
    carry_local_state(&mut source, &mut transaction, &selection()).await?;
    transaction.commit().await?;
    let row = query::<Turso>("SELECT rowid,_rowid_,oid,payload FROM shadowed")
        .fetch_one(&mut staging)
        .await?;
    assert_eq!(row.try_get::<String, _>("rowid")?, "named rowid");
    assert_eq!(row.try_get::<String, _>("_rowid_")?, "named underscore");
    assert_eq!(row.try_get::<String, _>("oid")?, "named oid");
    assert_eq!(row.try_get::<Vec<u8>, _>("payload")?, [0, 255]);
    assert_eq!(
        query::<Turso>("SELECT oid AS preserved_identity FROM partially_shadowed")
            .fetch_one(&mut staging)
            .await?
            .try_get::<i64, _>("preserved_identity")?,
        700
    );
    // Removing only the shadow reveals that named-content carry may allocate
    // a new hidden rowid. The copied user columns above remain identical.
    source
        .execute("ALTER TABLE shadowed RENAME COLUMN oid TO revealed")
        .await?;
    staging
        .execute("ALTER TABLE shadowed RENAME COLUMN oid TO revealed")
        .await?;
    let old_rowid = query::<Turso>("SELECT oid AS hidden_identity FROM shadowed")
        .fetch_one(&mut source)
        .await?
        .try_get::<i64, _>("hidden_identity")?;
    let new_rowid = query::<Turso>("SELECT oid AS hidden_identity FROM shadowed")
        .fetch_one(&mut staging)
        .await?
        .try_get::<i64, _>("hidden_identity")?;
    assert_eq!(old_rowid, 100);
    assert_eq!(new_rowid, 1);
    source.close().await?;
    staging.close().await?;
    Ok(())
}

#[tokio::test]
async fn local_name_conflicts_refuse_before_changing_the_verified_snapshot() -> TestResult {
    let directory = tempfile::tempdir()?;
    let mut source = TursoConnectOptions::new()
        .filename(directory.path().join("source.db"))
        .create_if_missing(true)
        .connect()
        .await?;
    let mut staging = TursoConnectOptions::new()
        .filename(directory.path().join("staging.db"))
        .create_if_missing(true)
        .connect()
        .await?;
    source.execute("CREATE TABLE local_rows(value TEXT); INSERT INTO local_rows VALUES('target owned'); CREATE INDEX replica_index ON local_rows(value)").await?;
    staging.execute("CREATE TABLE records(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO records VALUES(2,'verified'); CREATE INDEX replica_index ON records(value); CREATE TABLE previous_carry(value TEXT); INSERT INTO previous_carry VALUES('retained on refusal')").await?;
    let mut transaction = staging.begin_with("BEGIN IMMEDIATE").await?;
    assert!(matches!(
        carry_local_state(&mut source, &mut transaction, &selection()).await,
        Err(LocalStateCarryError::LocalObjectConflict { name }) if name == "replica_index"
    ));
    assert_eq!(
        query::<Turso>("SELECT value FROM previous_carry")
            .fetch_one(&mut *transaction)
            .await?
            .try_get::<String, _>("value")?,
        "retained on refusal"
    );
    transaction.rollback().await?;
    assert_eq!(
        query::<Turso>("SELECT value FROM records")
            .fetch_one(&mut staging)
            .await?
            .try_get::<String, _>("value")?,
        "verified"
    );
    source.close().await?;
    staging.close().await?;
    Ok(())
}

struct NoCalls;
impl crate::CallRouter for NoCalls {
    async fn dispatch(
        &self,
        _: crate::CallEnvelope,
    ) -> Result<serde_json::Value, durable_steps::CallFailure> {
        Err(durable_steps::CallFailure::NotSent {
            reason: durable_steps::NotSentReason::Overloaded,
        })
    }
}
struct PendingTask;
impl durable_steps::Task for PendingTask {
    const KIND: &'static str = "carry-pending-task";
    const VERSION: u32 = 1;
    type Store = crate::TokioObject<crate::TokioHost<NoCalls>>;
    type Input = String;
    type Output = String;
    async fn run<C: durable_steps::OperationContext<Store = Self::Store>>(
        &self,
        _: &mut C,
        input: &String,
    ) -> Result<String, durable_steps::OperationFailure> {
        Ok(input.clone())
    }
}

#[tokio::test]
async fn carry_keeps_real_durable_journal_operations_and_unanswered_call_identity() -> TestResult {
    use durable_steps::{
        CallReplay, DriveLease, JournalScope, RequestKey, StepName, SyncTime, Uuid,
        storage::{CallAttempt, CallIntent, CallReach, JournalStorage, StepRecord, StepState},
        turso_store::{TursoScope, migrate_journal},
    };
    use runtime_primitives::{Clock, SystemClock};
    use std::sync::Arc;
    let directory = tempfile::tempdir()?;
    let host = crate::TokioHost::new(Arc::new(NoCalls), SystemClock);
    let mut source = TursoConnectOptions::new()
        .filename(directory.path().join("source.db"))
        .create_if_missing(true)
        .connect()
        .await?;
    let destination = directory.path().join("staging.db");
    let mut staging = TursoConnectOptions::new()
        .filename(&destination)
        .create_if_missing(true)
        .connect()
        .await?;
    let mut transaction = source.begin_with("BEGIN IMMEDIATE").await?;
    migrate_journal(&mut transaction).await?;
    let (operation, pending_step) = {
        let mut scope = TursoScope::new(&mut transaction, &host);
        let id = scope
            .admit::<PendingTask>(
                &RequestKey::new("original-follow-up")?,
                &"exact input".into(),
            )
            .await?
            .id();
        let token = Uuid::now_v7();
        let operation = scope
            .claim(
                id,
                host.now(),
                DriveLease {
                    token,
                    until: SyncTime::from_unix_millis(i64::MAX),
                },
            )
            .await?
            .ok_or("admitted operation was not claimable")?;
        let pending_step = StepRecord {
            operation: id,
            position: 0,
            name: StepName::new("external-follow-up")?,
            state: StepState::CallPending {
                intent: CallIntent {
                    target: "consumer-effect-sink".into(),
                    request_digest: vec![1, 0, 255],
                    replay: CallReplay::Deduplicated,
                    unsent_for: None,
                },
                attempt: CallAttempt {
                    first_attempt_at: host.now(),
                    last_seen: Some(host.now()),
                    reach: CallReach::MayHaveReached,
                },
            },
        };
        scope.save_step(pending_step.clone(), token).await?;
        (operation, pending_step)
    };
    transaction.commit().await?;
    let mut transaction = staging.begin_with("BEGIN IMMEDIATE").await?;
    carry_local_state(&mut source, &mut transaction, &selection()).await?;
    transaction.commit().await?;
    source.close().await?;
    staging.close().await?;
    let mut reopened = TursoConnectOptions::new()
        .filename(&destination)
        .connect()
        .await?;
    let mut scope = TursoScope::new(&mut reopened, &host);
    assert_eq!(scope.operation(operation.id).await?, Some(operation));
    assert_eq!(
        scope.step(pending_step.operation, 0).await?,
        Some(pending_step)
    );
    reopened.close().await?;
    Ok(())
}
