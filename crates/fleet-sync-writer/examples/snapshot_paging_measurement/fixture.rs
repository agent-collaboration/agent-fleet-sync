use durable_steps::storage::TransactionScope;
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::*;
use fleet_sync_receiver::{AppliedPiece, HookError, ReceiverRules};
use fleet_sync_writer::{Migration, ReplicatedDatabase, ReplicatedSchema, WriterOptions};
use runtime_primitives::Clock;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    migrate::MigrationType,
    query::query,
    sql_str::SqlSafeStr,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use std::{
    path::Path,
    sync::{Arc, LazyLock},
    time::Duration,
};

pub type MeasurementResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const SCHEMA: &str = "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY, body TEXT)";
static TABLES: LazyLock<Result<Vec<TableName>, TextValueError>> =
    LazyLock::new(|| Ok(vec![TableName::new("records")?]));
static MIGRATIONS: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    vec![Migration::new(
        1,
        "paging fixture".into(),
        MigrationType::Simple,
        SCHEMA.into_sql_str(),
        false,
    )]
});
pub struct MeasurementSchema(&'static [TableName]);
impl ReplicatedSchema for MeasurementSchema {
    fn migrations(&self) -> &'static [Migration] {
        &MIGRATIONS
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.0
    }
}
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(1_000)
    }
}
fn database() -> MeasurementResult<DatabaseId> {
    Ok(DatabaseId::new(
        "019f0000-0000-7000-8000-000000000011".parse()?,
    )?)
}
fn peer() -> MeasurementResult<PeerId> {
    Ok(PeerId::new(
        "019f0000-0000-7000-8000-000000000012".parse()?,
    )?)
}
pub async fn writer(path: &Path) -> MeasurementResult<ReplicatedDatabase<MeasurementSchema>> {
    let database = database()?;
    let peer = peer()?;
    let mut holder = LeaseHolder::provisional(
        database,
        peer,
        LeasePolicy {
            lease_duration: Duration::from_secs(3_600),
            renew_interval: Duration::from_secs(30),
        },
    );
    holder.observe_registration(
        LeaseStatus {
            database,
            epoch: Epoch::new(1)?,
            holder: peer,
            state: LeaseState::Held,
            expires_at: SyncTime::from_unix_millis(3_601_000),
            remaining_ms: 3_600_000,
        },
        SyncTime::from_unix_millis(1_000),
    )?;
    let tables = TABLES.as_ref().map_err(|error| error.to_string())?;
    Ok(ReplicatedDatabase::open_with(
        path,
        database,
        MeasurementSchema(tables),
        holder,
        Arc::new(FixedClock),
        WriterOptions::default(),
    )
    .await?)
}
pub async fn connection(path: &Path) -> MeasurementResult<TursoConnection> {
    Ok(TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .connect()
        .await?)
}
pub struct TursoScope<'a>(pub &'a mut TursoConnection);
impl TransactionScope for TursoScope<'_> {
    type Database = Turso;
    type Executor<'q>
        = &'q mut TursoConnection
    where
        Self: 'q;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}
struct Rules;
impl ReceiverRules<TursoScope<'_>> for Rules {
    type Protocol = NoConsumer;
    type FollowUp = ();
    async fn on_applied(
        &self,
        _: &mut TursoScope<'_>,
        _: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<()>, HookError> {
        Ok(Vec::new())
    }
}
async fn insert_rows(
    connection: &mut TursoConnection,
    rows: usize,
    body_bytes: usize,
) -> MeasurementResult {
    if rows > 0 {
        query::<Turso>("WITH RECURSIVE numbers(value) AS(VALUES(0) UNION ALL SELECT value+1 FROM numbers WHERE value+1<?) INSERT INTO records SELECT printf('019eef00-0000-7000-8000-%012x',value),? FROM numbers")
            .bind(i64::try_from(rows)?).bind(r"\u0001".repeat(body_bytes / 6) + &"x".repeat(body_bytes % 6)).execute(connection).await?;
    }
    Ok(())
}
pub async fn prepare_writer(path: &Path, rows: usize, body_bytes: usize) -> MeasurementResult {
    let database = writer(path).await?;
    let mut transaction = database.begin_write().await?;
    if rows > 0 {
        // One captured SQL statement; setup runs outside the measured process.
        query::<Turso>("WITH RECURSIVE numbers(value) AS(VALUES(0) UNION ALL SELECT value+1 FROM numbers WHERE value+1<?) INSERT INTO records SELECT printf('019eef00-0000-7000-8000-%012x',value),? FROM numbers")
            .bind(i64::try_from(rows)?).bind(r"\u0001".repeat(body_bytes / 6) + &"x".repeat(body_bytes % 6)).execute(&mut transaction).await?;
    }
    transaction.commit().await?;
    database.close().await;
    Ok(())
}
pub async fn prepare_copy(path: &Path, rows: usize, body_bytes: usize) -> MeasurementResult {
    let mut connection = connection(path).await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    fleet_sync_receiver::RECEIVER_MIGRATOR
        .run_direct(None, &mut *transaction, false)
        .await?;
    fleet_sync_receiver::create_copy(
        &mut TursoScope(&mut transaction),
        &CreateCopy {
            database: database()?,
            epoch: Epoch::new(1)?,
            holder: Some(peer()?),
            origin: CopyOrigin::Empty,
        },
    )
    .await?;
    let position = |sequence| {
        Ok::<_, Box<dyn std::error::Error>>(ReplicationPosition::new(
            Epoch::new(1)?,
            CommitSequence::new(sequence),
        ))
    };
    let schema = Piece::<()>::new(PieceContents {
        database: database()?,
        epoch: Epoch::new(1)?,
        continues_from: position(0)?,
        through: position(1)?,
        transactions: vec![ReplicatedTransaction::SchemaStep {
            commit_id: CommitSequence::new(1),
            migration_version: 1,
            statements: vec![SCHEMA.into()],
        }],
        part: None,
        annotation: None,
    })?;
    fleet_sync_receiver::apply_piece(
        &mut TursoScope(&mut transaction),
        &schema,
        &peer()?,
        &Rules,
        SyncTime::from_unix_millis(1_000),
    )
    .await?;
    insert_rows(&mut transaction, rows, body_bytes).await?;
    transaction.commit().await?;
    connection.close().await?;
    Ok(())
}
