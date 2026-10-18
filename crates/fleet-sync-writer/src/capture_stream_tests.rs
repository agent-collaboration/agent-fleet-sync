#![allow(clippy::panic_in_result_fn)]
use crate::{
    Migration, ReplicatedDatabase, ReplicatedSchema, WriterOptions,
    capture_stream::read_window,
    checked_queries,
    writer_file_record::{read_file_record, write_file_record},
    writer_scope::WriterScope,
};
use durable_steps::Uuid;
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::{PeerId, ReplicatedTransaction, RowChange, SqlValue, TableName};
use sqlx_core::{
    connection::Connection,
    executor::Executor,
    migrate::{MigrationType, Migrator},
    sql_str::SqlSafeStr,
};
use std::{sync::LazyLock, time::Duration};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static REGULAR: Migrator = sqlx::migrate!("./tests/fixtures/regular");
static TABLES: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
    LazyLock::new(|| {
        ["text_records", "composite_records", "real_records"]
            .into_iter()
            .map(TableName::new)
            .collect()
    });
struct CaptureSchema {
    migrations: &'static [Migration],
    tables: &'static [TableName],
}
impl ReplicatedSchema for CaptureSchema {
    fn migrations(&self) -> &'static [Migration] {
        self.migrations
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}
fn schema(migrations: &'static [Migration]) -> TestResult<CaptureSchema> {
    Ok(CaptureSchema {
        migrations,
        tables: TABLES.as_ref().map_err(Clone::clone)?,
    })
}
fn peer() -> TestResult<PeerId> {
    Ok(PeerId::new(Uuid::from_u128(
        0x019eef00000070008000000000000002,
    ))?)
}
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(10),
        renew_interval: Duration::from_secs(1),
    }
}

#[tokio::test]
async fn migrations_are_single_schema_steps_and_payload_updates_keep_every_uuid_key() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let database = ReplicatedDatabase::create(
        &directory.path().join("writer.db"),
        schema(REGULAR.iter().as_slice())?,
        peer()?,
        policy(),
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    let mut write = database.begin_write().await?;
    (&mut write).execute("UPDATE text_records SET amount=123; UPDATE composite_records SET payload=x'80ff'; UPDATE real_records SET ratio=2.5").await?;
    write.commit().await?;
    let mut connection = database.maintenance.acquire().await?;
    let mut transaction = connection.begin().await?;
    let record = read_file_record(&mut transaction)
        .await?
        .ok_or("no writer record")?;
    let commits = read_window(&mut transaction, &record, record.start, 10).await?;
    assert_eq!(commits.len(), 3);
    assert!(commits.iter().take(2).all(|commit| matches!(
        &commit.transaction,
        ReplicatedTransaction::SchemaStep { .. }
    )));
    let ReplicatedTransaction::Rows { rows, .. } = &commits
        .last()
        .ok_or("no captured payload update")?
        .transaction
    else {
        return Err("expected rows".into());
    };
    assert_eq!(rows.len(), 3);
    for (table, key) in [
        (
            "text_records",
            vec![SqlValue::Text(
                "019eef00-0000-7000-8000-000000000001".into(),
            )],
        ),
        (
            "composite_records",
            vec![
                SqlValue::Text("019eef00-0000-7000-8000-000000000002".into()),
                SqlValue::Text("019eef00-0000-7000-8000-000000000003".into()),
            ],
        ),
        (
            "real_records",
            vec![SqlValue::Text(
                "019eef00-0000-7000-8000-000000000004".into(),
            )],
        ),
    ] {
        assert!(rows.iter().any(|row|matches!(row,RowChange::Upsert{table:name,key:found,..} if name==table && found==&key)),"{table}");
    }
    assert!(
        rows.iter()
            .all(|row| matches!(row, RowChange::Upsert { .. }))
    );
    transaction.commit().await?;
    drop(connection);
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn historical_images_use_the_layout_at_their_commit_before_and_after_pruning() -> TestResult {
    static CHANGED: LazyLock<Vec<Migration>> = LazyLock::new(|| {
        let mut migrations = REGULAR.iter().take(1).cloned().collect::<Vec<_>>();
        migrations.push(Migration::new(
            2,
            "add column".into(),
            MigrationType::Simple,
            "ALTER TABLE text_records ADD COLUMN extra TEXT".into_sql_str(),
            false,
        ));
        migrations
    });
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("writer.db");
    let first = REGULAR
        .iter()
        .as_slice()
        .get(..1)
        .ok_or("no first migration")?;
    let database = ReplicatedDatabase::create(
        &path,
        schema(first)?,
        peer()?,
        policy(),
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    let mut write = database.begin_write().await?;
    (&mut write)
        .execute("INSERT INTO text_records VALUES('019eef00-0000-7000-8000-000000000005',9007199254740993,'three columns')")
        .await?;
    write.commit().await?;
    let id = database.database();
    database.close().await;
    let database = ReplicatedDatabase::open_with(
        &path,
        id,
        schema(&CHANGED)?,
        LeaseHolder::provisional(id, peer()?, policy()),
        std::sync::Arc::new(runtime_primitives::SystemClock),
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    let mut write = database.begin_write().await?;
    (&mut write)
        .execute("UPDATE text_records SET extra='four columns' WHERE id='019eef00-0000-7000-8000-000000000005'")
        .await?;
    write.commit().await?;
    let mut connection = database.maintenance.acquire().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut record = read_file_record(&mut transaction)
        .await?
        .ok_or("no writer record")?;
    let commits = read_window(&mut transaction, &record, record.start, 10).await?;
    assert_eq!(commits.len(), 4);
    let old = commits.get(1).ok_or("no pre-migration data")?;
    let new = commits.get(3).ok_or("no post-migration data")?;
    for (commit, columns) in [(old, 3), (new, 4)] {
        assert!(
            matches!(&commit.transaction,ReplicatedTransaction::Rows{rows,..} if matches!(rows.as_slice(),[RowChange::Upsert{values,..}] if values.len()==columns))
        );
    }
    record.retained_after = old.position;
    record.retained_change_id = old.position.sequence.value();
    record.retained_layout = old.layout_after.clone();
    checked_queries::prune_capture(
        &mut WriterScope(&mut transaction),
        i64::try_from(record.retained_change_id)?,
    )
    .await?;
    write_file_record(&mut transaction, &record).await?;
    transaction.commit().await?;
    drop(connection);
    database.close().await;
    let database = ReplicatedDatabase::open(
        &path,
        id,
        schema(&CHANGED)?,
        LeaseHolder::provisional(id, peer()?, policy()),
    )
    .await?;
    let mut connection = database.maintenance.acquire().await?;
    let mut transaction = connection.begin().await?;
    let record = read_file_record(&mut transaction)
        .await?
        .ok_or("no writer record")?;
    let remaining = read_window(&mut transaction, &record, record.retained_after, 10).await?;
    assert_eq!(remaining.len(), 2);
    assert_eq!(
        remaining.last().ok_or("no remaining commit")?.transaction,
        new.transaction
    );
    transaction.commit().await?;
    drop(connection);
    database.close().await;
    Ok(())
}
