#![allow(clippy::panic_in_result_fn)]

use durable_steps::Uuid;
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::PeerId;
use fleet_sync_protocol::TableName;
use fleet_sync_writer::{
    AdoptionHistory, Migration, OpenError, ReplicatedDatabase, ReplicatedSchema, WriterOptions,
};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    migrate::Migrator,
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::{sync::LazyLock, time::Duration};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static MIGRATIONS: Migrator = sqlx::migrate!("./tests/fixtures/adoption");
static TABLES: LazyLock<Result<Vec<TableName>, fleet_sync_protocol::TextValueError>> =
    LazyLock::new(|| {
        ["text_records", "composite_records", "real_records"]
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
async fn adopt(path: &std::path::Path) -> Result<ReplicatedDatabase<Schema>, OpenError> {
    ReplicatedDatabase::adopt(
        path,
        Schema {
            tables: TABLES.as_ref().map_err(|error| OpenError::Storage {
                message: error.to_string(),
            })?,
        },
        PeerId::new(Uuid::from_u128(2)).map_err(|error| OpenError::Storage {
            message: error.to_string(),
        })?,
        LeasePolicy {
            lease_duration: Duration::from_secs(60),
            renew_interval: Duration::from_secs(10),
        },
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await
}

#[tokio::test]
async fn adoption_preserves_prefix_data_and_captures_only_later_migrations() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("existing.db");
    let mut connection = TursoConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .connect()
        .await?;
    Migrator::with_migrations(MIGRATIONS.iter().take(1).cloned().collect::<Vec<_>>())
        .run_direct(None, &mut connection, false)
        .await?;
    connection
        .execute(
            "UPDATE text_records SET amount=42 WHERE id='019eef00-0000-7000-8000-000000000001'",
        )
        .await?;
    connection.close().await?;
    let writer = adopt(&path).await?;
    let row = query::<Turso>(
        "SELECT amount,label FROM text_records WHERE id='019eef00-0000-7000-8000-000000000001'",
    )
    .fetch_one(writer.reader())
    .await?;
    assert_eq!(
        row.try_get::<i64, _>("amount")?,
        43,
        "adoption reran prefix DML"
    );
    assert_eq!(row.try_get::<String, _>("label")?, "backfilled");
    let steps = query::<Turso>("SELECT version FROM _fleet_sync_schema_steps ORDER BY version")
        .fetch_all(writer.reader())
        .await?;
    assert_eq!(steps.len(), 1);
    assert_eq!(
        steps
            .first()
            .ok_or("no captured migration")?
            .try_get::<i64, _>("version")?,
        2
    );
    let captured_prefix =
        query::<Turso>("SELECT change_id FROM turso_cdc WHERE table_name='composite_records'")
            .fetch_all(writer.reader())
            .await?;
    assert!(captured_prefix.is_empty(), "prefix rows entered capture");
    let mut snapshot = writer.snapshot().await?;
    let page = snapshot.page(None).await?;
    assert!(
        page.schema_steps
            .iter()
            .all(|sql| !sql.to_ascii_uppercase().contains("INSERT")
                && !sql.to_ascii_uppercase().contains("UPDATE")),
        "seed schema reruns historical DML"
    );
    seed_into_real_copy(&directory.path().join("copy.db"), &mut snapshot).await?;
    snapshot.close().await?;
    writer.close().await;
    Ok(())
}

struct CopyScope<'a>(&'a mut sqlx_turso::TursoConnection);
impl durable_steps::storage::TransactionScope for CopyScope<'_> {
    type Database = Turso;
    type Executor<'a>
        = &'a mut sqlx_turso::TursoConnection
    where
        Self: 'a;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.0
    }
}
struct CopyHook;
impl fleet_sync_receiver::ReceiverHook<CopyScope<'_>> for CopyHook {
    type Protocol = fleet_sync_protocol::NoConsumer;
    type FollowUp = ();
    async fn on_applied(
        &self,
        _: &mut CopyScope<'_>,
        _: &fleet_sync_receiver::AppliedPiece<'_, ()>,
    ) -> Result<Vec<()>, fleet_sync_protocol::HookRefusal> {
        Ok(Vec::new())
    }
    async fn follow_up(
        &self,
        _: &(),
        _: durable_steps::CallId,
    ) -> Result<fleet_sync_receiver::FollowUpDone, fleet_sync_receiver::FollowUpFailure> {
        Ok(fleet_sync_receiver::FollowUpDone)
    }
}

async fn seed_into_real_copy(
    path: &std::path::Path,
    snapshot: &mut fleet_sync_writer::WriterSnapshot,
) -> TestResult {
    use fleet_sync_protocol::{
        CopyOrigin, CreateCopy, PageDigest, SeedAnswer, SeedComplete, SeedPage, SeedPageAnswer,
    };
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .foreign_keys(false)
        .connect()
        .await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut migrator = sqlx::migrate!("../fleet-sync-receiver/migrations");
    migrator.dangerous_set_table_name("_fleet_sync_copy_migrations");
    migrator.run_direct(None, &mut *transaction, false).await?;
    let manifest = snapshot.manifest().await?;
    let peer = PeerId::new(Uuid::from_u128(2))?;
    let authority = fleet_sync_receiver::SeedAuthority::Writer(peer);
    let mut scope = CopyScope(&mut transaction);
    fleet_sync_receiver::initialize_hook(&mut scope, &CopyHook).await?;
    fleet_sync_receiver::create_copy(
        &mut scope,
        &CreateCopy {
            database: manifest.database,
            epoch: manifest.position.epoch,
            holder: Some(peer),
            origin: CopyOrigin::AwaitingSeed {
                expected_position: Some(manifest.position),
            },
        },
    )
    .await?;
    let seed = fleet_sync_receiver::seed_begin(
        &mut scope,
        manifest.database,
        Uuid::from_u128(10),
        &authority,
    )
    .await?;
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let contents = snapshot.page(cursor).await?;
        cursor = contents.next.clone();
        let page = SeedPage {
            seed,
            page: pages,
            digest: PageDigest::of(&contents)?,
            contents,
        };
        assert_eq!(
            fleet_sync_receiver::seed_page(&mut scope, &page, &authority).await?,
            SeedPageAnswer::Staged
        );
        pages += 1;
        if cursor.is_none() {
            break;
        }
    }
    let done = SeedComplete {
        database: manifest.database,
        seed,
        position: manifest.position,
        pages,
        schema: manifest.schema,
        table_digests: manifest
            .tables
            .into_iter()
            .map(|table| (table.table, table.digest))
            .collect(),
    };
    assert!(
        matches!(fleet_sync_receiver::seed_complete(&mut scope, &done, &CopyHook, &authority).await?, fleet_sync_receiver::SeedDecision::Published { position, .. } if position == done.position)
    );
    assert!(
        matches!(fleet_sync_receiver::seed_complete(&mut scope, &done, &CopyHook, &authority).await?, fleet_sync_receiver::SeedDecision::Answer(SeedAnswer::Published { position }) if position == done.position)
    );
    transaction.commit().await?;
    let row = query::<Turso>(
        "SELECT amount,label FROM text_records WHERE id='019eef00-0000-7000-8000-000000000001'",
    )
    .fetch_one(&mut connection)
    .await?;
    assert_eq!(
        row.try_get::<i64, _>("amount")?,
        43,
        "seed replayed historical DML"
    );
    assert_eq!(row.try_get::<String, _>("label")?, "backfilled");
    connection.close().await?;
    Ok(())
}

#[tokio::test]
async fn invalid_history_refuses_without_changing_file_bytes() -> TestResult {
    for case in ["missing", "checksum", "gap", "unknown", "failed"] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("existing.db");
        let mut connection = TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .connect()
            .await?;
        Migrator::with_migrations(MIGRATIONS.iter().cloned().collect::<Vec<_>>())
            .run_direct(None, &mut connection, false)
            .await?;
        match case {
            "missing" => connection.execute("DROP TABLE _sqlx_migrations").await?,
            "checksum" => {
                connection
                    .execute("UPDATE _sqlx_migrations SET checksum=x'00' WHERE version=1")
                    .await?
            }
            "gap" => {
                connection
                    .execute("DELETE FROM _sqlx_migrations WHERE version=1")
                    .await?
            }
            "unknown" => {
                connection
                    .execute("UPDATE _sqlx_migrations SET version=99 WHERE version=2")
                    .await?
            }
            "failed" => {
                connection
                    .execute("UPDATE _sqlx_migrations SET success=0 WHERE version=2")
                    .await?
            }
            _ => return Err("unknown fixture".into()),
        };
        connection.close().await?;
        let before = tokio::fs::read(&path).await?;
        let error = match adopt(&path).await {
            Ok(writer) => {
                writer.close().await;
                return Err(format!("adopt accepted {case}").into());
            }
            Err(error) => error,
        };
        assert!(
            matches!(
                (&error, case),
                (
                    OpenError::AdoptionHistory {
                        reason: AdoptionHistory::Missing
                    },
                    "missing"
                ) | (
                    OpenError::AdoptionHistory {
                        reason: AdoptionHistory::Checksum { version: 1 }
                    },
                    "checksum"
                ) | (
                    OpenError::AdoptionHistory {
                        reason: AdoptionHistory::NonPrefix {
                            version: 2,
                            expected: Some(1)
                        }
                    },
                    "gap"
                ) | (
                    OpenError::AdoptionHistory {
                        reason: AdoptionHistory::NonPrefix {
                            version: 99,
                            expected: Some(2)
                        }
                    },
                    "unknown"
                ) | (
                    OpenError::AdoptionHistory {
                        reason: AdoptionHistory::Failed { version: 2 }
                    },
                    "failed"
                )
            ),
            "{case}: {error}"
        );
        assert_eq!(
            tokio::fs::read(&path).await?,
            before,
            "{case} refusal changed file bytes"
        );
    }
    Ok(())
}
