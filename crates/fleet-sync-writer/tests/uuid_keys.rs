#![allow(clippy::panic_in_result_fn, clippy::expect_used)]
use durable_steps::Uuid;
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::{PeerId, TableName};
use fleet_sync_writer::{
    Migration, OpenError, ReplicatedDatabase, ReplicatedSchema, SchemaForm, WriteRefusal,
    WriterOptions,
};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    migrate::{MigrationType, Migrator},
    query::query,
    row::Row,
    sql_str::SqlSafeStr,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::{sync::LazyLock, time::Duration};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const KEY: &str = "019eef00-0000-7000-8000-000000000001";
const NEXT_KEY: &str = "019eef00-0000-7000-8000-000000000002";
const SCHEMAS: &[&str] = &[
    "CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY,value TEXT)",
    "CREATE TABLE records(id INTEGER PRIMARY KEY,value TEXT)",
    "CREATE TABLE records(id TEXT NOT NULL,other INTEGER NOT NULL,value TEXT,PRIMARY KEY(id,other))",
    "CREATE TABLE records(id TEXT PRIMARY KEY,value TEXT)",
    "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,value TEXT)",
    "CREATE TABLE records(id TEXT NOT NULL,value TEXT)",
    "CREATE TABLE records(id TEXT NOT NULL,other TEXT NOT NULL,value TEXT,PRIMARY KEY(id,other))",
];
static BUNDLES: LazyLock<Vec<Vec<Migration>>> = LazyLock::new(|| {
    SCHEMAS
        .iter()
        .map(|sql| {
            vec![Migration::new(
                1,
                "key contract".into(),
                MigrationType::Simple,
                (*sql).into_sql_str(),
                false,
            )]
        })
        .collect()
});
static TABLES: LazyLock<Vec<TableName>> =
    LazyLock::new(|| vec![TableName::new("records").expect("fixture table")]);
struct Schema {
    index: usize,
    tables: bool,
}
impl ReplicatedSchema for Schema {
    fn migrations(&self) -> &'static [Migration] {
        BUNDLES
            .get(self.index)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        if self.tables { &TABLES } else { &[] }
    }
}
fn peer() -> TestResult<PeerId> {
    Ok(PeerId::new(Uuid::from_u128(2))?)
}
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(60),
        renew_interval: Duration::from_secs(10),
    }
}
async fn writer(path: &std::path::Path) -> TestResult<ReplicatedDatabase<Schema>> {
    Ok(ReplicatedDatabase::create(
        path,
        Schema {
            index: 0,
            tables: true,
        },
        peer()?,
        policy(),
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?)
}

#[tokio::test]
async fn open_and_adopt_refuse_non_uuid_key_schemas_without_modifying_the_file() -> TestResult {
    for index in 1..6 {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("managed.db");
        let empty = ReplicatedDatabase::create(
            &path,
            Schema {
                index: usize::MAX,
                tables: false,
            },
            peer()?,
            policy(),
            WriterOptions::default(),
        )
        .await?;
        let database = empty.database();
        empty.close().await;
        let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
        connection
            .execute(SCHEMAS.get(index).copied().ok_or("fixture schema")?)
            .await?;
        connection.close().await?;
        let before = tokio::fs::read(&path).await?;
        let result = ReplicatedDatabase::open(
            &path,
            database,
            Schema {
                index: usize::MAX,
                tables: true,
            },
            LeaseHolder::provisional(database, peer()?, policy()),
        )
        .await;
        match result {
            Ok(writer) => {
                writer.close().await;
                panic!("unsupported key schema {index} opened");
            }
            Err(error) => assert!(
                matches!(
                    error,
                    OpenError::SchemaUnsupported {
                        form: SchemaForm::KeyNotUuidV7 { .. }
                    }
                ),
                "{error}"
            ),
        }
        assert_eq!(tokio::fs::read(&path).await?, before);
        let path = directory.path().join("adopted.db");
        let mut connection = TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .connect()
            .await?;
        Migrator::with_migrations(BUNDLES.get(index).ok_or("fixture bundle")?.clone())
            .run_direct(None, &mut connection, false)
            .await?;
        connection.close().await?;
        let before = tokio::fs::read(&path).await?;
        let result = ReplicatedDatabase::adopt(
            &path,
            Schema {
                index,
                tables: true,
            },
            peer()?,
            policy(),
            WriterOptions::default(),
        )
        .await;
        match result {
            Ok(writer) => {
                writer.close().await;
                panic!("unsupported key schema {index} adopted");
            }
            Err(error) => assert!(
                matches!(
                    error,
                    OpenError::SchemaUnsupported {
                        form: SchemaForm::KeyNotUuidV7 { .. }
                    }
                ),
                "{error}"
            ),
        }
        assert_eq!(tokio::fs::read(&path).await?, before);
    }
    Ok(())
}

#[tokio::test]
async fn invalid_uuid_key_values_roll_back_every_row_and_capture_before_commit() -> TestResult {
    let directory = tempfile::tempdir()?;
    let writer = writer(&directory.path().join("writer.db")).await?;
    for invalid in [
        "plain",
        "019EEF00-0000-7000-8000-000000000001",
        "019eef00000070008000000000000001",
        "019eef00-0000-4000-8000-000000000001",
        "00000000-0000-0000-0000-000000000000",
    ] {
        let mut transaction = writer.begin_write().await?;
        query::<Turso>("INSERT INTO records VALUES(?,'accepted earlier in transaction')")
            .bind(KEY)
            .execute(&mut transaction)
            .await?;
        query::<Turso>("INSERT INTO records VALUES(?,'invalid')")
            .bind(invalid)
            .execute(&mut transaction)
            .await?;
        let result = transaction.commit().await;
        assert!(result.is_err(), "invalid key {invalid} committed");
        assert!(
            matches!(result,Err(WriteRefusal::InvalidKey{ref table}) if table=="records"),
            "{result:?}"
        );
        assert!(
            query::<Turso>("SELECT id FROM records")
                .fetch_all(writer.reader())
                .await?
                .is_empty()
        );
        assert!(
            query::<Turso>("SELECT change_id FROM turso_cdc WHERE table_name='records'")
                .fetch_all(writer.reader())
                .await?
                .is_empty()
        );
    }
    writer.close().await;
    Ok(())
}

#[tokio::test]
async fn composite_uuid_keys_validate_each_component_and_reject_transient_invalid_images()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let writer = ReplicatedDatabase::create(
        &directory.path().join("writer.db"),
        Schema {
            index: 6,
            tables: true,
        },
        peer()?,
        policy(),
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    for (first, second) in [("bad", KEY), (KEY, "bad")] {
        let mut transaction = writer.begin_write().await?;
        query::<Turso>("INSERT INTO records VALUES(?,?,'invalid')")
            .bind(first)
            .bind(second)
            .execute(&mut transaction)
            .await?;
        // The final table is empty. An invalid captured identity must still
        // roll back instead of poisoning the replication stream after commit.
        transaction.execute("DELETE FROM records").await?;
        assert!(matches!(
            transaction.commit().await,
            Err(WriteRefusal::InvalidKey { .. })
        ));
    }
    let mut transaction = writer.begin_write().await?;
    query::<Turso>("INSERT INTO records VALUES(?,?,'valid')")
        .bind(KEY)
        .bind(NEXT_KEY)
        .execute(&mut transaction)
        .await?;
    transaction.commit().await?;
    assert_eq!(
        query::<Turso>("SELECT id,other FROM records")
            .fetch_all(writer.reader())
            .await?
            .len(),
        1
    );
    for sql in [
        "UPDATE records SET id=id",
        "UPDATE records SET other=other",
        "INSERT INTO records VALUES(?,?,'new') ON CONFLICT(id,other) DO UPDATE SET other=excluded.other",
    ] {
        let mut transaction = writer.begin_write().await?;
        let result = if sql.starts_with("INSERT") {
            query::<Turso>(sql)
                .bind(KEY)
                .bind(NEXT_KEY)
                .execute(&mut transaction)
                .await
        } else {
            transaction.execute(sql).await
        };
        assert!(
            matches!(
                result.map_err(WriteRefusal::from_sqlx),
                Err(WriteRefusal::KeyChange { .. })
            ),
            "{sql}"
        );
        transaction.rollback().await?;
    }
    writer.close().await;
    Ok(())
}

#[tokio::test]
async fn assigning_the_key_is_refused_even_if_the_new_value_is_a_valid_or_identical_uuid()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let writer = writer(&directory.path().join("writer.db")).await?;
    let mut transaction = writer.begin_write().await?;
    query::<Turso>("INSERT INTO records VALUES(?,'old')")
        .bind(KEY)
        .execute(&mut transaction)
        .await?;
    transaction.commit().await?;
    for sql in [
        "UPDATE records SET id=?",
        "INSERT INTO records VALUES(?,'new') ON CONFLICT(id) DO UPDATE SET id=excluded.id",
    ] {
        for key in [KEY, NEXT_KEY] {
            let mut transaction = writer.begin_write().await?;
            let answer = query::<Turso>(sql)
                .bind(key)
                .execute(&mut transaction)
                .await;
            assert!(answer.is_err(), "key assignment admitted: {sql}");
            assert!(format!("{answer:?}").contains("KeyChange"), "{answer:?}");
            transaction.rollback().await?;
        }
    }
    writer.close().await;
    Ok(())
}

#[tokio::test]
async fn native_capture_exposes_uncommitted_row_images_for_transactional_key_validation()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let writer = writer(&directory.path().join("writer.db")).await?;
    let mut transaction = writer.begin_write().await?;
    query::<Turso>("INSERT INTO records VALUES(?,'uncommitted')")
        .bind(KEY)
        .execute(&mut transaction)
        .await?;
    let captured =
        query::<Turso>("SELECT change_type,after FROM turso_cdc WHERE table_name='records'")
            .fetch_all(&mut transaction)
            .await?;
    println!("OBS uncommitted CDC row images: {}", captured.len());
    assert_eq!(captured.len(), 1);
    assert_eq!(
        captured
            .first()
            .ok_or("no captured row")?
            .try_get::<i64, _>("change_type")?,
        1
    );
    transaction.rollback().await?;
    writer.close().await;
    Ok(())
}

mod uuid_key_migrations;

#[tokio::test]
async fn existing_non_uuid_values_refuse_open_and_adoption_without_writing() -> TestResult {
    for managed in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("existing.db");
        let database = if managed {
            let writer = writer(&path).await?;
            let database = writer.database();
            writer.close().await;
            Some(database)
        } else {
            let mut connection = TursoConnectOptions::new()
                .filename(&path)
                .create_if_missing(true)
                .connect()
                .await?;
            Migrator::with_migrations(BUNDLES.first().ok_or("no base bundle")?.clone())
                .run_direct(None, &mut connection, false)
                .await?;
            connection.close().await?;
            None
        };
        let mut connection = TursoConnectOptions::new().filename(&path).connect().await?;
        connection
            .execute("INSERT INTO records VALUES('invalid','existing value')")
            .await?;
        connection.close().await?;
        let before = tokio::fs::read(&path).await?;
        let result = if let Some(database) = database {
            ReplicatedDatabase::open(
                &path,
                database,
                Schema {
                    index: 0,
                    tables: true,
                },
                LeaseHolder::provisional(database, peer()?, policy()),
            )
            .await
        } else {
            ReplicatedDatabase::adopt(
                &path,
                Schema {
                    index: 0,
                    tables: true,
                },
                peer()?,
                policy(),
                WriterOptions::default(),
            )
            .await
        };
        let error = match result {
            Ok(writer) => {
                writer.close().await;
                return Err("existing non-v7 key accepted".into());
            }
            Err(error) => error,
        };
        assert!(
            matches!(
                error,
                OpenError::WriteRefused {
                    refusal: WriteRefusal::InvalidKey { .. }
                }
            ),
            "{error}"
        );
        assert_eq!(tokio::fs::read(&path).await?, before);
    }
    Ok(())
}
