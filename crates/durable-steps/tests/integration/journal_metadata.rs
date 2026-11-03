use super::legacy_rows::{
    LegacyOperationRow, LegacyReceiptRow, LegacyStepRow, OPERATIONS, RECEIPTS, STEPS,
};
use crate::support::TestHost;
use durable_steps::{
    AtomicStore, CallTarget, DriveLease, Engine, JournalScope, OperationContext, OperationFailure,
    OperationId, OperationState, RequestKey, StepName, StoreError, SyncTime, Task, Uuid,
    storage::{JournalStorage, StepRecord, StepState, TransactionScope},
    turso_store::{TursoStore, migrate_journal},
};
use runtime_primitives::canonical_uuid_v7;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    query::query,
    query_as::query_as,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
use std::{
    path::Path,
    sync::{Arc, atomic::Ordering},
};

pub(super) type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const OPERATION: &str = "0197abc0-def0-7123-8123-abcdef012345";
const TOKEN: Uuid = Uuid::from_u128(0x0197abc0def071238123abcdef012346);

pub(super) struct ReplayTask;
#[derive(serde::Serialize, serde::Deserialize)]
struct Double;
impl CallTarget for Double {
    const KIND: &'static str = "metadata-double";
    type Request = u64;
    type Answer = u64;
}
impl Task for ReplayTask {
    const KIND: &'static str = "metadata-replay";
    const VERSION: u32 = 1;
    type Store = TursoStore<TestHost>;
    type Input = u64;
    type Output = u64;
    async fn run<C: OperationContext<Store = Self::Store>>(
        &self,
        ctx: &mut C,
        input: &u64,
    ) -> std::result::Result<u64, OperationFailure> {
        let input = *input;
        let saved = ctx
            .local(
                StepName::new("local").map_err(|error| OperationFailure::InvalidPayload {
                    message: error.to_string(),
                })?,
                move |scope| {
                    Box::pin(async move {
                        query::<Turso>("UPDATE effects SET value=value+1")
                            .execute(scope.executor())
                            .await
                            .map_err(StoreError::backend)?;
                        Ok(input)
                    })
                },
            )
            .await?;
        ctx.call(
            StepName::new("remote").map_err(|error| OperationFailure::InvalidPayload {
                message: error.to_string(),
            })?,
            Double,
            saved,
        )
        .await
        .map_err(Into::into)
    }
}

pub(super) async fn connect(path: &Path) -> Result<TursoConnection> {
    Ok(TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .foreign_keys(true)
        .connect()
        .await?)
}

// Frozen v1 row encoding: this deliberately does not seed through the new adapter.
// It catches migrations that change saved bytes, lose the pending call, or replay local effects.
pub(super) async fn legacy_file(path: &Path) -> Result<TursoConnection> {
    let mut connection = connect(path).await?;
    let corpus = sqlx::migrate!("./migrations");
    let mut migrator = sqlx_core::migrate::Migrator::with_migrations(
        corpus
            .iter()
            .filter(|migration| migration.version == 1)
            .cloned()
            .collect(),
    );
    migrator.dangerous_set_table_name("_fleet_sync_migrations");
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    migrator.run_direct(None, &mut *transaction, false).await?;
    query::<Turso>("UPDATE _fleet_sync_migrations SET description='historical v1 journal',installed_on='2000-01-02 03:04:05',execution_time=424242 WHERE version=1").execute(&mut *transaction).await?;
    transaction
        .execute(include_str!("fixtures/v1_pending.sql"))
        .await?;
    transaction.commit().await?;
    Ok(connection)
}

pub(super) async fn history(connection: &mut TursoConnection) -> Result<Vec<LegacyStepRow>> {
    Ok(query_as::<Turso, LegacyStepRow>(STEPS)
        .fetch_all(connection)
        .await?)
}

pub(super) async fn operations(
    connection: &mut TursoConnection,
) -> Result<Vec<LegacyOperationRow>> {
    Ok(query_as::<Turso, LegacyOperationRow>(OPERATIONS)
        .fetch_all(connection)
        .await?)
}
pub(super) async fn receipts(connection: &mut TursoConnection) -> Result<Vec<LegacyReceiptRow>> {
    Ok(query_as::<Turso, LegacyReceiptRow>(RECEIPTS)
        .fetch_all(connection)
        .await?)
}

pub(super) async fn metadata(
    connection: &mut TursoConnection,
    table: &str,
    key: &str,
) -> Result<Vec<(String, i64, i64)>> {
    let sql = format!("SELECT {key} AS identity,created_at,updated_at FROM {table} ORDER BY {key}");
    Ok(query::<Turso>(sqlx_core::sql_str::AssertSqlSafe(sql))
        .fetch_all(connection)
        .await?
        .into_iter()
        .map(|row| {
            Ok((
                row.try_get("identity")?,
                row.try_get("created_at")?,
                row.try_get("updated_at")?,
            ))
        })
        .collect::<std::result::Result<_, sqlx_core::Error>>()?)
}

#[tokio::test]
async fn native_upgrade_preserves_saved_history_and_replays_it_once() -> Result {
    for home in ["fleet", "project", "machine"] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join(format!("{home}.sqlite"));
        let mut connection = legacy_file(&path).await?;
        let before = history(&mut connection).await?;
        let operation_before = operations(&mut connection).await?;
        let receipt_before = receipts(&mut connection).await?;
        assert_eq!(operation_before[1].attempt, 7);
        assert_eq!(operation_before[1].kind_version, 9);
        assert_eq!(operation_before[1].drive_until, Some(5_000_000_001_000));
        assert_eq!(before[2].first_attempt_at, Some(400));
        assert_eq!(before[2].may_have_reached, Some(1));
        assert!(before[2].intent.as_deref().is_some_and(
            |intent| intent.contains("may_have_reached") && intent.contains("retained retry")
        ));
        connection.close().await?;
        let host = TestHost::new();
        let store = Arc::new(TursoStore::open_and_migrate(&path, host.clone()).await?);
        let mut connection = connect(&path).await?;
        assert_eq!(history(&mut connection).await?, before);
        assert_eq!(operations(&mut connection).await?, operation_before);
        let receipt_after = receipts(&mut connection).await?;
        assert_eq!(&receipt_after[..1], receipt_before.as_slice());
        assert_eq!(receipt_after.len(), 2);
        for (table, key) in [
            ("_fleet_sync_operations", "operation_id"),
            ("_fleet_sync_steps", "id"),
            ("_fleet_sync_schema", "id"),
            ("_fleet_sync_migrations", "id"),
        ] {
            let rows = metadata(&mut connection, table, key).await?;
            assert!(!rows.is_empty(), "{home}: {table}");
            assert!(
                rows.iter()
                    .all(|(id, created, updated)| canonical_uuid_v7(id) && updated >= created)
            );
            let columns = query::<Turso>(sqlx_core::sql_str::AssertSqlSafe(format!(
                "PRAGMA table_info({table})"
            )))
            .fetch_all(&mut connection)
            .await?;
            let primary = columns
                .iter()
                .filter(|row| row.try_get::<i64, _>("pk").is_ok_and(|order| order > 0))
                .map(|row| row.try_get::<String, _>("name"))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            assert_eq!(
                primary,
                vec![key.to_owned()],
                "{table} must have exactly its UUIDv7 primary key"
            );
        }
        let old_times = metadata(&mut connection, "_fleet_sync_operations", "operation_id").await?;
        assert_eq!(
            old_times[0].1, 250,
            "earliest saved call attempt is the best recorded time"
        );
        assert_eq!(old_times[1].1, 400);
        let mut stable = Vec::new();
        for (table, key) in [
            ("_fleet_sync_operations", "operation_id"),
            ("_fleet_sync_steps", "id"),
            ("_fleet_sync_schema", "id"),
            ("_fleet_sync_migrations", "id"),
        ] {
            let rows = metadata(&mut connection, table, key).await?;
            assert!(
                rows.iter().all(|(_, created, updated)| *created != 0
                    && *updated != 0
                    && created <= updated)
            );
            stable.push(rows);
        }
        let receipt_times =
            query::<Turso>("SELECT created_at FROM _fleet_sync_migrations WHERE version=1")
                .fetch_one(&mut connection)
                .await?;
        assert_eq!(
            receipt_times.try_get::<i64, _>("created_at")?,
            946_782_245_000
        );
        let step_times=query::<Turso>("SELECT operation_id,position,created_at FROM _fleet_sync_steps ORDER BY operation_id,position").fetch_all(&mut connection).await?;
        assert_eq!(
            step_times
                .iter()
                .map(|row| row.try_get::<i64, _>("created_at"))
                .collect::<std::result::Result<Vec<_>, _>>()?,
            vec![250, 250, 400]
        );
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        migrate_journal(&mut transaction).await?;
        transaction.commit().await?;
        for ((table, key), before) in [
            ("_fleet_sync_operations", "operation_id"),
            ("_fleet_sync_steps", "id"),
            ("_fleet_sync_schema", "id"),
            ("_fleet_sync_migrations", "id"),
        ]
        .into_iter()
        .zip(stable)
        {
            assert_eq!(metadata(&mut connection, table, key).await?, before);
        }
        connection.close().await?;
        let replayed = store
            .transaction(|scope| {
                Box::pin(async move {
                    Ok(scope
                        .admit::<ReplayTask>(
                            &RequestKey::new("old-request").map_err(StoreError::backend)?,
                            &7,
                        )
                        .await
                        .map_err(StoreError::backend)?
                        .id())
                })
            })
            .await?;
        assert_eq!(replayed, OperationId::try_from(OPERATION.to_owned())?);
        let mut engine = Engine::new(store.clone(), host.clone());
        engine.register(ReplayTask)?;
        engine.drive_due().await?;
        let id = OperationId::try_from(OPERATION.to_owned())?;
        assert!(
            matches!(engine.status(&id).await?.state, OperationState::Completed{result} if result==b"14")
        );
        assert_eq!(host.deliveries.load(Ordering::SeqCst), 1);
        assert_eq!(host.effects.load(Ordering::SeqCst), 1);
        store
            .transaction(|scope| {
                Box::pin(async move {
                    let row = query::<Turso>("SELECT value FROM effects")
                        .fetch_one(scope.executor())
                        .await?;
                    assert_eq!(
                        row.try_get::<i64, _>("value")?,
                        1,
                        "saved local step must not run again"
                    );
                    Ok(())
                })
            })
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_upgrade_rolls_back_with_its_callers_transaction() -> Result {
    let directory = tempfile::tempdir()?;
    let mut connection = legacy_file(&directory.path().join("rollback.sqlite")).await?;
    let before = history(&mut connection).await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    migrate_journal(&mut transaction).await?;
    assert_eq!(
        metadata(&mut transaction, "_fleet_sync_steps", "id")
            .await?
            .len(),
        3
    );
    transaction.rollback().await?;
    assert_eq!(history(&mut connection).await?, before);
    let columns = query::<Turso>("PRAGMA table_info(_fleet_sync_steps)")
        .fetch_all(&mut connection)
        .await?;
    assert!(!columns.iter().any(|row| {
        row.try_get::<String, _>("name")
            .is_ok_and(|name| name == "id")
    }));
    let versions = query::<Turso>("SELECT version FROM _fleet_sync_migrations")
        .fetch_all(&mut connection)
        .await?;
    assert_eq!(versions.len(), 1);
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    migrate_journal(&mut transaction).await?;
    transaction.commit().await?;
    assert_eq!(
        metadata(&mut connection, "_fleet_sync_steps", "id")
            .await?
            .len(),
        3
    );
    Ok(())
}

#[tokio::test]
async fn fresh_writes_preserve_step_identity_and_creation_time_on_update() -> Result {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("fresh.sqlite");
    let host = TestHost::new();
    let store = TursoStore::open_and_migrate(&path, host.clone()).await?;
    let id = store
        .transaction(|scope| {
            Box::pin(async move {
                Ok(scope
                    .admit::<ReplayTask>(
                        &RequestKey::new("fresh").map_err(StoreError::backend)?,
                        &7,
                    )
                    .await
                    .map_err(StoreError::backend)?
                    .id())
            })
        })
        .await?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                scope
                    .claim(
                        id,
                        SyncTime::from_unix_millis(1000),
                        DriveLease {
                            token: TOKEN,
                            until: SyncTime::from_unix_millis(5000),
                        },
                    )
                    .await?;
                scope
                    .save_step(
                        StepRecord {
                            operation: id,
                            position: 0,
                            name: StepName::new("local").map_err(StoreError::backend)?,
                            state: StepState::Local {
                                output: b"7".to_vec(),
                            },
                        },
                        TOKEN,
                    )
                    .await
            })
        })
        .await?;
    let mut connection = connect(&path).await?;
    let before = metadata(&mut connection, "_fleet_sync_steps", "id").await?;
    assert!(canonical_uuid_v7(&before[0].0));
    assert_eq!((before[0].1, before[0].2), (1000, 1000));
    host.advance(100);
    store
        .transaction(move |scope| {
            Box::pin(async move {
                scope
                    .save_step(
                        StepRecord {
                            operation: id,
                            position: 0,
                            name: StepName::new("local").map_err(StoreError::backend)?,
                            state: StepState::Local {
                                output: b"8".to_vec(),
                            },
                        },
                        TOKEN,
                    )
                    .await
            })
        })
        .await?;
    let after = metadata(&mut connection, "_fleet_sync_steps", "id").await?;
    assert_eq!(after, vec![(before[0].0.clone(), 1000, 1100)]);
    let operations = metadata(&mut connection, "_fleet_sync_operations", "operation_id").await?;
    assert_eq!((operations[0].1, operations[0].2), (1000, 1000));
    host.advance(100);
    let rolled_back = store
        .transaction(move |scope| {
            Box::pin(async move {
                scope
                    .save_step(
                        StepRecord {
                            operation: id,
                            position: 0,
                            name: StepName::new("local").map_err(StoreError::backend)?,
                            state: StepState::Local {
                                output: b"9".to_vec(),
                            },
                        },
                        TOKEN,
                    )
                    .await?;
                Err::<(), _>(StoreError::Aborted {
                    reason: "requested rollback".into(),
                })
            })
        })
        .await;
    assert!(matches!(rolled_back,Err(StoreError::Aborted{reason}) if reason=="requested rollback"));
    assert_eq!(
        metadata(&mut connection, "_fleet_sync_steps", "id").await?,
        after
    );
    store
        .transaction(move |scope| {
            Box::pin(async move {
                scope
                    .retry(id, TOKEN, SyncTime::from_unix_millis(2000), 1)
                    .await
            })
        })
        .await?;
    let operations = metadata(&mut connection, "_fleet_sync_operations", "operation_id").await?;
    assert_eq!((operations[0].1, operations[0].2), (1000, 1200));
    Ok(())
}

#[tokio::test]
async fn reopened_step_rows_refuse_noncanonical_and_nontext_surrogate_ids() -> Result {
    for (offered, blob) in [
        (OPERATION.replace("7123", "4123"), false),
        (OPERATION.to_uppercase(), false),
        (OPERATION.replace('-', ""), false),
        ("not-an-id".to_owned(), false),
        (OPERATION.to_owned(), true),
    ] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bad-step.sqlite");
        legacy_file(&path).await?.close().await?;
        let host = TestHost::new();
        let store = TursoStore::open_and_migrate(&path, host.clone()).await?;
        drop(store);
        let mut connection = connect(&path).await?;
        let update = query::<Turso>("UPDATE _fleet_sync_steps SET id=? WHERE position=0");
        if blob {
            update
                .bind(offered.as_bytes())
                .execute(&mut connection)
                .await?;
        } else {
            update.bind(offered).execute(&mut connection).await?;
        }
        connection.close().await?;
        let store = TursoStore::open_and_migrate(&path, host).await?;
        let id = OperationId::try_from(OPERATION.to_owned())?;
        let result = store
            .transaction(move |scope| Box::pin(async move { scope.step(id, 0).await }))
            .await;
        assert!(
            matches!(result,Err(StoreError::Corrupt{field,message}) if field=="step.id" && message.contains(OPERATION) && message.contains("position 0"))
        );
    }
    Ok(())
}

#[tokio::test]
async fn bootstrap_refuses_invalid_schema_and_native_receipt_row_ids() -> Result {
    for table in ["_fleet_sync_schema", "_fleet_sync_migrations"] {
        for (offered, blob) in [
            (OPERATION.replace("7123", "4123"), false),
            (OPERATION.to_uppercase(), false),
            (OPERATION.replace('-', ""), false),
            ("not-an-id".to_owned(), false),
            (OPERATION.to_owned(), true),
        ] {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("bad-metadata.sqlite");
            let host = TestHost::new();
            drop(TursoStore::open_and_migrate(&path, host.clone()).await?);
            let mut connection = connect(&path).await?;
            let sql = match table {
                "_fleet_sync_schema" => "UPDATE _fleet_sync_schema SET id=?",
                _ => "UPDATE _fleet_sync_migrations SET id=? WHERE version=1",
            };
            let update = query::<Turso>(sql);
            if blob {
                update
                    .bind(offered.as_bytes())
                    .execute(&mut connection)
                    .await?;
            } else {
                update.bind(offered).execute(&mut connection).await?;
            }
            connection.close().await?;
            let result = TursoStore::open_and_migrate(&path, host).await;
            assert!(
                matches!(result,Err(StoreError::Corrupt{field,message}) if field==format!("{table}.id") && message.contains("version"))
            );
        }
    }
    Ok(())
}
