//! Local migration receipts survive retries, reject changes, and preserve replicated rows.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_rule_fixture::*, *};
use durable_steps::CallId;
use fleet_sync_protocol::NoConsumer;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    migrate::{Migration, MigrationType, Migrator},
    row::Row,
    sql_str::SqlSafeStr,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::sync::LazyLock;

struct LocalHook(&'static [Migration]);
impl ReceiverRules<TursoScope<'_>> for LocalHook {
    type Protocol = NoConsumer;
    type FollowUp = ();
    fn local_migrations(&self) -> &'static [Migration] {
        self.0
    }
    async fn on_applied(
        &self,
        _: &mut TursoScope<'_>,
        _: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<()>, HookError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn legacy_hook_receipts_adopt_atomically_and_wrong_checksums_refuse() -> TestResult {
    static INITIAL: LazyLock<Vec<Migration>> = LazyLock::new(|| {
        vec![migration(
            1,
            "CREATE TABLE hook_records(value TEXT PRIMARY KEY);",
        )]
    });
    for wrong_checksum in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("legacy-copy.sqlite");
        let options = TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let mut connection = options.connect().await?;
        let mut migrator = Migrator::with_migrations(
            RECEIVER_MIGRATOR
                .iter()
                .filter(|migration| migration.version <= 3)
                .cloned()
                .collect(),
        );
        migrator.dangerous_set_table_name("_fleet_sync_copy_migrations");
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        migrator.run_direct(None, &mut *transaction, false).await?;
        transaction.execute("CREATE TABLE hook_records(value TEXT PRIMARY KEY); INSERT INTO hook_records VALUES('retained')").await?;
        let original = INITIAL.first().ok_or("missing hook fixture")?;
        let checksum = if wrong_checksum {
            "[0]".into()
        } else {
            serde_json::to_string(original.checksum.as_ref())?
        };
        sqlx_core::query::query::<Turso>(
            "INSERT INTO _fleet_sync_hook_migrations(version,checksum) VALUES('1',?)",
        )
        .bind(&checksum)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        connection.close().await?;
        let mut connection = options.connect().await?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let mut migrator = Migrator::with_migrations(RECEIVER_MIGRATOR.iter().cloned().collect());
        migrator.dangerous_set_table_name("_fleet_sync_copy_migrations");
        migrator.run_direct(None, &mut *transaction, false).await?;
        let result = initialize_hook(&mut TursoScope(&mut transaction), &LocalHook(&INITIAL)).await;
        if wrong_checksum {
            assert!(
                matches!(result,Err(ReceiverError::InvalidInput{field}) if field=="hook migration checksum changed")
            );
            transaction.rollback().await?;
            let row = sqlx_core::query::query::<Turso>(
                "SELECT checksum FROM _fleet_sync_hook_migrations WHERE version='1'",
            )
            .fetch_one(&mut connection)
            .await?;
            assert_eq!(row.try_get::<String, _>("checksum")?, "[0]");
        } else {
            result?;
            transaction.commit().await?;
            let row = sqlx_core::query::query::<Turso>("SELECT version,description,CAST(success AS INTEGER) AS success,checksum,execution_time FROM _fleet_sync_hook_migrations").fetch_one(&mut connection).await?;
            assert_eq!(row.try_get::<i64, _>("version")?, 1);
            assert_eq!(row.try_get::<String, _>("description")?, "hook migration");
            assert_eq!(row.try_get::<i64, _>("success")?, 1);
            assert_eq!(
                row.try_get::<Vec<u8>, _>("checksum")?,
                original.checksum.as_ref()
            );
            assert_eq!(row.try_get::<i64, _>("execution_time")?, -1);
            assert!(
                sqlx_core::query::query::<Turso>(
                    "SELECT name FROM sqlite_schema WHERE name='_fleet_sync_legacy_hook_migrations'"
                )
                .fetch_all(&mut connection)
                .await?
                .is_empty()
            );
        }
        let row = sqlx_core::query::query::<Turso>("SELECT value FROM hook_records")
            .fetch_one(&mut connection)
            .await?;
        assert_eq!(row.try_get::<String, _>("value")?, "retained");
        connection.close().await?;
    }
    Ok(())
}
impl FollowUpHandler for LocalHook {
    type Item = ();
    async fn follow_up(&self, _: &(), _: CallId) -> Result<FollowUpDone, FollowUpFailure> {
        Ok(FollowUpDone)
    }
}

fn migration(version: i64, sql: &'static str) -> Migration {
    Migration::new(
        version,
        "hook migration".into(),
        MigrationType::Simple,
        sql.into_sql_str(),
        false,
    )
}

#[tokio::test]
async fn hook_migrations_run_once_and_changed_checksum_refuses_before_writing() -> TestResult {
    static INITIAL: LazyLock<Vec<Migration>> = LazyLock::new(|| {
        vec![migration(
            1,
            "CREATE TABLE hook_records(value TEXT PRIMARY KEY);",
        )]
    });
    static UPGRADE: LazyLock<Vec<Migration>> = LazyLock::new(|| {
        vec![
            migration(1, "CREATE TABLE hook_records(value TEXT PRIMARY KEY);"),
            migration(
                2,
                "CREATE TABLE local_marks(value TEXT PRIMARY KEY); INSERT INTO local_marks(value) VALUES('once'); CREATE INDEX local_values ON local_marks(value); DROP INDEX local_values;",
            ),
        ]
    });
    static CHANGED: LazyLock<Vec<Migration>> = LazyLock::new(|| {
        vec![migration(
            1,
            "CREATE TABLE hook_records(value TEXT PRIMARY KEY); INSERT INTO hook_records(value) VALUES('bad');",
        )]
    });
    let mut connection = connection().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    initialize_hook(&mut TursoScope(&mut transaction), &LocalHook(&INITIAL)).await?;
    initialize_hook(&mut TursoScope(&mut transaction), &LocalHook(&UPGRADE)).await?;
    transaction.commit().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    initialize_hook(&mut TursoScope(&mut transaction), &LocalHook(&UPGRADE)).await?;
    transaction.commit().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(
        matches!(initialize_hook(&mut TursoScope(&mut transaction), &LocalHook(&CHANGED)).await, Err(ReceiverError::InvalidInput { field }) if field == "hook migration checksum changed")
    );
    transaction.rollback().await?;
    let row = sqlx_core::query::query::<Turso>("SELECT CAST((SELECT count(*) FROM local_marks) AS TEXT) AS marks, CAST((SELECT count(*) FROM hook_records) AS TEXT) AS hooks, CAST((SELECT count(*) FROM _fleet_sync_hook_migrations) AS TEXT) AS migrations").fetch_one(&mut connection).await?;
    assert_eq!(row.try_get::<String, _>("marks")?, "1");
    assert_eq!(row.try_get::<String, _>("hooks")?, "0");
    assert_eq!(row.try_get::<String, _>("migrations")?, "2");
    let columns =
        sqlx_core::query::query::<Turso>("PRAGMA table_info(_fleet_sync_hook_migrations)")
            .fetch_all(&mut connection)
            .await?;
    let shape = columns
        .iter()
        .map(|row| {
            Ok((
                row.try_get::<String, _>("name")?,
                row.try_get::<String, _>("type")?,
            ))
        })
        .collect::<Result<Vec<_>, sqlx_core::Error>>()?;
    assert_eq!(
        shape,
        vec![
            ("version".into(), "BIGINT".into()),
            ("description".into(), "TEXT".into()),
            ("installed_on".into(), "TIMESTAMP".into()),
            ("success".into(), "BOOLEAN".into()),
            ("checksum".into(), "BLOB".into()),
            ("execution_time".into(), "BIGINT".into()),
        ]
    );
    let receipts = sqlx_core::query::query::<Turso>(
        "SELECT version,description,CAST(installed_on AS TEXT) AS installed_on,CAST(success AS INTEGER) AS success,checksum,execution_time,typeof(version) AS version_class,typeof(checksum) AS checksum_class FROM _fleet_sync_hook_migrations ORDER BY version",
    )
    .fetch_all(&mut connection)
    .await?;
    for (receipt, migration) in receipts.iter().zip(UPGRADE.iter()) {
        assert_eq!(receipt.try_get::<i64, _>("version")?, migration.version);
        assert_eq!(receipt.try_get::<String, _>("version_class")?, "integer");
        assert_eq!(
            receipt.try_get::<String, _>("description")?,
            if migration.version == 1 {
                "local hook records"
            } else {
                "hook migration"
            }
        );
        assert!(!receipt.try_get::<String, _>("installed_on")?.is_empty());
        assert_eq!(receipt.try_get::<i64, _>("success")?, 1);
        assert_eq!(receipt.try_get::<String, _>("checksum_class")?, "blob");
        assert_eq!(
            receipt.try_get::<Vec<u8>, _>("checksum")?,
            migration.checksum.as_ref()
        );
        assert_eq!(receipt.try_get::<i64, _>("execution_time")?, -1);
    }
    Ok(())
}

#[tokio::test]
async fn hook_migration_cannot_modify_a_replicated_table_and_rolls_back_its_earlier_statement()
-> TestResult {
    static BAD: LazyLock<Vec<Migration>> = LazyLock::new(|| {
        vec![
            migration(1, "CREATE TABLE hook_records(value TEXT PRIMARY KEY);"),
            migration(
                2,
                "CREATE TABLE local_marks(value TEXT PRIMARY KEY); INSERT INTO records(id,body) VALUES(1,'wrong owner');",
            ),
        ]
    });
    let mut connection = connection().await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    assert!(matches!(
        initialize_hook(&mut TursoScope(&mut transaction), &LocalHook(&BAD)).await,
        Err(ReceiverError::InvalidInput { .. })
    ));
    transaction.rollback().await?;
    let row = sqlx_core::query::query::<Turso>("SELECT CAST((SELECT count(*) FROM sqlite_schema WHERE name='local_marks') AS TEXT) AS local_table, CAST((SELECT count(*) FROM records) AS TEXT) AS records, CAST((SELECT count(*) FROM _fleet_sync_hook_migrations) AS TEXT) AS migrations").fetch_one(&mut connection).await?;
    assert_eq!(row.try_get::<String, _>("local_table")?, "0");
    assert_eq!(row.try_get::<String, _>("records")?, "0");
    assert_eq!(row.try_get::<String, _>("migrations")?, "1");
    Ok(())
}
