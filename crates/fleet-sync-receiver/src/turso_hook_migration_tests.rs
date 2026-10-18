//! Local migration receipts survive retries, reject changes, and preserve replicated rows.
#![allow(clippy::panic_in_result_fn)]
use crate::{turso_rule_fixture::*, *};
use durable_steps::CallId;
use fleet_sync_protocol::{HookRefusal, NoConsumer};
use sqlx_core::{
    connection::Connection,
    migrate::{Migration, MigrationType},
    row::Row,
    sql_str::SqlSafeStr,
};
use sqlx_turso::Turso;
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
    ) -> Result<Vec<()>, HookRefusal> {
        Ok(Vec::new())
    }
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
