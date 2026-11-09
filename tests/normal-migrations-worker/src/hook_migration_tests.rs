//! Exercise portable checked hook receipts on real Durable Object SQLite.
use crate::WorkerScope;
use fleet_sync_receiver::{AppliedPiece, HookError, ReceiverRules, initialize_hook};
use sqlx::{Executor, Row};
use sqlx_core::migrate::{Migration, MigrationType};
use sqlx_core::sql_str::SqlSafeStr;
use sqlx_durable_object::{Do, DoConnection};
use std::sync::LazyLock;

struct LocalHook(&'static [Migration]);
impl ReceiverRules<WorkerScope<'_>> for LocalHook {
    type Protocol = fleet_sync_protocol::NoConsumer;
    type FollowUp = ();
    fn local_migrations(&self) -> &'static [Migration] {
        self.0
    }
    async fn on_applied(
        &self,
        _: &mut WorkerScope<'_>,
        _: &AppliedPiece<'_, ()>,
    ) -> Result<Vec<()>, HookError> {
        Ok(Vec::new())
    }
}

pub(crate) async fn run(connection: &DoConnection) -> Result<serde_json::Value, sqlx::Error> {
    static INITIAL: LazyLock<Vec<Migration>> = LazyLock::new(|| {
        vec![Migration::new(
            1,
            "local hook".into(),
            MigrationType::Simple,
            "CREATE TABLE hook_effect(value TEXT); INSERT INTO hook_effect VALUES('once');"
                .into_sql_str(),
            false,
        )]
    });
    static CHANGED: LazyLock<Vec<Migration>> = LazyLock::new(|| {
        vec![Migration::new(
            1,
            "local hook".into(),
            MigrationType::Simple,
            "CREATE TABLE hook_effect(value TEXT); INSERT INTO hook_effect VALUES('edited');"
                .into_sql_str(),
            false,
        )]
    });
    connection
        .transaction(|transaction| async move {
            let corpus = sqlx::migrate!("../../crates/fleet-sync-receiver/migrations");
            for migration in corpus.iter() {
                (&transaction).execute(migration.sql.clone()).await?;
            }
            initialize_hook(&mut WorkerScope(&transaction), &LocalHook(&INITIAL))
                .await
                .map_err(crate::storage_error)?;
            Ok::<_, sqlx::Error>(())
        })
        .await?;
    connection
        .transaction(|transaction| async move {
            initialize_hook(&mut WorkerScope(&transaction), &LocalHook(&INITIAL))
                .await
                .map_err(crate::storage_error)?;
            Ok::<_, sqlx::Error>(())
        })
        .await?;
    let changed = connection
        .transaction(|transaction| async move {
            match initialize_hook(&mut WorkerScope(&transaction), &LocalHook(&CHANGED)).await {
                Err(fleet_sync_receiver::ReceiverError::InvalidInput { field })
                    if field == "hook migration checksum changed" =>
                {
                    Err(sqlx::Error::Protocol("edited checksum refused".into()))
                }
                Err(error) => Err(crate::storage_error(error)),
                Ok(()) => Ok(()),
            }
        })
        .await;
    let refused =
        matches!(changed,Err(sqlx::Error::Protocol(message)) if message=="edited checksum refused");
    let row = sqlx::query::<Do>("SELECT CAST((SELECT count(*) FROM hook_effect) AS TEXT) AS effects,CAST((SELECT count(*) FROM _fleet_sync_hook_migrations) AS TEXT) AS receipts,typeof(version) AS version_class,typeof(checksum) AS checksum_class FROM _fleet_sync_hook_migrations").fetch_one(connection).await?;
    Ok(
        serde_json::json!({"effects":row.try_get::<String,_>("effects")?,"receipts":row.try_get::<String,_>("receipts")?,"versionClass":row.try_get::<String,_>("version_class")?,"checksumClass":row.try_get::<String,_>("checksum_class")?,"editedRefused":refused}),
    )
}
