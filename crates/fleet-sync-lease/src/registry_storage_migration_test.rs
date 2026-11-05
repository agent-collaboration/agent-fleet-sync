#![allow(clippy::unwrap_used, clippy::panic_in_result_fn)]
use crate::{turso_registry_fixture::*, *};
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_core::{migrate::Migrator, query::query, row::Row};
use sqlx_turso::Turso;

async fn schema_rows(store: &TestStore) -> TestResult<Vec<(String, String)>> {
    Ok(store.transaction(|scope|Box::pin(async move {
        query::<Turso>("SELECT name,sql FROM sqlite_schema WHERE name LIKE '_fleet_sync_%' AND sql IS NOT NULL ORDER BY name").fetch_all(scope.executor()).await?.into_iter().map(|row|Ok((row.try_get("name")?,row.try_get("sql")?))).collect()
    })).await?)
}
async fn legacy_identity_refusal(case: &'static str) -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&directory.path().join("registry.db"), host, false).await?;
    let database = database()?;
    let other = DatabaseId::new(durable_steps::Uuid::from_u128(
        0x019eef0000007000800000000000076c,
    ))?;
    let token = "019eef00-0000-7000-8000-000000000011";
    let (insert,read,key,body)=match case {
        "database"=>("INSERT INTO _fleet_sync_databases(database,record) VALUES(?1,?2)","SELECT record AS value FROM _fleet_sync_databases",database.to_string(),serde_json::json!({"database":other,"lifecycle":{"state":"unheld","importDigest":ImportDigest::new("a".repeat(64))?},"revision":{"epoch":1,"change":1},"changed_at":1000}).to_string()),
        "preview"=>("INSERT INTO _fleet_sync_takeover_previews(token,database,record) VALUES(?1,'019eef00-0000-7000-8000-000000000041',?2)","SELECT record AS value FROM _fleet_sync_takeover_previews",token.into(),serde_json::json!({"database":"019eef00-0000-7000-8000-000000000041","preview":{"token":"019eef00-0000-4000-8000-000000000011","fence":1,"acked":{"epoch":1,"sequence":0},"holder":peer(1)?,"lastSeen":null}}).to_string()),
        _=>("INSERT INTO _fleet_sync_epoch_history(database,epoch,record) VALUES(?1,'1',?2)","SELECT record AS value FROM _fleet_sync_epoch_history",database.to_string(),serde_json::json!({"database":other,"epoch":2,"holder":peer(1)?,"granted_at":1000,"ended_at":null}).to_string()),
    };
    let original = body.clone();
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut old = Migrator::with_migrations(
                    LEASE_MIGRATOR
                        .sql_migrations()
                        .filter(|m| m.version <= 4)
                        .cloned()
                        .collect::<Vec<_>>(),
                );
                old.dangerous_set_table_name("_fleet_sync_registry_migrations");
                old.run_direct(None, scope.executor(), false)
                    .await
                    .map_err(storage)?;
                query::<Turso>(insert)
                    .bind(key)
                    .bind(body)
                    .execute(scope.executor())
                    .await?;
                Ok(())
            })
        })
        .await?;
    let before = schema_rows(&store).await?;
    assert_eq!(scalar(&store, read).await?, original);
    let result = store
        .transaction(|scope| {
            Box::pin(async move {
                let all = LEASE_MIGRATOR.with_history_table("_fleet_sync_registry_migrations");
                all.run_direct(None, scope.executor(), false)
                    .await
                    .map_err(storage)
            })
        })
        .await;
    assert!(
        result.is_err(),
        "migration accepted contradictory legacy identity: {case}"
    );
    assert_eq!(
        schema_rows(&store).await?,
        before,
        "partial schema after refused {case}"
    );
    assert_eq!(
        scalar(&store, read).await?,
        original,
        "legacy row changed after refused {case}"
    );
    Ok(())
}

#[tokio::test]
async fn legacy_database_identity_mismatch_refuses_migration_atomically() -> TestResult {
    legacy_identity_refusal("database").await
}
#[tokio::test]
async fn legacy_preview_token_mismatch_refuses_migration_atomically() -> TestResult {
    legacy_identity_refusal("preview").await
}
#[tokio::test]
async fn legacy_history_identity_mismatch_refuses_migration_atomically() -> TestResult {
    legacy_identity_refusal("history").await
}
