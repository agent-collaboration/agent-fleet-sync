//! The registry's validated records, rather than copy files, own its inventory.
#![allow(clippy::panic_in_result_fn)]
use crate::{
    registry_records::DatabaseRecord, registry_storage::RegistryStorage,
    turso_change_tests::register_first, turso_registry_fixture::*, *,
};
use durable_steps::{AtomicStore, Uuid, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_turso::Turso;
use std::sync::Arc;

#[tokio::test]
async fn database_inventory_is_read_only_and_excludes_retired_and_unregistered_ids() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    let ids = store
        .transaction(|scope| Box::pin(async move { database_ids(scope).await.map_err(storage) }))
        .await?;
    assert!(ids.is_empty());
    register_first(&engine).await?;
    let imported = DatabaseId::new(Uuid::from_u128(2000))?;
    let retiring = DatabaseId::new(Uuid::from_u128(2001))?;
    let retired = DatabaseId::new(Uuid::from_u128(2002))?;
    let unregistered = DatabaseId::new(Uuid::from_u128(2003))?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut registry = RegistryStorage(scope);
                for (database, lifecycle) in [
                    (
                        imported,
                        DatabaseLifecycle::Unheld {
                            import_digest: ImportDigest::new("a".repeat(64)).map_err(storage)?,
                        },
                    ),
                    (
                        retiring,
                        DatabaseLifecycle::Retiring {
                            attempt: RequestId::new(Uuid::from_u128(2004)).map_err(storage)?,
                        },
                    ),
                    (
                        retired,
                        DatabaseLifecycle::Retired {
                            at: SyncTime::from_unix_millis(100),
                        },
                    ),
                ] {
                    registry
                        .save_database(&DatabaseRecord {
                            database,
                            lifecycle,
                        })
                        .await
                        .map_err(storage)?;
                }
                Ok(())
            })
        })
        .await?;
    let calls = host.calls.lock().await.len();
    let ids = store
        .transaction(|scope| Box::pin(async move { database_ids(scope).await.map_err(storage) }))
        .await?;
    assert_eq!(ids, vec![database()?, imported, retiring]);
    assert!(!ids.contains(&retired));
    assert!(!ids.contains(&unregistered));
    assert_eq!(host.calls.lock().await.len(), calls);
    assert_eq!(
        scalar(
            &store,
            "SELECT CAST(count(*) AS TEXT) AS value FROM _fleet_sync_databases"
        )
        .await?,
        "4"
    );
    assert_eq!(
        scalar(&store, "SELECT epoch AS value FROM _fleet_sync_epoch_floor").await?,
        "1"
    );
    Ok(())
}

#[tokio::test]
async fn database_inventory_fails_closed_if_a_stored_record_has_another_identity() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    register_first(&engine).await?;
    store
        .transaction(|scope| {
            Box::pin(async move {
                sqlx_core::query::query::<Turso>(
                    "UPDATE _fleet_sync_databases SET database='different-key'",
                )
                .execute(scope.executor())
                .await?;
                assert!(matches!(
                    database_ids(scope).await,
                    Err(LeaseError::Corrupt { .. })
                ));
                Ok(())
            })
        })
        .await?;
    Ok(())
}
