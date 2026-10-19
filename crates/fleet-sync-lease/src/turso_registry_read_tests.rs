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
    let imported = DatabaseId::new(Uuid::from_u128(0x019eef000000700080000000000007d0))?;
    let retiring = DatabaseId::new(Uuid::from_u128(0x019eef000000700080000000000007d1))?;
    let retired = DatabaseId::new(Uuid::from_u128(0x019eef000000700080000000000007d2))?;
    let unregistered = DatabaseId::new(Uuid::from_u128(0x019eef000000700080000000000007d3))?;
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
                            attempt: RequestId::new(Uuid::from_u128(
                                0x019eef000000700080000000000007d4,
                            ))
                            .map_err(storage)?,
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

async fn historical_grant(store: &TestStore, epoch: u64) -> TestResult<Option<EpochGrant>> {
    let database = database()?;
    let epoch = Epoch::new(epoch)?;
    Ok(store
        .transaction(move |scope| {
            Box::pin(async move { epoch_holder(scope, database, epoch).await.map_err(storage) })
        })
        .await?)
}

async fn history_records(store: &TestStore) -> TestResult<Vec<String>> {
    use sqlx_core::row::Row;
    Ok(store
        .transaction(|scope| {
            Box::pin(async move {
                sqlx_core::query::query::<Turso>(
                    "SELECT record FROM _fleet_sync_epoch_history ORDER BY database,epoch",
                )
                .fetch_all(scope.executor())
                .await?
                .into_iter()
                .map(|row| {
                    row.try_get::<String, _>("record")
                        .map_err(durable_steps::StoreError::backend)
                })
                .collect()
            })
        })
        .await?)
}

#[tokio::test]
async fn epoch_holder_preserves_the_historical_grant_after_a_real_move_and_store_reopen()
-> TestResult {
    use std::sync::atomic::Ordering;
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let path = directory.path().join("registry.db");
    let store = open_store(&path, Arc::clone(&host), true).await?;
    let engine = engine(Arc::clone(&store), Arc::clone(&host))?;
    assert_eq!(historical_grant(&store, 1).await?, None);
    register_first(&engine).await?;
    assert_eq!(
        historical_grant(&store, 1).await?,
        Some(EpochGrant {
            epoch: Epoch::new(1)?,
            holder: peer(1)?,
            granted_at: SyncTime::from_unix_millis(1000),
            ended_at: None,
        })
    );
    host.now.store(1500, Ordering::SeqCst);
    let attempt = TransferId::new(Uuid::from_u128(0x019eef00000070008000000000000300))?;
    let answer = task_answer::<HolderChangeTask<TestStore, Turso>>(
        &engine,
        &holder_change_request_key(attempt)?,
        HolderChangeRequest::Move {
            attempt,
            database: database()?,
            from_epoch: Epoch::new(1)?,
            to: peer(2)?,
        },
    )
    .await?;
    let HolderChangeAnswer::Granted { epoch, start, .. } = answer else {
        return Err("move did not grant successor epoch".into());
    };
    assert_eq!(epoch, Epoch::new(2)?);
    let historical = EpochGrant {
        epoch: Epoch::new(1)?,
        holder: peer(1)?,
        granted_at: SyncTime::from_unix_millis(1000),
        ended_at: Some(start),
    };
    let current = EpochGrant {
        epoch: Epoch::new(2)?,
        holder: peer(2)?,
        granted_at: SyncTime::from_unix_millis(1500),
        ended_at: None,
    };
    let records_before = history_records(&store).await?;
    let lease_before = scalar(&store, "SELECT record AS value FROM _fleet_sync_leases").await?;
    let calls_before = host.calls.lock().await.len();
    for _ in 0..2 {
        assert_eq!(historical_grant(&store, 1).await?, Some(historical.clone()));
        assert_eq!(historical_grant(&store, 2).await?, Some(current.clone()));
        assert_eq!(historical_grant(&store, 3).await?, None);
    }
    assert_eq!(history_records(&store).await?, records_before);
    assert_eq!(
        scalar(&store, "SELECT record AS value FROM _fleet_sync_leases").await?,
        lease_before
    );
    assert_eq!(host.calls.lock().await.len(), calls_before);
    drop(engine);
    drop(store);
    let reopened = open_store(&path, host, false).await?;
    assert_eq!(historical_grant(&reopened, 1).await?, Some(historical));
    assert_eq!(historical_grant(&reopened, 2).await?, Some(current));
    assert_eq!(historical_grant(&reopened, 3).await?, None);
    Ok(())
}

#[tokio::test]
async fn epoch_holder_returns_typed_corruption_without_repairing_or_guessing_a_holder() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(
        &directory.path().join("registry.db"),
        Arc::clone(&host),
        true,
    )
    .await?;
    let engine = engine(Arc::clone(&store), host)?;
    register_first(&engine).await?;
    let original = history_records(&store)
        .await?
        .pop()
        .ok_or("missing original grant")?;
    for corruption in ["malformed", "database", "epoch", "holder", "future end"] {
        let mut damaged: serde_json::Value = serde_json::from_str(&original)?;
        match corruption {
            "database" => {
                damaged["database"] = serde_json::json!(DatabaseId::new(Uuid::from_u128(
                    0x019eef00000070008000000000000400
                ))?)
            }
            "epoch" => damaged["epoch"] = serde_json::json!(2),
            "holder" => {
                damaged["holder"] = serde_json::json!("019eef00-0000-4000-8000-000000000001")
            }
            "future end" => {
                damaged["ended_at"] = serde_json::to_value(ReplicationPosition::new(
                    Epoch::new(2)?,
                    fleet_sync_protocol::CommitSequence::new(1),
                ))?
            }
            _ => {}
        }
        let damaged = if corruption == "malformed" {
            "invalid JSON".to_owned()
        } else {
            serde_json::to_string(&damaged)?
        };
        let expected = damaged.clone();
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    sqlx_core::query::query::<Turso>(
                        "UPDATE _fleet_sync_epoch_history SET record=?1",
                    )
                    .bind(damaged)
                    .execute(scope.executor())
                    .await?;
                    assert!(
                        matches!(
                            epoch_holder(
                                scope,
                                database().map_err(storage)?,
                                Epoch::new(1).map_err(storage)?
                            )
                            .await,
                            Err(LeaseError::Corrupt { .. })
                        ),
                        "accepted corrupt history: {corruption}"
                    );
                    Ok(())
                })
            })
            .await?;
        assert_eq!(history_records(&store).await?, [expected]);
    }
    Ok(())
}
