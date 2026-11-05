#![allow(clippy::unwrap_used, clippy::panic_in_result_fn)]
use super::*;
use crate::{turso_registry_fixture::*, *};
use durable_steps::{AtomicStore, storage::TransactionScope};
use sqlx_core::{migrate::Migrator, query::query, row::Row};
use sqlx_turso::Turso;

#[tokio::test]
async fn floor_metadata_uses_the_injected_clock_and_preserves_creation_on_upsert() -> TestResult {
    use runtime_primitives::Clock;
    use std::sync::atomic::Ordering;
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    host.now.store(1234, Ordering::SeqCst);
    let store = open_store(&directory.path().join("clock.db"), host.clone(), true).await?;
    let database = database()?;
    for (epoch, millis) in [(1, 1234), (2, 5678)] {
        host.now.store(millis, Ordering::SeqCst);
        let now = host.now();
        store.transaction(move |scope|Box::pin(async move {
            RegistryStorage(scope).save_floor(database,Epoch::new(epoch).map_err(storage)?, now).await.map_err(storage)?;
            let row=query::<Turso>("SELECT created_at,updated_at FROM _fleet_sync_epoch_floor WHERE database=?1").bind(database.to_string()).fetch_one(scope.executor()).await?;
            assert_eq!(row.try_get::<i64,_>("created_at")?,1234);
            assert_eq!(row.try_get::<i64,_>("updated_at")?,now.unix_millis());
            Ok(())
        })).await?;
    }
    Ok(())
}

#[tokio::test]
async fn legacy_registry_core_rows_migrate_to_scalars_without_losing_full_u64() -> TestResult {
    let directory = tempfile::tempdir()?;
    let host = TestHost::open(directory.path(), false).await?;
    let store = open_store(&directory.path().join("registry.db"), host, false).await?;
    let database = database()?;
    let holder = peer(1)?;
    let epoch = Epoch::new(u64::MAX)?;
    let mut record = DatabaseRecord::initial(
        database,
        DatabaseLifecycle::Registered {
            origin: Origin::Seeded {
                position: ReplicationPosition::new(epoch, CommitSequence::new(u64::MAX)),
            },
            imported_from: None,
        },
        epoch,
        SyncTime::from_unix_millis(1000),
    );
    record.revision.change = std::num::NonZeroU64::new(u64::MAX).unwrap();
    let grant = RegistryLease {
        database,
        holder,
        epoch,
        expires_at: SyncTime::from_unix_millis(3000),
        last_renewed_at: SyncTime::from_unix_millis(2000),
    };
    let history = EpochHistory {
        database,
        holder,
        epoch,
        granted_at: SyncTime::from_unix_millis(1000),
        ended_at: None,
    };
    let report = ReportSchemaSupport {
        peer: holder,
        session_id: SessionId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000333,
        ))?,
        attach_seq: AttachSequence::new(u64::MAX)?,
        support: SchemaSupport {
            max_version: u64::MAX,
        },
    };
    let expected_record = record.clone();
    let expected_grant = grant.clone();
    let expected_history = history.clone();
    let expected_report = report.clone();
    store.transaction(move |scope| Box::pin(async move {
        let mut old=Migrator::with_migrations(LEASE_MIGRATOR.sql_migrations().filter(|m|m.version<=4).cloned().collect::<Vec<_>>());
        old.dangerous_set_table_name("_fleet_sync_registry_migrations");
        old.run_direct(None,scope.executor(),false).await.map_err(storage)?;
        for (statement,body) in [
            ("INSERT INTO _fleet_sync_databases(database,record) VALUES(?1,?2)",super::encode(&record).map_err(storage)?),
            ("INSERT INTO _fleet_sync_leases(database,record) VALUES(?1,?2)",super::encode(&grant).map_err(storage)?),
        ] {query::<Turso>(statement).bind(database.to_string()).bind(body).execute(scope.executor()).await?;}
        query::<Turso>("INSERT INTO _fleet_sync_epoch_floor(database,epoch) VALUES(?1,'18446744073709551615')").bind(database.to_string()).execute(scope.executor()).await?;
        query::<Turso>("INSERT INTO _fleet_sync_epoch_history(database,epoch,record) VALUES(?1,'18446744073709551615',?2)").bind(database.to_string()).bind(super::encode(&history).map_err(storage)?).execute(scope.executor()).await?;
        query::<Turso>("INSERT INTO _fleet_sync_schema_support(peer,record) VALUES(?1,?2)").bind(holder.to_string()).bind(super::encode(&report).map_err(storage)?).execute(scope.executor()).await?;
        let all=LEASE_MIGRATOR.with_history_table("_fleet_sync_registry_migrations");
        all.run_direct(None,scope.executor(),false).await.map_err(storage)?;
        Ok(())
    })).await?;
    store
        .transaction(move |scope| {
            Box::pin(async move {
                let mut registry = RegistryStorage(scope);
                assert_eq!(
                    registry.database(database).await.map_err(storage)?,
                    Some(expected_record.clone())
                );
                assert_eq!(
                    registry.lease(database).await.map_err(storage)?,
                    Some(expected_grant)
                );
                assert_eq!(
                    registry.floor(database).await.map_err(storage)?,
                    Some(epoch)
                );
                assert_eq!(
                    registry.history(database, epoch).await.map_err(storage)?,
                    Some(expected_history)
                );
                assert_eq!(
                    accepted_schema_support(registry.0, holder)
                        .await
                        .map_err(storage)?,
                    Some(expected_report)
                );
                let row = query::<Turso>(
                    "SELECT id,epoch,created_at,updated_at FROM _fleet_sync_epoch_history",
                )
                .fetch_one(registry.0.executor())
                .await?;
                let id = row.try_get::<String, _>("id")?;
                assert_eq!(
                    durable_steps::Uuid::parse_str(&id)
                        .map_err(storage)?
                        .get_version_num(),
                    7
                );
                assert_eq!(row.try_get::<String, _>("epoch")?, "18446744073709551615");
                assert_eq!(row.try_get::<i64, _>("created_at")?, 1000);
                let lease_times =
                    query::<Turso>("SELECT created_at,updated_at FROM _fleet_sync_leases")
                        .fetch_one(registry.0.executor())
                        .await?;
                assert_eq!(lease_times.try_get::<i64, _>("created_at")?, 1000);
                assert_eq!(lease_times.try_get::<i64, _>("updated_at")?, 2000);
                registry
                    .save_database(
                        &expected_record,
                        fleet_sync_protocol::SyncTime::from_unix_millis(1000),
                    )
                    .await
                    .map_err(storage)?;
                for table in [
                    "_fleet_sync_databases",
                    "_fleet_sync_leases",
                    "_fleet_sync_epoch_floor",
                    "_fleet_sync_epoch_history",
                    "_fleet_sync_takeover_previews",
                    "_fleet_sync_transfer_cancels",
                    "_fleet_sync_schema_support",
                ] {
                    let columns = query::<Turso>("SELECT name FROM pragma_table_info(?1)")
                        .bind(table)
                        .fetch_all(registry.0.executor())
                        .await?;
                    let columns = columns
                        .iter()
                        .map(|r| r.try_get::<String, _>("name"))
                        .collect::<Result<Vec<_>, _>>()?;
                    assert!(
                        !columns.iter().any(|name| name == "record"),
                        "{table} still stores a library record"
                    );
                    assert!(columns.iter().any(|name| name == "created_at"));
                    assert!(columns.iter().any(|name| name == "updated_at"));
                }
                Ok(())
            })
        })
        .await?;
    Ok(())
}
