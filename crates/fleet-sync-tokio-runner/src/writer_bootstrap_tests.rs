#![allow(clippy::panic_in_result_fn)]
use crate::{writer_bootstrap_fixture::*, *};
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_lease::WriterState;
use fleet_sync_protocol::*;
use fleet_sync_receiver::SnapshotInstallRules;
use fleet_sync_writer::{
    BootstrapError, ReplicatedDatabase, ReplicationSender, SenderOptions, WriteRefusal,
};
use futures_util::StreamExt;
use runtime_primitives::SystemClock;
use sqlx_core::{executor::Executor, migrate::Migrator, query::query, row::Row};
use sqlx_turso::Turso;
use std::{path::Path, sync::Arc};

#[tokio::test]
async fn real_snapshot_bootstraps_without_replaying_dml_and_later_migration_follows_to_copy()
-> TestResult {
    let root = tempfile::tempdir()?;
    let source = source(&root.path().join("source.db")).await?;
    let staging = Arc::new(StagingStore::open(root.path().join("staging")).await?);
    let (install, _) = stage(&staging, &source).await?;
    let grant = grant(&install)?;
    let mut target = Arc::new(
        ReplicatedDatabase::seeding(
            &root.path().join("target.db"),
            Schema {
                count: 3,
                different: false,
            },
            grant.clone(),
            policy(),
        )
        .await?,
    );
    let objects = ObjectStore::open(root.path().join("objects")).await?;
    let control = objects
        .object(
            ObjectKey::new("control")?,
            ObjectOptions::default(),
            Arc::new(TokioHost::new(Arc::new(NoCalls), SystemClock)),
        )
        .await?;
    let coordinator = Arc::new(InstallCoordinator::open(control, staging.clone()).await?);
    let key = ObjectKey::new("writer-target")?;
    let mut owned = WriterInstallTarget::new(key.clone(), target.clone());
    coordinator
        .start(
            InstallRequest {
                attempt: fleet_sync_protocol::InstallAttemptId::new(install.install_id.as_uuid())?,
                target: key.clone(),
                database: install.database,
                source: ReceiverId::new(durable_steps::Uuid::from_u128(
                    0x019eef00000070008000000000000009,
                ))?,
            },
            &owned,
        )
        .await?;
    coordinator.pin(install.clone()).await?;
    assert!(matches!(
        target.begin_write().await,
        Err(WriteRefusal::Seeding)
    ));
    let mut held = target
        .reader()
        .fetch("SELECT 1 AS value UNION ALL SELECT 2 AS value");
    held.next().await.ok_or("no held row")??;
    assert!(matches!(
        coordinator.publish(install.install_id, &owned).await,
        Err(StagingError::Bootstrap(BootstrapError::Publish(
            PublishError::ActiveReaders
        )))
    ));
    drop(held);
    coordinator.publish(install.install_id, &owned).await?;
    coordinator.publish(install.install_id, &owned).await?;
    assert_eq!(target.snapshot().await?.schema(), &install.schema);
    assert_eq!(
        query::<Turso>("SELECT amount FROM records")
            .fetch_one(target.reader())
            .await?
            .try_get::<i64, _>("amount")?,
        2,
        "historical backfill was executed twice"
    );
    assert!(matches!(
        target.writer_state().await?,
        WriterState::Seeding { .. }
    ));
    assert!(!target.installed_snapshot().await?.ok_or("no marker")?.ready);
    assert_eq!(
        query::<Turso>("SELECT count(*) AS count FROM _sqlx_migrations")
            .fetch_one(target.reader())
            .await?
            .try_get::<i64, _>("count")?,
        2
    );
    assert_eq!(
        query::<Turso>("SELECT count(*) AS count FROM sqlite_schema WHERE name='_fleet_sync_copy'")
            .fetch_one(target.reader())
            .await?
            .try_get::<i64, _>("count")?,
        0
    );

    target.close().await;
    target = Arc::new(
        ReplicatedDatabase::seeding(
            &root.path().join("target.db"),
            Schema {
                count: 3,
                different: false,
            },
            grant.clone(),
            policy(),
        )
        .await?,
    );
    owned = WriterInstallTarget::new(key.clone(), target.clone());
    assert!(matches!(
        target.writer_state().await?,
        WriterState::Seeding { .. }
    ));
    assert_eq!(
        target.snapshot().await?.schema(),
        &install.schema,
        "restart applied a pending migration before ready"
    );
    coordinator.publish(install.install_id, &owned).await?;
    let older = install;
    let (install, _) = stage(&staging, &source).await?;
    coordinator
        .start(
            InstallRequest {
                attempt: fleet_sync_protocol::InstallAttemptId::new(install.install_id.as_uuid())?,
                target: key,
                database: install.database,
                source: ReceiverId::new(durable_steps::Uuid::from_u128(
                    0x019eef00000070008000000000000009,
                ))?,
            },
            &owned,
        )
        .await?;
    coordinator.pin(install.clone()).await?;
    assert!(
        matches!(coordinator.mark_writer_ready(older.install_id, &owned).await?, InstallFileAnswer::Superseded {by} if by==install.install_id)
    );
    assert!(
        matches!(coordinator.publish(older.install_id, &owned).await?, InstallFileAnswer::Published {snapshot} if snapshot==older)
    );
    coordinator.publish(install.install_id, &owned).await?;
    assert_eq!(
        target
            .installed_snapshot()
            .await?
            .ok_or("new marker missing")?
            .install,
        install
    );
    let copy = objects
        .object(
            ObjectKey::new("follower")?,
            ObjectOptions {
                foreign_keys: false,
            },
            Arc::new(TokioHost::new(Arc::new(NoCalls), SystemClock)),
        )
        .await?;
    copy.transaction(|scope| {
        Box::pin(async move {
            let mut migrations: Migrator = sqlx::migrate!("../fleet-sync-receiver/migrations");
            migrations.dangerous_set_table_name("_fleet_sync_copy_migrations");
            migrations
                .run_direct(None, scope.executor(), false)
                .await
                .map_err(StoreError::backend)
        })
    })
    .await?;
    let (copy_install, _) = stage(&staging, &source).await?;
    staging.publish(&copy_install, &copy).await?;
    let copied = copy_install.clone();
    copy.transaction(move |scope| {
        Box::pin(async move {
            SnapshotInstallRules(scope)
                .mark_ready(&copied, &Hook)
                .await
                .map_err(StoreError::backend)?;
            Ok(())
        })
    })
    .await?;
    let fence = FenceRequest {
        database: install.database,
        transfer_id: TransferId::new(durable_steps::Uuid::now_v7())?,
        new_epoch: grant.lease.epoch,
        expect: FenceExpectation {
            epoch: install.position.epoch,
            acked: install.position,
        },
        target_schema_max: 3,
        holder: grant.lease.holder,
    };
    assert!(matches!(
        copy.transaction(move |scope| Box::pin(async move {
            fleet_sync_receiver::fence(scope, &fence)
                .await
                .map_err(StoreError::backend)
        }))
        .await?,
        FenceResult::Fenced { .. }
    ));
    coordinator
        .mark_writer_ready(install.install_id, &owned)
        .await?;
    let remaining = query::<Turso>("SELECT (SELECT count(*) FROM _fleet_sync_install)+(SELECT count(*) FROM _fleet_sync_install_pages)+(SELECT count(*) FROM _fleet_sync_copy_migrations)+(SELECT count(*) FROM _fleet_sync_copy_tables) AS count")
        .fetch_one(target.reader()).await?.try_get::<i64,_>("count")?;
    assert_eq!(remaining, 0, "ready retained receiver bookkeeping rows");
    assert!(matches!(
        target.writer_state().await?,
        WriterState::Writing { .. }
    ));
    assert_eq!(
        target.snapshot().await?.schema(),
        &install.schema,
        "ready exposed the received prefix before later migrations"
    );
    let receiver = ReceiverId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000007,
    ))?;
    let sender = ReplicationSender::new(
        target.clone(),
        Arc::new(()),
        Arc::new(()),
        Arc::new(CopyTransport {
            copy: copy.clone(),
            sender: grant.lease.holder,
        }),
        SenderOptions::default(),
    );
    sender
        .add_copy(receiver, CopyRole::Follower, install.position)
        .await?;
    target.migrate().await?;
    let through = target.snapshot().await?.position();
    assert!(through > install.position);
    sender.replicate_once(receiver).await?;
    let actual = copy
        .transaction(|scope| {
            Box::pin(async move {
                let row = query::<Turso>("SELECT amount,note FROM records")
                    .fetch_one(scope.executor())
                    .await
                    .map_err(StoreError::backend)?;
                Ok((
                    row.try_get::<i64, _>("amount")
                        .map_err(StoreError::backend)?,
                    row.try_get::<String, _>("note")
                        .map_err(StoreError::backend)?,
                ))
            })
        })
        .await?;
    assert_eq!(actual, (2, "after bootstrap".into()));
    let copies = sender.copies().await?;
    assert_eq!(
        copies
            .iter()
            .find(|copy| copy.copy == receiver)
            .ok_or("copy missing")?
            .acked,
        through
    );
    assert!(copies.iter().any(|copy| {
        copy.copy
            == ReceiverId::new(durable_steps::Uuid::from_u128(
                0x019eef00000070008000000000000009,
            ))
            .expect("valid receiver")
            && copy.role == CopyRole::Authoritative
            && copy.start == install.position
    }));
    target.close().await;
    source.close().await;
    objects.close().await?;
    Ok(())
}

#[tokio::test]
async fn different_consumer_prefix_refuses_bootstrap_before_target_file_or_wal_changes()
-> TestResult {
    let root = tempfile::tempdir()?;
    let source = source(&root.path().join("source.db")).await?;
    let staging = StagingStore::open(root.path().join("staging")).await?;
    let (install, _) = stage(&staging, &source).await?;
    let target = ReplicatedDatabase::seeding(
        &root.path().join("target.db"),
        Schema {
            count: 2,
            different: true,
        },
        grant(&install)?,
        policy(),
    )
    .await?;
    async fn images(path: &Path) -> TestResult<(Vec<u8>, Vec<u8>)> {
        let mut wal = path.as_os_str().to_owned();
        wal.push("-wal");
        let wal = match tokio::fs::read(wal).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        Ok((tokio::fs::read(path).await?, wal))
    }
    let before = images(target.path()).await?;
    assert!(matches!(
        staging.publish_writer(&install, &target).await,
        Err(StagingError::Bootstrap(BootstrapError::Writer(
            fleet_sync_writer::OpenError::BootstrapSchemaMismatch { .. }
        )))
    ));
    assert_eq!(images(target.path()).await?, before);
    assert!(target.installed_snapshot().await?.is_none());
    target.close().await;
    source.close().await;
    Ok(())
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn cancelled_after_conversion_before_rename_replays_with_the_latest_live_journal()
-> TestResult {
    interrupted_before_rename(false).await
}
#[cfg(feature = "test-support")]
#[tokio::test]
async fn cancelled_after_conversion_recovers_on_the_same_writer_arc() -> TestResult {
    interrupted_before_rename(true).await
}
#[cfg(feature = "test-support")]
async fn interrupted_before_rename(same_arc: bool) -> TestResult {
    let root = tempfile::tempdir()?;
    let source = source(&root.path().join("source.db")).await?;
    let staging = Arc::new(StagingStore::open(root.path().join("staging")).await?);
    let (install, _) = stage(&staging, &source).await?;
    let grant = grant(&install)?;
    let path = root.path().join("target.db");
    let target = Arc::new(
        ReplicatedDatabase::seeding(
            &path,
            Schema {
                count: 3,
                different: false,
            },
            grant.clone(),
            policy(),
        )
        .await?,
    );
    let objects = ObjectStore::open(root.path().join("objects")).await?;
    let host = Arc::new(TokioHost::new(Arc::new(NoCalls), SystemClock));
    let initial_journal = target.journal_store(host.clone()).await?;
    drop(initial_journal);
    let control = objects
        .object(ObjectKey::new("control")?, ObjectOptions::default(), host)
        .await?;
    let coordinator = Arc::new(InstallCoordinator::open(control, staging).await?);
    let key = ObjectKey::new("writer-target")?;
    let owned = Arc::new(WriterInstallTarget::new(key.clone(), target.clone()));
    coordinator
        .start(
            InstallRequest {
                attempt: fleet_sync_protocol::InstallAttemptId::new(install.install_id.as_uuid())?,
                target: key.clone(),
                database: install.database,
                source: ReceiverId::new(durable_steps::Uuid::from_u128(
                    0x019eef00000070008000000000000009,
                ))?,
            },
            &owned,
        )
        .await?;
    coordinator.pin(install.clone()).await?;
    target.hold_bootstrap_before_rename();
    let pending_owner = owned.clone();
    let pending_coordinator = coordinator.clone();
    let install_id = install.install_id;
    let publishing = tokio::spawn(async move {
        pending_coordinator
            .publish(install_id, &pending_owner)
            .await
    });
    let converted = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        target.wait_bootstrap_converted(),
    )
    .await??;
    assert_eq!(converted, install);
    publishing.abort();
    assert!(publishing.await.is_err_and(|error| error.is_cancelled()));
    drop(owned);
    let target = if same_arc {
        target
    } else {
        target.close().await;
        drop(target);
        Arc::new(
            ReplicatedDatabase::seeding(
                &path,
                Schema {
                    count: 3,
                    different: false,
                },
                grant,
                policy(),
            )
            .await?,
        )
    };
    assert!(matches!(
        target.begin_write().await,
        Err(WriteRefusal::Seeding)
    ));
    assert!(
        target.installed_snapshot().await?.is_none(),
        "rename happened before the held boundary"
    );
    admit_pending(&target).await?;
    let owned = WriterInstallTarget::new(key, target.clone());
    coordinator.publish(install_id, &owned).await?;
    assert_eq!(
        query::<Turso>("SELECT amount FROM records")
            .fetch_one(target.reader())
            .await?
            .try_get::<i64, _>("amount")?,
        2
    );
    assert_eq!(
        query::<Turso>("SELECT count(*) AS count FROM _fleet_sync_operations")
            .fetch_one(target.reader())
            .await?
            .try_get::<i64, _>("count")?,
        1,
        "replay did not re-carry the journal admitted after conversion"
    );
    coordinator.mark_writer_ready(install_id, &owned).await?;
    assert!(
        target
            .installed_snapshot()
            .await?
            .ok_or("writer marker missing")?
            .ready
    );
    let remaining = query::<Turso>("SELECT (SELECT count(*) FROM _fleet_sync_install)+(SELECT count(*) FROM _fleet_sync_install_pages)+(SELECT count(*) FROM _fleet_sync_copy_migrations)+(SELECT count(*) FROM _fleet_sync_copy_tables) AS count").fetch_one(target.reader()).await?.try_get::<i64,_>("count")?;
    assert_eq!(remaining, 0);
    target.close().await;
    source.close().await;
    objects.close().await?;
    Ok(())
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn transient_reopen_failure_after_rename_recovers_the_published_file_on_same_arc()
-> TestResult {
    let root = tempfile::tempdir()?;
    let source = source(&root.path().join("source.db")).await?;
    let staging = Arc::new(StagingStore::open(root.path().join("staging")).await?);
    let (install, _) = stage(&staging, &source).await?;
    let target = Arc::new(
        ReplicatedDatabase::seeding(
            &root.path().join("target.db"),
            Schema {
                count: 3,
                different: false,
            },
            grant(&install)?,
            policy(),
        )
        .await?,
    );
    let objects = ObjectStore::open(root.path().join("objects")).await?;
    let control = objects
        .object(
            ObjectKey::new("control")?,
            ObjectOptions::default(),
            Arc::new(TokioHost::new(Arc::new(NoCalls), SystemClock)),
        )
        .await?;
    let coordinator = Arc::new(InstallCoordinator::open(control, staging).await?);
    let key = ObjectKey::new("writer-target")?;
    let owned = WriterInstallTarget::new(key.clone(), target.clone());
    coordinator
        .start(
            InstallRequest {
                attempt: fleet_sync_protocol::InstallAttemptId::new(install.install_id.as_uuid())?,
                target: key,
                database: install.database,
                source: ReceiverId::new(durable_steps::Uuid::from_u128(
                    0x019eef00000070008000000000000009,
                ))?,
            },
            &owned,
        )
        .await?;
    coordinator.pin(install.clone()).await?;
    target.block_bootstrap_reopen(true);
    assert!(matches!(
        coordinator.publish(install.install_id, &owned).await,
        Err(StagingError::Bootstrap(BootstrapError::Publish(
            PublishError::Storage(_)
        )))
    ));
    assert!(matches!(
        target.begin_write().await,
        Err(WriteRefusal::Storage { .. })
    ));
    assert!(
        matches!(
            coordinator.record(install.install_id).await?.status,
            InstallStatus::Running { .. }
        ),
        "transient reopen failure ended the install"
    );
    target.block_bootstrap_reopen(false);
    let published = target
        .installed_snapshot()
        .await?
        .ok_or("published receipt missing after recovery")?;
    assert_eq!(published.install, install);
    assert!(!published.ready);
    assert!(matches!(
        target.begin_write().await,
        Err(WriteRefusal::Seeding)
    ));
    assert_eq!(
        query::<Turso>("SELECT amount FROM records")
            .fetch_one(target.reader())
            .await?
            .try_get::<i64, _>("amount")?,
        2
    );
    coordinator.publish(install.install_id, &owned).await?;
    coordinator
        .mark_writer_ready(install.install_id, &owned)
        .await?;
    assert!(
        target
            .installed_snapshot()
            .await?
            .ok_or("ready marker missing")?
            .ready
    );
    assert!(matches!(
        target.writer_state().await?,
        WriterState::Writing { .. }
    ));
    target.close().await;
    source.close().await;
    objects.close().await?;
    Ok(())
}
