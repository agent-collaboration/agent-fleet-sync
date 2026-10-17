#![allow(clippy::panic_in_result_fn)]
use crate::{
    sender_test_fixture::{SenderFixture, TestResult},
    writer_scope::WriterScope,
    *,
};
use fleet_sync_protocol::*;
use fleet_sync_receiver::{SnapshotInstall, SnapshotInstallRules};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
};
use sqlx_turso::TursoConnectOptions;
use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

async fn status(fixture: &SenderFixture, copy: ReceiverId) -> TestResult<CopyStatus> {
    let file = fixture.transport.files.get(&copy).ok_or("copy missing")?;
    let mut connection = file.connection.lock().await;
    let connection = connection.as_mut().ok_or("copy closed")?;
    match fleet_sync_receiver::copy_status(&mut WriterScope(connection)).await? {
        CopyStatusAnswer::Status(status) => Ok(status),
        _ => Err("copy is not following".into()),
    }
}

// This adapter exercises the receiver's real prepare/page/verify/fence/ready
// rules. It replaces a closed fixture connection; native host/control-journal
// publication and its crash windows are separate integration proof.
async fn install_follower(
    fixture: &SenderFixture,
    preserve_fence: bool,
) -> TestResult<ReplicationPosition> {
    let source = status(fixture, fixture.first).await?;
    let mut snapshot = fixture.database.snapshot().await?;
    let manifest = snapshot.manifest().await?;
    assert_eq!(source.acked, manifest.position);
    let install = SnapshotInstall {
        install_id: durable_steps::Uuid::now_v7(),
        database: manifest.database,
        position: manifest.position,
        schema: manifest.schema.clone(),
    };
    let file = fixture
        .transport
        .files
        .get(&fixture.second)
        .ok_or("follower missing")?;
    let mut target = file.connection.lock().await;
    let old_fence = if preserve_fence {
        SnapshotInstallRules(&mut WriterScope(target.as_mut().ok_or("follower closed")?))
            .read_old_fence()
            .await?
    } else {
        None
    };
    let staging = fixture
        ._directory
        .path()
        .join(format!("snapshot-{}.db", install.install_id));
    let mut connection = TursoConnectOptions::new()
        .filename(&staging)
        .create_if_missing(true)
        .foreign_keys(false)
        .connect()
        .await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let mut migrator = sqlx::migrate!("../fleet-sync-receiver/migrations");
    migrator.dangerous_set_table_name("_fleet_sync_receiver_history");
    migrator.run_direct(None, &mut *transaction, false).await?;
    let mut scope = WriterScope(&mut transaction);
    fleet_sync_receiver::initialize_hook(&mut scope, &file.hook).await?;
    SnapshotInstallRules(&mut scope)
        .prepare(&install, &source)
        .await?;
    let mut cursor = None;
    let mut page = 0_u32;
    loop {
        let contents = snapshot.page(cursor.clone()).await?;
        SnapshotInstallRules(&mut scope)
            .stage_page(&install, page, cursor.as_ref(), &contents)
            .await?;
        cursor = contents.next;
        if cursor.is_none() {
            break;
        }
        page = page.checked_add(1).ok_or("page overflow")?;
    }
    SnapshotInstallRules(&mut scope)
        .verify(&install, &manifest)
        .await?;
    SnapshotInstallRules(&mut scope)
        .merge_old_fence(&install, old_fence.as_ref())
        .await?;
    SnapshotInstallRules(&mut scope)
        .mark_ready(&install, &file.hook)
        .await?;
    transaction.commit().await?;
    snapshot.close().await?;
    connection
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .await?;
    connection.close().await?;
    if let Some(connection) = target.take() {
        let mut connection = connection;
        connection
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .await?;
        connection.close().await?;
    }
    // The fixture owns the only connections and verifies their WALs drained.
    for path in [&staging, &file.path] {
        let mut wal = path.as_os_str().to_owned();
        wal.push("-wal");
        match tokio::fs::metadata(wal).await {
            Ok(metadata) => assert_eq!(metadata.len(), 0, "fixture WAL was not drained"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    tokio::fs::rename(&staging, &file.path).await?;
    *target = Some(
        TursoConnectOptions::new()
            .filename(&file.path)
            .foreign_keys(false)
            .connect()
            .await?,
    );
    Ok(install.position)
}

async fn writer_head(fixture: &SenderFixture) -> TestResult<ReplicationPosition> {
    let snapshot = fixture.database.snapshot().await?;
    let head = snapshot.position();
    snapshot.close().await?;
    Ok(head)
}
async fn drain(fixture: &SenderFixture, copy: ReceiverId) -> TestResult {
    let head = writer_head(fixture).await?;
    loop {
        let before = fixture
            .sender
            .copies()
            .await?
            .into_iter()
            .find(|state| state.copy == copy)
            .ok_or("no sender registration")?
            .acked;
        if before == head {
            return Ok(());
        }
        fixture.sender.replicate_once(copy).await?;
        let after = fixture
            .sender
            .copies()
            .await?
            .into_iter()
            .find(|state| state.copy == copy)
            .ok_or("no sender registration")?
            .acked;
        assert!(
            after > before && after <= head,
            "a real replication step made no forward progress"
        );
    }
}
async fn assert_equal(fixture: &SenderFixture) -> TestResult {
    let mut snapshot = fixture.database.snapshot().await?;
    let expected_manifest = snapshot.manifest().await?;
    let mut expected = BTreeMap::<String, Vec<Vec<SqlValue>>>::new();
    let mut cursor = None;
    loop {
        let page = snapshot.page(cursor).await?;
        cursor = page.next;
        expected.entry(page.table).or_default().extend(page.rows);
        if cursor.is_none() {
            break;
        }
    }
    snapshot.close().await?;
    let file = fixture
        .transport
        .files
        .get(&fixture.second)
        .ok_or("no follower")?;
    let mut connection = file.connection.lock().await;
    let mut scope = WriterScope(connection.as_mut().ok_or("closed follower")?);
    assert_eq!(
        fleet_sync_receiver::snapshot_manifest(&mut scope, expected_manifest.position).await?,
        SnapshotManifestAnswer::Manifest(expected_manifest)
    );
    let mut actual = BTreeMap::<String, Vec<Vec<SqlValue>>>::new();
    let mut cursor = None;
    loop {
        let SnapshotRead::Page(page) =
            fleet_sync_receiver::snapshot_page(&mut scope, cursor).await?
        else {
            return Err("follower snapshot moved".into());
        };
        cursor = page.next;
        actual.entry(page.table).or_default().extend(page.rows);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        actual, expected,
        "rows or storage classes diverged after recovery"
    );
    Ok(())
}

#[tokio::test]
async fn same_follower_resumes_after_snapshot_then_migration_before_and_after_native_restart()
-> TestResult {
    let mut fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    install_follower(&fixture, false).await?;
    for migration_count in [3, 4] {
        let before = fixture
            .sender
            .copies()
            .await?
            .into_iter()
            .find(|state| state.copy == fixture.second)
            .ok_or("no follower")?;
        let mut write = fixture.database.begin_write().await?;
        write
            .execute("UPDATE text_records SET label='while follower offline'")
            .await?;
        write.commit().await?;
        drain(&fixture, fixture.first).await?;
        let bounded = ReplicationSender::new(
            fixture.database.clone(),
            Arc::new(()),
            fixture.marks.clone(),
            fixture.transport.clone(),
            SenderOptions {
                retention: Retention::Bounded {
                    max_age: Duration::MAX,
                    max_changes: 0,
                },
                ..SenderOptions::default()
            },
        );
        assert!(bounded.replicate_once(fixture.second).await?.iter().any(|event|matches!(event,ReplicationEvents::CopyNeedsSnapshot{copy} if *copy==fixture.second)));
        drop(bounded);
        let installed = install_follower(&fixture, true).await?;
        assert!(
            matches!(
                fixture
                    .sender
                    .add_copy(fixture.second, CopyRole::Follower, installed)
                    .await,
                Err(SenderError::Invalid {
                    field: "copy.registration_conflict"
                })
            ),
            "recovery must not loosen registration"
        );
        let mut write = fixture.database.begin_write().await?;
        write
            .execute("UPDATE text_records SET label='after snapshot'")
            .await?;
        write.commit().await?;
        let head = writer_head(&fixture).await?;
        let non_marker = ReplicationPosition::new(
            installed.epoch,
            CommitSequence::new(
                installed
                    .sequence
                    .value()
                    .checked_add(1)
                    .ok_or("position overflow")?,
            ),
        );
        let future = ReplicationPosition::new(
            head.epoch,
            CommitSequence::new(
                head.sequence
                    .value()
                    .checked_add(100)
                    .ok_or("position overflow")?,
            ),
        );
        for invalid in [before.acked, non_marker, future] {
            assert!(
                fixture
                    .sender
                    .complete_snapshot(fixture.second, invalid)
                    .await
                    .is_err(),
                "invalid completion {invalid:?} was accepted"
            );
            let state = fixture
                .sender
                .copies()
                .await?
                .into_iter()
                .find(|state| state.copy == fixture.second)
                .ok_or("no follower")?;
            assert!(state.needs_snapshot);
            assert_eq!(state.acked, before.acked);
        }
        assert!(
            fixture
                .sender
                .complete_snapshot(fixture.first, installed)
                .await
                .is_err()
        );
        let recovered = fixture
            .sender
            .complete_snapshot(fixture.second, installed)
            .await?;
        assert_eq!(recovered.start, before.start);
        assert_eq!(recovered.acked, installed);
        assert!(!recovered.needs_snapshot);
        fixture
            .database
            .schema
            .migration_count
            .store(migration_count, Ordering::SeqCst);
        fixture.database.migrate().await?;
        drain(&fixture, fixture.first).await?;
        drain(&fixture, fixture.second).await?;
        assert_equal(&fixture).await?;
        let repeated = fixture
            .sender
            .complete_snapshot(fixture.second, installed)
            .await?;
        assert!(!repeated.needs_snapshot && repeated.acked > installed);
        if migration_count == 3 {
            let now = fixture.database.clock.now();
            fixture
                .database
                .observe_renewal(
                    RenewAnswer::Renewed {
                        recorded: RenewResult {
                            epoch: Epoch::new(1)?,
                            expires_at: now.after(Duration::from_secs(600)),
                        },
                        current: LeaseStatus {
                            database: fixture.database.database(),
                            holder: PeerId::new(durable_steps::Uuid::from_u128(2))?,
                            epoch: Epoch::new(1)?,
                            state: LeaseState::Held,
                            expires_at: now.after(Duration::from_secs(600)),
                            remaining_ms: 600_000,
                        },
                        transfer_in_progress: None,
                        replayed: false,
                    },
                    now,
                )
                .await?;
            fixture = fixture.restart().await?;
            assert_equal(&fixture).await?;
            assert!(
                fixture
                    .sender
                    .copies()
                    .await?
                    .iter()
                    .any(|state| state.copy == fixture.second
                        && !state.needs_snapshot
                        && state.acked == repeated.acked)
            );
        }
    }
    fixture.close().await
}
