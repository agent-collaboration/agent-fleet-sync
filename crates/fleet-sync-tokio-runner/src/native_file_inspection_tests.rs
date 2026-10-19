//! Inspect the real published bootstrap before native local migrations exist.
#![allow(clippy::panic_in_result_fn)]
use crate::{StagingStore, writer_bootstrap_fixture::*};
use fleet_sync_protocol::{Epoch, ReceiverId};
use fleet_sync_receiver::SnapshotInstallState;
use fleet_sync_writer::{
    ReplicatedDatabase, RetainedFileOrigin, RetainedFileOriginKind, inspect_native_file,
};
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::SystemTime,
};

#[derive(Debug, Eq, PartialEq)]
struct DirectoryImage {
    modified: SystemTime,
    entries: BTreeMap<OsString, FileImage>,
}
#[derive(Debug, Eq, PartialEq)]
struct FileImage {
    bytes: Vec<u8>,
    modified: SystemTime,
    mode: u32,
}
fn image(directory: &Path) -> TestResult<DirectoryImage> {
    let entries = std::fs::read_dir(directory)?
        .map(|entry| {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if !metadata.is_file() {
                return Err("fixture contains a non-file".into());
            }
            Ok((
                entry.file_name(),
                FileImage {
                    bytes: std::fs::read(entry.path())?,
                    modified: metadata.modified()?,
                    mode: metadata.permissions().mode(),
                },
            ))
        })
        .collect::<TestResult<_>>()?;
    Ok(DirectoryImage {
        modified: std::fs::metadata(directory)?.modified()?,
        entries,
    })
}
struct ReadOnlyDirectory(PathBuf);
impl ReadOnlyDirectory {
    fn apply(directory: &Path) -> TestResult<Self> {
        for entry in std::fs::read_dir(directory)? {
            std::fs::set_permissions(entry?.path(), std::fs::Permissions::from_mode(0o444))?;
        }
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o555))?;
        Ok(Self(directory.into()))
    }
}
impl Drop for ReadOnlyDirectory {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
        if let Ok(entries) = std::fs::read_dir(&self.0) {
            for entry in entries.flatten() {
                let _ =
                    std::fs::set_permissions(entry.path(), std::fs::Permissions::from_mode(0o644));
            }
        }
    }
}

#[tokio::test]
async fn published_pending_bootstrap_is_inspected_without_local_history_or_writes() -> TestResult {
    inspect_published_bootstrap(false).await
}

#[tokio::test]
async fn ready_bootstrap_inspection_preserves_the_completed_marker_without_writes() -> TestResult {
    inspect_published_bootstrap(true).await
}

async fn inspect_published_bootstrap(ready: bool) -> TestResult {
    // Arrange actual snapshot pages, manifest verification and writer publication.
    let root = tempfile::tempdir()?;
    let source = source(&root.path().join("source.db")).await?;
    let staging = StagingStore::open(root.path().join("staging")).await?;
    let (install, _) = stage(&staging, &source).await?;
    let native = root.path().join("native");
    std::fs::create_dir(&native)?;
    let path = native.join("pending.db");
    let target = ReplicatedDatabase::seeding(
        &path,
        Schema {
            count: 3,
            different: false,
        },
        grant(&install)?,
        policy(),
    )
    .await?;
    staging.publish_writer(&install, &target).await?;
    assert_eq!(
        query::<Turso>(
            "SELECT count(*) AS count FROM sqlite_schema WHERE name='_fleet_sync_local_migrations'"
        )
        .fetch_one(target.reader())
        .await?
        .try_get::<i64, _>("count")?,
        0
    );
    if ready {
        target
            .mark_bootstrap_ready(
                &install,
                ReceiverId::new(durable_steps::Uuid::from_u128(
                    0x019eef00000070008000000000000009,
                ))?,
            )
            .await?;
        target.migrate().await?;
        assert_eq!(
            query::<Turso>(
                "SELECT count(*) AS count FROM sqlite_schema WHERE name='_fleet_sync_local_migrations'"
            )
            .fetch_one(target.reader())
            .await?
            .try_get::<i64, _>("count")?,
            1
        );
    }
    target.close().await;
    drop(target);
    source.close().await;
    drop(source);
    let _permissions = ReadOnlyDirectory::apply(&native)?;
    let before = image(&native)?;

    // Act without a schema, reader pool or writable reopen.
    for _ in 0..2 {
        let inspected = inspect_native_file(&path).await?;
        assert_eq!(
            inspected.origin,
            RetainedFileOrigin {
                database: install.database,
                epoch: Epoch::new(2)?,
                start: install.position,
                kind: RetainedFileOriginKind::Bootstrap,
            }
        );
        assert_eq!(
            inspected.bootstrap,
            Some(SnapshotInstallState {
                install: install.clone(),
                verified: true,
                ready,
            })
        );
        assert_eq!(inspected.authority, None);
    }
    assert_eq!(image(&native)?, before);
    Ok(())
}
