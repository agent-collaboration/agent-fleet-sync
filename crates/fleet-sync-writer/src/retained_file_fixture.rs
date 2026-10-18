#![allow(clippy::panic_in_result_fn)]
use crate::{
    OpenError, ReadPool, ReplicatedDatabase, WriterOptions, read_transaction_tests::ReadSchema,
};
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::{DatabaseId, PeerId};
use sha2::{Digest, Sha256};
use sqlx_core::{executor::Executor, row::Row};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy)]
pub(super) struct WriterIdentity {
    pub database: DatabaseId,
    pub peer: PeerId,
}
impl WriterIdentity {
    pub async fn observe(database: &ReplicatedDatabase<ReadSchema>) -> Self {
        Self {
            database: database.database(),
            peer: database.control.lock().await.holder.peer(),
        }
    }
    pub async fn open(self, path: &Path) -> Result<ReplicatedDatabase<ReadSchema>, OpenError> {
        ReplicatedDatabase::open_with(
            path,
            self.database,
            ReadSchema,
            LeaseHolder::provisional(self.database, self.peer, policy()),
            std::sync::Arc::new(runtime_primitives::SystemClock),
            WriterOptions {
                allow_provisional_writes: true,
            },
        )
        .await
    }
}
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(60),
        renew_interval: Duration::from_secs(10),
    }
}
pub(super) async fn add_mark(database: &ReplicatedDatabase<ReadSchema>, mark: &str) -> TestResult {
    let mut write = database.begin_write().await?;
    sqlx_core::query::query("INSERT INTO local_marks VALUES(?)")
        .bind(mark)
        .execute(&mut write)
        .await?;
    write.commit().await?;
    Ok(())
}
pub(super) async fn mark_count(readers: &ReadPool) -> TestResult<i64> {
    Ok(readers
        .fetch_one("SELECT COUNT(*) AS count FROM local_marks")
        .await?
        .try_get("count")?)
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct DirectorySnapshot {
    modified: SystemTime,
    entries: BTreeMap<OsString, FileSnapshot>,
}
#[derive(Debug, Eq, PartialEq)]
struct FileSnapshot {
    bytes: u64,
    digest: [u8; 32],
    modified: SystemTime,
    mode: u32,
}
pub(super) fn snapshot(directory: &Path) -> TestResult<DirectorySnapshot> {
    use std::os::unix::fs::PermissionsExt;
    let entries = std::fs::read_dir(directory)?
        .map(|entry| {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if !metadata.is_file() {
                return Err("fixture contains a non-file entry".into());
            }
            Ok((
                entry.file_name(),
                FileSnapshot {
                    bytes: metadata.len(),
                    digest: Sha256::digest(std::fs::read(entry.path())?).into(),
                    modified: metadata.modified()?,
                    mode: metadata.permissions().mode(),
                },
            ))
        })
        .collect::<TestResult<_>>()?;
    Ok(DirectorySnapshot {
        modified: std::fs::metadata(directory)?.modified()?,
        entries,
    })
}

pub(super) struct ReadOnlyFixture(PathBuf);
impl ReadOnlyFixture {
    pub fn apply(directory: &Path) -> TestResult<Self> {
        use std::os::unix::fs::PermissionsExt;
        for entry in std::fs::read_dir(directory)? {
            std::fs::set_permissions(entry?.path(), std::fs::Permissions::from_mode(0o444))?;
        }
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o555))?;
        Ok(Self(directory.into()))
    }
}
impl Drop for ReadOnlyFixture {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
        if let Ok(entries) = std::fs::read_dir(&self.0) {
            for entry in entries.flatten() {
                let _ =
                    std::fs::set_permissions(entry.path(), std::fs::Permissions::from_mode(0o644));
            }
        }
    }
}

pub(super) async fn child(test: &str, action: &str, path: &Path) -> TestResult {
    let output = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new(std::env::current_exe()?)
            .args(["--exact", test, "--nocapture"])
            .env("FLEET_RETAINED_TEST_ACTION", action)
            .env("FLEET_RETAINED_TEST_PATH", path)
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    println!(
        "child {action}: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.status.success(),
        "child {action}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
pub(super) fn child_action() -> Option<(OsString, PathBuf)> {
    Some((
        std::env::var_os("FLEET_RETAINED_TEST_ACTION")?,
        std::env::var_os("FLEET_RETAINED_TEST_PATH")?.into(),
    ))
}
