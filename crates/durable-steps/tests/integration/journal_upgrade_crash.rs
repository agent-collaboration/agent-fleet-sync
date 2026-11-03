//! SIGKILL occurs after real upgrade writes, strictly before the caller commits.
use super::journal_metadata::{ReplayTask, Result, connect, history, legacy_file, metadata};
use crate::support::TestHost;
use durable_steps::{
    Engine, OperationId, OperationState,
    turso_store::{TursoStore, migrate_journal},
};
use sqlx_core::{connection::Connection, query::query, row::Row};
use sqlx_turso::Turso;
use std::{
    io::Write, os::unix::process::ExitStatusExt, path::Path, process::Stdio, sync::Arc,
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, BufReader};

const CHECKPOINT: &str = "L7_UPGRADE_WRITTEN_NOT_COMMITTED";

#[tokio::test]
async fn upgrade_child_checkpoint() -> Result {
    let Some(path) = std::env::var_os("FLEET_JOURNAL_UPGRADE_CHILD") else {
        return Ok(());
    };
    let mut connection = connect(Path::new(&path)).await?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    migrate_journal(&mut transaction).await?;
    assert_eq!(
        metadata(&mut transaction, "_fleet_sync_steps", "id")
            .await?
            .len(),
        2
    );
    // The parent acts only after this signal. No timer chooses the crash point.
    println!("\n{CHECKPOINT}");
    std::io::stdout().flush()?;
    std::future::pending::<()>().await;
    transaction.commit().await?;
    Ok(())
}

#[tokio::test]
async fn sigkill_inside_uncommitted_upgrade_recovers_v1_before_retry() -> Result {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("crash-upgrade.sqlite");
    let mut connection = legacy_file(&path).await?;
    let before = history(&mut connection).await?;
    connection.close().await?;
    // current_exe is the test binary already signed by the configured runner.
    let mut child = tokio::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "journal_upgrade_crash::upgrade_child_checkpoint",
            "--nocapture",
        ])
        .env("FLEET_JOURNAL_UPGRADE_CHILD", &path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = child.stdout.take().ok_or("child stdout was not piped")?;
    let mut lines = BufReader::new(stdout).lines();
    let observed = tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(line) = lines.next_line().await? {
            if line == CHECKPOINT {
                return Ok::<_, std::io::Error>(true);
            }
        }
        Ok(false)
    })
    .await;
    let killed = child.kill().await;
    let status = child.wait().await?;
    killed?;
    assert!(
        observed??,
        "child exited without the causal upgrade checkpoint"
    );
    assert_eq!(status.signal(), Some(9));
    let mut connection = connect(&path).await?;
    assert_eq!(history(&mut connection).await?, before);
    let columns = query::<Turso>("PRAGMA table_info(_fleet_sync_steps)")
        .fetch_all(&mut connection)
        .await?;
    assert!(
        !columns.iter().any(|row| row
            .try_get::<String, _>("name")
            .is_ok_and(|name| name == "id")),
        "uncommitted DDL survived the crash"
    );
    let schema = query::<Turso>("SELECT version FROM _fleet_sync_schema")
        .fetch_one(&mut connection)
        .await?;
    assert_eq!(schema.try_get::<i64, _>("version")?, 1);
    let receipts = query::<Turso>("SELECT version FROM _fleet_sync_migrations ORDER BY version")
        .fetch_all(&mut connection)
        .await?;
    assert_eq!(receipts.len(), 1, "uncommitted migration receipt survived");
    assert_eq!(receipts[0].try_get::<i64, _>("version")?, 1);
    connection.close().await?;
    let host = TestHost::new();
    let store = Arc::new(TursoStore::open_and_migrate(&path, host.clone()).await?);
    let mut connection = connect(&path).await?;
    assert_eq!(history(&mut connection).await?, before);
    assert_eq!(
        metadata(&mut connection, "_fleet_sync_steps", "id")
            .await?
            .len(),
        2
    );
    connection.close().await?;
    let mut engine = Engine::new(store, host);
    engine.register(ReplayTask)?;
    engine.drive_due().await?;
    let id = OperationId::try_from("0197abc0-def0-7123-8123-abcdef012345".to_owned())?;
    assert!(
        matches!(engine.status(&id).await?.state,OperationState::Completed{result} if result==b"14")
    );
    println!(
        "uncommitted upgrade killed and joined; v1 recovered; upgrade retried; saved history replayed"
    );
    Ok(())
}
