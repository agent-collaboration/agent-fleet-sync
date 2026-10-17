#![cfg(unix)]
use fleet_sync_test_consumer::TestResult;
use std::{os::unix::fs::MetadataExt, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    time::timeout,
};

#[tokio::test]
async fn hard_link_refuses_occupied_destination_and_survives_kill_before_source_unlink()
-> TestResult {
    let scratch = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tmp");
    std::fs::create_dir_all(&scratch)?;
    let directory = tempfile::Builder::new()
        .prefix("move-link-")
        .tempdir_in(scratch)?;
    let source = directory.path().join("source");
    let destination = directory.path().join("destination");
    std::fs::write(&source, b"rejected source contents")?;
    std::fs::write(&destination, b"occupied destination contents")?;
    let error = std::fs::hard_link(&source, &destination)
        .err()
        .ok_or("occupied destination was replaced")?;
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&source)?, b"rejected source contents");
    assert_eq!(
        std::fs::read(&destination)?,
        b"occupied destination contents"
    );
    std::fs::remove_file(&destination)?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_fleet-sync-test-consumer"))
        .arg(directory.path())
        .arg("link-probe")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let mut lines = BufReader::new(child.stdout.take().ok_or("probe stdout missing")?).lines();
    assert_eq!(
        timeout(Duration::from_secs(10), lines.next_line())
            .await??
            .as_deref(),
        Some("LINK_READY")
    );
    child.kill().await?;
    child.wait().await?;
    let original = std::fs::metadata(&source)?;
    let linked = std::fs::metadata(&destination)?;
    assert_eq!(
        (original.dev(), original.ino()),
        (linked.dev(), linked.ino())
    );
    assert_eq!(std::fs::read(&source)?, b"rejected source contents");
    assert_eq!(std::fs::read(&destination)?, b"rejected source contents");
    std::fs::remove_file(&source)?;
    std::fs::File::open(directory.path())?.sync_all()?;
    assert!(!source.exists());
    assert_eq!(std::fs::read(&destination)?, b"rejected source contents");
    Ok(())
}
