#![cfg(unix)]
#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_test_consumer::{
    ControlRequest, SampleSchema, TestResult, WriterCommand, WriterReply,
};
use fleet_sync_writer::{MoveAsidePhase, RejectedTail, RetainedFile, WriterRegistrationOutcome};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
    row::Row,
};
use std::{os::unix::fs::MetadataExt, path::Path, process::Stdio, time::Duration};
use support::{writer_process::WriterProcess, *};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    time::timeout,
};

fn move_command(
    directory: &Path,
    destination: &Path,
    database: fleet_sync_protocol::DatabaseId,
    retry: bool,
) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fleet-sync-test-consumer"));
    command
        .arg(directory)
        .arg(if retry {
            "retry-move-aside"
        } else {
            "move-aside"
        })
        .arg(destination)
        .arg(database.as_uuid().to_string())
        .kill_on_drop(true);
    command
}

#[tokio::test]
async fn rejected_registration_moves_aside_after_kills_before_link_between_names_and_after_unlink()
-> TestResult {
    let scratch = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tmp");
    std::fs::create_dir_all(&scratch)?;
    let root = tempfile::Builder::new()
        .prefix("rejected-move-")
        .tempdir_in(scratch)?;
    let service = ServiceProcess::start(&root.path().join("service")).await?;
    let endpoint = format!("ws://127.0.0.1:{}/session", service.port);
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    control_request(
        &mut control,
        ControlRequest::SetRegistrationFailure { enabled: true },
    )
    .await?;
    let expected_refusal = WriterRegistrationOutcome::Refused {
        reason: fleet_sync_protocol::RegistrationRefusal::DatabaseRejected {
            reason: fleet_sync_protocol::GateReason {
                code: fleet_sync_protocol::GateReasonCode::new("registration_denied")?,
                detail: "consumer refused registration".into(),
            },
        },
    };
    for (index, phase) in [
        MoveAsidePhase::BeforeLink,
        MoveAsidePhase::AfterLink,
        MoveAsidePhase::AfterUnlink,
    ]
    .into_iter()
    .enumerate()
    {
        let directory = root.path().join(format!("writer-{index}"));
        let source = directory.join("writer.db");
        let destination = root.path().join(format!("rejected-{index}.db"));
        let mut writer = WriterProcess::start(&directory, &endpoint, "create", None).await?;
        let database = writer.database;
        writer.command(WriterCommand::Register).await?;
        match writer.command(WriterCommand::WaitReady).await? {
            WriterReply::Registration(outcome) => assert_eq!(outcome, expected_refusal),
            _ => return Err("rejected registration outcome missing".into()),
        }
        assert!(matches!(
            writer
                .command(WriterCommand::TryWrite {
                    label: "after refusal".into()
                })
                .await?,
            WriterReply::WriteRefused { .. }
        ));
        let position = match writer.command(WriterCommand::Position).await? {
            WriterReply::Position(position) => position,
            _ => return Err("rejected writer position missing".into()),
        };
        writer.stop().await?;
        std::fs::write(&destination, b"another database")?;
        let before = std::fs::read(&source)?;
        let occupied = move_command(&directory, &destination, database, true)
            .output()
            .await?;
        assert!(!occupied.status.success());
        assert_eq!(std::fs::read(&source)?, before);
        assert_eq!(std::fs::read(&destination)?, b"another database");
        std::fs::remove_file(&destination)?;
        let mut child = move_command(&directory, &destination, database, false)
            .arg(serde_json::to_string(&phase)?)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let mut lines = BufReader::new(child.stdout.take().ok_or("move stdout missing")?).lines();
        let observed = timeout(Duration::from_secs(15), lines.next_line())
            .await??
            .ok_or("move exited before phase")?;
        assert_eq!(
            observed,
            format!("MOVE_PHASE {}", serde_json::to_string(&phase)?)
        );
        child.kill().await?;
        child.wait().await?;
        assert_eq!(source.exists(), phase != MoveAsidePhase::AfterUnlink);
        assert_eq!(destination.exists(), phase != MoveAsidePhase::BeforeLink);
        if phase == MoveAsidePhase::AfterLink {
            let original = std::fs::metadata(&source)?;
            let linked = std::fs::metadata(&destination)?;
            assert_eq!(
                (original.dev(), original.ino()),
                (linked.dev(), linked.ino())
            );
        }
        for sidecar in [
            directory.join("writer.db-wal"),
            directory.join("writer.db-shm"),
        ] {
            assert!(std::fs::metadata(sidecar).map_or(true, |metadata| metadata.len() == 0));
        }
        let retried = move_command(&directory, &destination, database, true)
            .output()
            .await?;
        assert!(
            retried.status.success(),
            "{}",
            String::from_utf8_lossy(&retried.stderr)
        );
        assert!(!source.exists());
        let repeated = move_command(&directory, &destination, database, true)
            .output()
            .await?;
        assert!(
            repeated.status.success(),
            "{}",
            String::from_utf8_lossy(&repeated.stderr)
        );
        let mut connection = sqlx_turso::TursoConnectOptions::new()
            .filename(&destination)
            .create_if_missing(false)
            .connect()
            .await?;
        let row = query::<sqlx_turso::Turso>(
            "SELECT exact_value,typeof(exact_value) AS class,payload FROM samples",
        )
        .fetch_one(&mut connection)
        .await?;
        assert_eq!(row.try_get::<i64, _>("exact_value")?, 9_007_199_254_740_993);
        assert_eq!(row.try_get::<String, _>("class")?, "integer");
        assert_eq!(row.try_get::<Vec<u8>, _>("payload")?, [0, 255, 128]);
        let tail: RejectedTail = serde_json::from_str(
            &query::<sqlx_turso::Turso>("SELECT record FROM _fleet_sync_rejected_tails")
                .fetch_one(&mut connection)
                .await?
                .try_get::<String, _>("record")?,
        )?;
        assert_eq!(tail.database, database);
        assert_eq!(tail.last_commit, position);
        assert!(tail.ended_at < tail.last_commit);
        connection.close().await?;
        // All writer and mover processes have ended. Import the public retained
        // API as a consumer, preserving the real registry gate's exact refusal.
        let retained = RetainedFile::open_read_only(&destination, SampleSchema::new()?).await?;
        assert_eq!(retained.registration_outcome(), &expected_refusal);
        let mut read = retained.reader().begin_read().await?;
        let row = query::<sqlx_turso::Turso>(
            "SELECT exact_value,typeof(exact_value) AS class,payload FROM samples",
        )
        .fetch_one(&mut read)
        .await?;
        assert_eq!(row.try_get::<i64, _>("exact_value")?, 9_007_199_254_740_993);
        assert_eq!(row.try_get::<String, _>("class")?, "integer");
        assert_eq!(row.try_get::<Vec<u8>, _>("payload")?, [0, 255, 128]);
        read.close().await?;
        drop(retained);
    }
    service.stop(&mut control).await?;
    Ok(())
}
