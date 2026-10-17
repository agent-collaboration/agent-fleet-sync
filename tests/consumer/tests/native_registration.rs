#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_lease::WriterState;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{
    ControlAnswer, ControlRequest, TestResult, WriterCommand, WriterReply,
};
use fleet_sync_writer::WriterRegistrationOutcome;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
    row::Row,
};
use support::{writer_process::WriterProcess, *};

#[tokio::test]
async fn adopted_registration_freezes_provisional_writes_and_restarts_seed_from_same_position_after_writer_kill()
-> TestResult {
    let root = tempfile::tempdir()?;
    let source = ServiceProcess::start(&root.path().join("service")).await?;
    let endpoint = format!("ws://127.0.0.1:{}/session", source.port);
    let writer_path = root.path().join("writer");
    let mut writer = WriterProcess::start(&writer_path, &endpoint, "adopt", None).await?;
    let (database, session) = (writer.database, writer.session);
    assert!(matches!(
        writer.command(WriterCommand::State).await?,
        WriterReply::State(WriterState::Provisional)
    ));
    writer.command(WriterCommand::HoldSeedPage).await?;
    let operation = match writer.command(WriterCommand::Register).await? {
        WriterReply::Operation(operation) => operation,
        _ => return Err("registration was not admitted".into()),
    };
    let first = match writer.command(WriterCommand::WaitSeedPage).await? {
        WriterReply::SeedPage(page) => page,
        _ => return Err("seed page was not observed".into()),
    };
    assert_eq!(first.seed.generation, 1);
    assert_eq!((first.page, first.rows), (0, 1));
    let position = match writer.command(WriterCommand::Position).await? {
        WriterReply::Position(position) => position,
        _ => return Err("no frozen position".into()),
    };
    assert!(
        position.sequence.value() > 0,
        "opt-in provisional write was not captured before freezing"
    );
    assert!(matches!(
        writer
            .command(WriterCommand::TryWrite {
                label: "must be refused during seed".into()
            })
            .await?,
        WriterReply::WriteRefused { seeding: true, .. }
    ));
    writer.kill().await?;
    let mut writer =
        WriterProcess::start(&writer_path, &endpoint, "resume", Some(database)).await?;
    assert_eq!(writer.session, session);
    assert!(
        matches!(writer.command(WriterCommand::Position).await?,WriterReply::Position(actual) if actual==position)
    );
    assert!(matches!(
        writer.command(WriterCommand::State).await?,
        WriterReply::State(WriterState::Seeding { .. })
    ));
    writer
        .command(WriterCommand::HoldSeedPageAt { page: 2 })
        .await?;
    let second = match writer.command(WriterCommand::WaitSeedPage).await? {
        WriterReply::SeedPage(page) => page,
        _ => return Err("resumed seed page was not observed".into()),
    };
    assert_eq!(second.seed.generation, 2);
    assert_ne!(second.generation, first.generation);
    assert_eq!(second.page, 2);
    let steps = match writer
        .command(WriterCommand::JournalSteps { operation })
        .await?
    {
        WriterReply::JournalSteps(steps) => steps,
        _ => return Err("writer journal was not observed".into()),
    };
    assert!(!steps.is_empty());
    for step in steps {
        use durable_steps::storage::StepState;
        let saved = match step.state {
            StepState::Local { output } | StepState::CallDone { output, .. } => {
                String::from_utf8(output)?
            }
            StepState::CallPending { intent, .. } => serde_json::to_string(&intent)?,
        };
        assert!(
            !saved.contains("adopted-")
                && !saved.contains("provisional")
                && !saved.contains("\"rows\":["),
            "seed journal contains a row payload"
        );
    }
    writer.command(WriterCommand::ReleaseSeedPage).await?;
    assert!(matches!(
        writer.command(WriterCommand::WaitReady).await?,
        WriterReply::Registration(WriterRegistrationOutcome::Ready { .. })
    ));
    assert!(
        matches!(writer.command(WriterCommand::Register).await?,WriterReply::Operation(replayed) if replayed==operation)
    );
    assert!(matches!(
        writer.command(WriterCommand::RejectRegistration).await?,
        WriterReply::LateRejectionRefused {
            already_granted: true
        }
    ));
    let mut reader = source
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let rows = copy_rows(&mut reader, database).await?;
    let samples = rows.get("samples").ok_or("samples missing")?;
    assert_eq!(samples.len(), 301);
    assert!(samples.iter().all(|row|matches!(row.as_slice(),[SqlValue::Text(id),SqlValue::Text(_),SqlValue::Integer(9_007_199_254_740_993),SqlValue::Blob(bytes)] if canonical_uuid_v7(id) && bytes==&[0,255,128])));
    assert_eq!(rows.get("readings").map(Vec::len), Some(1));
    let later = match writer
        .command(WriterCommand::TryWrite {
            label: "after seed".into(),
        })
        .await?
    {
        WriterReply::Position(position) => position,
        _ => return Err("writer did not admit after ready".into()),
    };
    writer
        .command(WriterCommand::WaitAcked { through: later })
        .await?;
    assert_eq!(
        copy_rows(&mut reader, database)
            .await?
            .get("samples")
            .map(Vec::len),
        Some(302)
    );
    writer.command(WriterCommand::WaitPeriodicRenewal).await?;
    writer.stop().await?;
    let mut control = source.connect("control", "Bearer consumer-control").await?;
    assert!(
        matches!(control_request(&mut control,ControlRequest::CopyStatus {database}).await?,ControlAnswer::CopyStatus(CopyStatusAnswer::Status(status)) if status.acked==later)
    );
    source.stop(&mut control).await?;
    println!(
        "OBS adopted writer: 301 preexisting cells plus a captured provisional write freeze at P; process kill loses pinned read and restarts a new seed at the same P; same session/registration ID; gate opens only at ready; later capture follows over the same WebSocket"
    );
    Ok(())
}

#[tokio::test]
async fn empty_registration_and_periodic_renewals_use_owned_durable_tasks_over_real_websocket()
-> TestResult {
    let root = tempfile::tempdir()?;
    let source = ServiceProcess::start(&root.path().join("service")).await?;
    let endpoint = format!("ws://127.0.0.1:{}/session", source.port);
    let mut writer =
        WriterProcess::start(&root.path().join("writer"), &endpoint, "create", None).await?;
    let database = writer.database;
    writer.command(WriterCommand::Register).await?;
    assert!(matches!(
        writer.command(WriterCommand::WaitReady).await?,
        WriterReply::Registration(WriterRegistrationOutcome::Ready { .. })
    ));
    let position = match writer.command(WriterCommand::Position).await? {
        WriterReply::Position(position) => position,
        _ => return Err("no writer position".into()),
    };
    writer
        .command(WriterCommand::WaitAcked { through: position })
        .await?;
    let mut reader = source
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    assert_eq!(
        copy_rows(&mut reader, database)
            .await?
            .get("samples")
            .map(Vec::len),
        Some(1)
    );
    writer.command(WriterCommand::WaitPeriodicRenewal).await?;
    writer.stop().await?;
    let mut journal = sqlx_turso::TursoConnectOptions::new()
        .filename(root.path().join("writer/writer.db"))
        .create_if_missing(false)
        .foreign_keys(false)
        .connect()
        .await?;
    let record: serde_json::Value = serde_json::from_str(
        &query::<sqlx_turso::Turso>("SELECT record FROM _fleet_sync_writer WHERE singleton=1")
            .fetch_one(&mut journal)
            .await?
            .try_get::<String, _>("record")?,
    )?;
    assert_eq!(
        record
            .get("authority")
            .and_then(|authority| authority.get("kind"))
            .and_then(serde_json::Value::as_str),
        Some("renewal")
    );
    journal.close().await?;
    let mut control = source.connect("control", "Bearer consumer-control").await?;
    source.stop(&mut control).await?;
    println!(
        "OBS empty-origin registration runs through durable writer/registry tasks and the captured migration and provisional row follow without a seed"
    );
    Ok(())
}
