#[path = "support/copy_fixture.rs"]
#[allow(dead_code)]
mod fixture;
#[allow(dead_code, unused_imports)]
mod support;
use durable_steps::{CallFailure, CallId, CallTarget, OperationId, StepName};
use fixture::*;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::*;
use fleet_sync_tokio_runner::{CallEnvelope, CallRouter, WriterSessionCalls};
use fleet_sync_writer::*;
use std::{path::Path, sync::Arc};
use support::control_request;

async fn marker(
    service: &mut CopyService,
    database: DatabaseId,
    replacement: String,
) -> TestResult<String> {
    match control_request(
        &mut service.control,
        ControlRequest::CopyFault(CopyFaultRequest::ReplaceMarker {
            database,
            replacement,
        }),
    )
    .await?
    {
        ControlAnswer::CopyMarker(previous) => Ok(previous),
        _ => Err("copy owner did not replace the test marker".into()),
    }
}
async fn seed_call(
    calls: &WriterSessionCalls<SampleSchema, SampleProtocol>,
    generation: durable_steps::Uuid,
    request: SeedFileRequest,
) -> Result<serde_json::Value, CallFailure> {
    let name = match &request {
        SeedFileRequest::Open { .. } => "open".into(),
        SeedFileRequest::Page { page, .. } => format!("page-{page}"),
        SeedFileRequest::Complete { .. } => "complete".into(),
    };
    calls
        .dispatch(CallEnvelope {
            kind: SeedFile::KIND,
            target: serde_json::to_value(SeedFile {
                receiver: local_receiver().map_err(|_| CallFailure::AnswerLost)?,
            })
            .map_err(|_| CallFailure::AnswerLost)?,
            request: serde_json::to_value(request).map_err(|_| CallFailure::AnswerLost)?,
            call: CallId {
                operation: OperationId::new(generation).map_err(|_| CallFailure::AnswerLost)?,
                step: StepName::new(name).map_err(|_| CallFailure::AnswerLost)?,
            },
        })
        .await
}
fn seed_answer(value: serde_json::Value) -> TestResult<SeedFileAnswer> {
    serde_json::from_value::<Result<SeedFileAnswer, SeedCopyRefusal>>(value)?
        .map_err(|reason| format!("unexpected terminal seed refusal: {reason:?}").into())
}
fn known_wait(result: Result<serde_json::Value, CallFailure>) -> TestResult {
    let Err(CallFailure::Retryable { reason }) = result else {
        return Err("seed unavailability must be a typed retry, not a terminal answer".into());
    };
    assert!(matches!(
        CopyUnavailableReason::decode_retry(&reason.code, &reason.detail)?,
        CopyUnavailableReason::Damaged { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn seed_begin_page_and_complete_keep_their_generation_and_pinned_snapshot_when_copy_waits()
-> TestResult {
    let root = tempfile::tempdir()?;
    let writer = Arc::new(
        ReplicatedDatabase::create(
            &root.path().join("writer.db"),
            SampleSchema::new()?,
            writer_peer()?,
            lease_policy(),
            WriterOptions {
                allow_provisional_writes: true,
            },
        )
        .await?,
    );
    let result = observe_seed_retries(root.path(), writer.clone()).await;
    writer.close().await;
    result
}
async fn observe_seed_retries(
    root: &Path,
    writer: Arc<ReplicatedDatabase<SampleSchema>>,
) -> TestResult {
    write_sample(&writer, "pinned seed row").await?;
    let snapshot = writer.snapshot().await?;
    let position = snapshot.position();
    snapshot.close().await?;
    let service_root = root.join("service");
    let mut service = CopyService::start(&service_root, &root.join("client.db")).await?;
    let calls = WriterSessionCalls::new(
        writer.clone(),
        local_receiver()?,
        service.session.requests.clone(),
    );
    let observed: TestResult = async {
        assert!(matches!(service.session.requests.request(OperationRequest::Lease(LeaseRequest::Register(Registration {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?, database: writer.database(), writer: writer_peer()?, origin: Origin::Seeded { position },
        }))).await?, OperationAnswer::Lease(LeaseAnswer::Register(RegistrationAnswer::Registered { .. }))));
        let generation = durable_steps::Uuid::now_v7();
        let original = marker(&mut service, writer.database(), "{damaged-before-seed-begin".into()).await?;
        known_wait(seed_call(&calls, generation, SeedFileRequest::Open { generation, position }).await)?;
        write_sample(&writer, "written after the pinned begin refusal").await?;
        marker(&mut service, writer.database(), original).await?;
        let (seed, schema) = match seed_answer(seed_call(&calls, generation, SeedFileRequest::Open { generation, position }).await?)? {
            SeedFileAnswer::Opened { seed, schema, position: actual, .. } => { assert_eq!(actual, position); (seed, schema) },
            _ => return Err("seed did not open after repair".into()),
        };
        let mut cursor = None;
        let mut pages = 0;
        let mut tables = Vec::new();
        let mut total_rows = 0;
        loop {
            let page = SeedFileRequest::Page { generation, seed, page: pages, cursor: cursor.clone() };
            if pages == 0 {
                let original = marker(&mut service, writer.database(), "{damaged-before-seed-page".into()).await?;
                known_wait(seed_call(&calls, generation, page.clone()).await)?;
                write_sample(&writer, "written after the pinned page refusal").await?;
                marker(&mut service, writer.database(), original).await?;
            }
            match seed_answer(seed_call(&calls, generation, page).await?)? {
                SeedFileAnswer::Page { rows, next, completed, .. } => {
                    total_rows += rows;
                    cursor = next;
                    if let Some(table) = completed { tables.push((table.table, table.digest)); }
                }
                _ => return Err("seed page did not resume".into()),
            }
            pages += 1;
            if cursor.is_none() { break; }
        }
        assert_eq!(total_rows, 1, "later writer commits must stay outside the pinned seed");
        let complete = SeedFileRequest::Complete { generation, request: SeedComplete { database: writer.database(), seed, position, pages, schema, table_digests: tables } };
        let original = marker(&mut service, writer.database(), "{damaged-before-seed-complete".into()).await?;
        known_wait(seed_call(&calls, generation, complete.clone()).await)?;
        marker(&mut service, writer.database(), original).await?;
        assert!(matches!(seed_answer(seed_call(&calls, generation, complete).await?)?, SeedFileAnswer::Published { position: actual } if actual == position));
        let sent = service.session.outgoing_replication().await;
        for kind in ["seedBegin", "seedPage", "seedComplete"] {
            let attempts: Vec<_> = sent.iter().filter(|value| value["kind"] == kind).collect();
            assert!(attempts.len() >= 2);
            assert_eq!(attempts[0], attempts[1], "the retried operation must keep its entire original identity/content");
        }
        assert_eq!(service.session.connection_count(), 1);
        Ok(())
    }.await;
    calls.close_seed().await;
    service.stop().await?;
    observed?;
    let copy = read_copy_snapshot(&copy_file_path(&service_root, writer.database())?).await?;
    assert_eq!(copy.rows.len(), 1);
    assert_eq!(copy.rows[0].1, "pinned seed row");
    assert_eq!(copy.hooks.len(), 1);
    Ok(())
}

#[tokio::test]
async fn oversized_seed_page_fails_the_deduplicated_seed_operation_without_a_retry() -> TestResult {
    use durable_steps::{AtomicStore, OperationFailure, OperationState, storage::JournalStorage};
    use fleet_sync_tokio_runner::NativeWriterRuntime;
    use sqlx_core::{
        connection::{ConnectOptions, Connection},
        migrate::Migrator,
    };
    use sqlx_turso::TursoConnectOptions;
    use std::time::Duration;

    assert!(
        SeedFile::DEDUPLICATES,
        "this regression must exercise the actual deduplicated call"
    );
    let root = tempfile::tempdir()?;
    let path = root.path().join("writer.db");
    let mut connection = TursoConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .connect()
        .await?;
    Migrator::with_migrations(SampleSchema::new()?.migrations().to_vec())
        .run_direct(None, &mut connection, false)
        .await?;
    connection.close().await?;
    let writer = Arc::new(
        ReplicatedDatabase::adopt(
            &path,
            SampleSchema::new()?,
            writer_peer()?,
            lease_policy(),
            WriterOptions {
                allow_provisional_writes: true,
            },
        )
        .await?,
    );
    write_sample(&writer, "one real source row").await?;
    let service =
        CopyService::start(&root.path().join("service"), &root.path().join("client.db")).await?;
    service.session.inject_oversized_seed_page();
    let runtime = NativeWriterRuntime::new(
        writer.clone(),
        local_receiver()?,
        service.session.requests.clone(),
    )
    .await?;
    let observed: TestResult = async {
        let registration = runtime.start_registration().await?;
        let engine = runtime.engine();
        let mut failures = Vec::new();
        for _ in 0..8 {
            let report = tokio::time::timeout(Duration::from_secs(15), engine.drive_due()).await??;
            let replies = service.session.seed_page_answers().await?;
            assert!(!report.retried.contains(&registration), "oversize seed scheduled a retry: {report:?}; oversized pages={}; actual receiver replies={replies:?}", service.session.oversized_seed_pages());
            failures.extend(report.failed.into_iter().filter(|(operation, _)| *operation == registration));
            if !failures.is_empty() { break; }
        }
        let [(operation, OperationFailure::Refused { code, detail })] = failures.as_slice() else {
            return Err(format!("expected one terminal seed refusal: {failures:?}").into());
        };
        assert_eq!(code, "seed_page_refused");
        assert!(detail.contains("UnitTooLarge"));
        let replies = service.session.seed_page_answers().await?;
        let Some(ReplicationAnswer::SeedPage(answer)) = replies.last() else {
            return Err(format!("missing actual seed refusal: {replies:?}").into());
        };
        assert_eq!(serde_json::to_value(answer)?, serde_json::json!({"kind":"unitTooLarge"}));
        let status = engine.status(operation).await?;
        assert!(matches!(status.state, OperationState::Failed {
            reason: OperationFailure::Refused { code: stored_code, detail: stored_detail },
        } if stored_code == *code && stored_detail == *detail));
        let operation = *operation;
        let due = runtime.store().transaction(move |scope| Box::pin(async move {
            scope.due(durable_steps::SyncTime::from_unix_millis(i64::MAX)).await
        })).await?;
        assert!(!due.contains(&operation), "failed seed remains scheduled");
        assert_eq!(service.session.oversized_seed_pages(), 1);
        let sent_before = service.session.outgoing_replication().await.len();
        let repeated = tokio::time::timeout(Duration::from_secs(15), engine.drive_due()).await??;
        assert!(!repeated.retried.contains(&operation));
        assert!(repeated.failed.is_empty(), "terminal failure was repeated");
        assert_eq!(service.session.oversized_seed_pages(), 1, "failed seed was dispatched again");
        let sent = service.session.outgoing_replication().await;
        assert_eq!(sent.len(), sent_before, "terminal operation sent another replication request");
        assert!(sent.iter().any(|value| value["kind"] == "seedPage"));
        println!("actual deduplicated SeedFile refused once with UnitTooLarge; operation={operation:?}; no due retry or later dispatch");
        Ok(())
    }.await;
    runtime.transport().close_seed().await;
    service.stop().await?;
    writer.close().await;
    observed
}
