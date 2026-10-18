//! Seed begin/page/complete keep their identities and the writer's pinned snapshot.
use super::{fault_controls::*, *};
use durable_steps::{CallFailure, CallId, CallTarget, OperationId, StepName};
use fleet_sync_tokio_runner::{CallEnvelope, CallRouter};

async fn call(
    calls: &WriterSessionCalls<SampleSchema, SampleProtocol>,
    receiver: ReceiverId,
    generation: durable_steps::Uuid,
    request: SeedFileRequest,
) -> Result<Value, CallFailure> {
    let step = match &request {
        SeedFileRequest::Open { .. } => "open".to_owned(),
        SeedFileRequest::Page { page, .. } => format!("page-{page}"),
        SeedFileRequest::Complete { .. } => "complete".to_owned(),
    };
    // Keep the explicit target and CallId throughout each refusal/retry pair.
    calls
        .dispatch(CallEnvelope {
            kind: SeedFile::KIND,
            target: serde_json::to_value(SeedFile { receiver })
                .map_err(|_| CallFailure::AnswerLost)?,
            request: serde_json::to_value(request).map_err(|_| CallFailure::AnswerLost)?,
            call: CallId {
                operation: OperationId::new(generation).map_err(|_| CallFailure::AnswerLost)?,
                step: StepName::new(step).map_err(|_| CallFailure::AnswerLost)?,
            },
        })
        .await
}

fn answer(value: Value) -> TestResult<SeedFileAnswer> {
    serde_json::from_value::<Result<SeedFileAnswer, SeedCopyRefusal>>(value)?
        .map_err(|reason| format!("unexpected final seed refusal: {reason:?}").into())
}
fn check_wait(
    result: Result<Value, CallFailure>,
    stage: &str,
    observations: &mut Observations,
) -> TestResult {
    let observed = match result {
        Err(CallFailure::Retryable { reason }) => {
            let decoded = CopyUnavailableReason::decode_retry(&reason.code, &reason.detail)?;
            observations.check(
                &format!("seed-{stage}-typed-copy-wait"),
                matches!(decoded, CopyUnavailableReason::Damaged { .. }),
                json!({"reason":"damaged","state":"retryable"}),
                json!({"reason":decoded}),
            );
            return Ok(());
        }
        Ok(value) => json!({"terminalAnswer":value}),
        Err(error) => json!({"failure":format!("{error:?}")}),
    };
    observations.check(
        &format!("seed-{stage}-typed-copy-wait"),
        false,
        json!({"reason":"damaged","state":"retryable"}),
        observed,
    );
    Err("seed path did not enter a typed copy wait".into())
}

pub(super) async fn observe(root: &Path, observations: &mut Observations) -> TestResult {
    let root = root.join("seed-resumption");
    tokio::fs::create_dir_all(&root).await?;
    let peer = new_peer().await?;
    let receiver = new_receiver().await?;
    let writer = new_writer(&root.join("writer.db"), peer, true).await?;
    let result: TestResult = async {
        write_sample(&writer, "pinned seed row").await?;
        let snapshot = writer.snapshot().await?;
        let position = snapshot.position();
        snapshot.close().await?;
        let service_root = root.join("service");
        let mut running =
            RunningCopy::start(&service_root, &root.join("client.db"), peer, receiver).await?;
        let calls =
            WriterSessionCalls::new(writer.clone(), receiver, running.session.requests.clone());
        let seeded: TestResult = async {
            let registered = running
                .session
                .requests
                .request(OperationRequest::Lease(LeaseRequest::Register(
                    Registration {
                        attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
                        database: writer.database(),
                        writer: peer,
                        origin: Origin::Seeded { position },
                    },
                )))
                .await?;
            if !matches!(
                registered,
                OperationAnswer::Lease(LeaseAnswer::Register(
                    RegistrationAnswer::Registered { .. }
                ))
            ) {
                return Err("seed writer registration failed".into());
            }
            let generation = durable_steps::Uuid::now_v7();
            let original = running
                .marker(writer.database(), "{damaged-before-seed-begin".into())
                .await?;
            check_wait(
                call(
                    &calls,
                    receiver,
                    generation,
                    SeedFileRequest::Open {
                        generation,
                        position,
                    },
                )
                .await,
                "begin",
                observations,
            )?;
            write_sample(&writer, "outside pinned begin").await?;
            running.marker(writer.database(), original).await?;
            let (seed, schema) = match answer(
                call(
                    &calls,
                    receiver,
                    generation,
                    SeedFileRequest::Open {
                        generation,
                        position,
                    },
                )
                .await?,
            )? {
                SeedFileAnswer::Opened {
                    seed,
                    schema,
                    position: actual,
                    ..
                } if actual == position => (seed, schema),
                _ => return Err("repaired seed did not retain pinned position".into()),
            };
            let mut cursor = None;
            let mut pages = 0;
            let mut tables = Vec::new();
            let mut total_rows = 0;
            loop {
                let page = SeedFileRequest::Page {
                    generation,
                    seed,
                    page: pages,
                    cursor: cursor.clone(),
                };
                if pages == 0 {
                    let original = running
                        .marker(writer.database(), "{damaged-before-seed-page".into())
                        .await?;
                    check_wait(
                        call(&calls, receiver, generation, page.clone()).await,
                        "page",
                        observations,
                    )?;
                    write_sample(&writer, "outside pinned page").await?;
                    running.marker(writer.database(), original).await?;
                }
                match answer(call(&calls, receiver, generation, page).await?)? {
                    SeedFileAnswer::Page {
                        rows,
                        next,
                        completed,
                        ..
                    } => {
                        total_rows += rows;
                        cursor = next;
                        if let Some(table) = completed {
                            tables.push((table.table, table.digest));
                        }
                    }
                    _ => return Err("seed page failed to resume".into()),
                }
                pages += 1;
                if cursor.is_none() {
                    break;
                }
            }
            observations.check(
                "seed-retains-pinned-snapshot",
                total_rows == 1,
                json!({"rows":1,"position":position}),
                json!({"rows":total_rows,"pages":pages}),
            );
            let complete = SeedFileRequest::Complete {
                generation,
                request: SeedComplete {
                    database: writer.database(),
                    seed,
                    position,
                    pages,
                    schema,
                    table_digests: tables,
                },
            };
            let original = running
                .marker(writer.database(), "{damaged-before-seed-complete".into())
                .await?;
            check_wait(
                call(&calls, receiver, generation, complete.clone()).await,
                "complete",
                observations,
            )?;
            running.marker(writer.database(), original).await?;
            let published = answer(call(&calls, receiver, generation, complete).await?)?;
            observations.check(
                "seed-complete-resumes-original-position",
                matches!(published,SeedFileAnswer::Published {position:actual} if actual==position),
                json!({"position":position}),
                json!(published),
            );
            let wire = running.session.wire.frames.lock().await.clone();
            for kind in ["seedBegin", "seedPage", "seedComplete"] {
                let operations = wire
                    .iter()
                    .filter(|row| {
                        row["direction"] == "out"
                            && row["frame"]["value"]["body"]["kind"] == "replication"
                            && row["frame"]["value"]["body"]["value"]["kind"] == kind
                    })
                    .map(|row| row["frame"]["value"]["body"]["value"].clone())
                    .collect::<Vec<_>>();
                observations.check(
                    &format!("{kind}-retries-identical-content"),
                    operations.len() >= 2 && operations[0] == operations[1],
                    json!({"identical":true}),
                    json!({"operations":operations}),
                );
            }
            Ok(())
        }
        .await;
        calls.close_seed().await;
        running.stop().await?;
        seeded?;
        let copy = read_snapshot(&copy_path(
            &service_root.join("service"),
            receiver,
            writer.database(),
        ))
        .await?;
        observations.check(
            "seed-publishes-only-pinned-row",
            copy.rows.len() == 1
                && copy.rows[0].1 == "pinned seed row"
                && copy.hook_events.len() == 1,
            json!({"rows":1,"label":"pinned seed row","hookEvents":1}),
            json!(copy),
        );
        Ok(())
    }
    .await;
    writer.close().await;
    result
}
