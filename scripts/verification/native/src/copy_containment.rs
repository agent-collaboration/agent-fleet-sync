//! Promotes X7's real native damage/repair route; assertions use VD SO57.
mod answer_loss;
mod registry_resume;
use crate::{
    Observations, fixture_service::new_peer, session_observation::*, support::ServiceProcess,
};
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::{SampleProtocol, SampleSchema, TestResult, insert_sample};
use fleet_sync_tokio_runner::WriterSessionCalls;
use fleet_sync_writer::*;
use runtime_primitives::{Clock, SystemClock};
use serde::Serialize;
use serde_json::{Value, json};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::{path::Path, sync::Arc, time::Duration};

struct Annotation;
impl PieceAnnotation for Annotation {
    type Protocol = SampleProtocol;
    fn annotate(&self, _: &[CapturedTransaction<'_>]) -> Option<String> {
        None
    }
}
type Sender = ReplicationSender<
    SampleSchema,
    Annotation,
    (),
    WriterSessionCalls<SampleSchema, SampleProtocol>,
>;
fn sender(
    writer: Arc<ReplicatedDatabase<SampleSchema>>,
    receiver: ReceiverId,
    session: &ObservedSession,
) -> Sender {
    ReplicationSender::new(
        writer.clone(),
        Arc::new(Annotation),
        Arc::new(()),
        Arc::new(WriterSessionCalls::new(
            writer,
            receiver,
            session.requests.clone(),
        )),
        SenderOptions::default(),
    )
}
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(600),
        renew_interval: Duration::from_secs(30),
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct CopySnapshot {
    record: Option<String>,
    rows: Vec<(String, String)>,
    receipts: Vec<(String, String)>,
    hook_events: Vec<(String, String)>,
}
async fn read_snapshot(path: &Path) -> TestResult<CopySnapshot> {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .connect()
        .await?;
    let record = query::<Turso>("SELECT record FROM _fleet_sync_copy WHERE singleton=1")
        .fetch_optional(&mut connection)
        .await?
        .map(|row| row.try_get("record"))
        .transpose()?;
    let rows = query::<Turso>("SELECT id,label FROM samples ORDER BY id")
        .fetch_all(&mut connection)
        .await?
        .into_iter()
        .map(|row| Ok((row.try_get("id")?, row.try_get("label")?)))
        .collect::<Result<Vec<_>, sqlx_core::Error>>()?;
    let receipts =
        query::<Turso>("SELECT identity,digest FROM _fleet_sync_receipts ORDER BY identity")
            .fetch_all(&mut connection)
            .await?
            .into_iter()
            .map(|row| Ok((row.try_get("identity")?, row.try_get("digest")?)))
            .collect::<Result<Vec<_>, sqlx_core::Error>>()?;
    let hook_events =
        query::<Turso>("SELECT event_id,annotation FROM hook_events ORDER BY event_id")
            .fetch_all(&mut connection)
            .await?
            .into_iter()
            .map(|row| Ok((row.try_get("event_id")?, row.try_get("annotation")?)))
            .collect::<Result<Vec<_>, sqlx_core::Error>>()?;
    connection.close().await?;
    Ok(CopySnapshot {
        record,
        rows,
        receipts,
        hook_events,
    })
}
async fn write_record(path: &Path, record: &str) -> TestResult {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .connect()
        .await?;
    let result = query::<Turso>("UPDATE _fleet_sync_copy SET record=? WHERE singleton=1")
        .bind(record.to_owned())
        .execute(&mut connection)
        .await?;
    if result.rows_affected() != 1 {
        return Err("fault/repair did not update exactly one copy marker".into());
    }
    connection.close().await?;
    Ok(())
}
async fn write_sample(writer: &ReplicatedDatabase<SampleSchema>, label: &str) -> TestResult {
    let mut transaction = writer.begin_write().await?;
    insert_sample(
        &mut transaction,
        durable_steps::Uuid::now_v7().to_string(),
        label.into(),
        9_007_199_254_740_993,
        vec![0, 255, 128],
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}
async fn register(
    writer: &ReplicatedDatabase<SampleSchema>,
    peer: PeerId,
    session: &ObservedSession,
) -> TestResult<LeaseStatus> {
    let answer = session
        .requests
        .request(OperationRequest::Lease(LeaseRequest::Register(
            Registration {
                attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
                database: writer.database(),
                writer: peer,
                origin: Origin::Empty,
            },
        )))
        .await?;
    let lease = match answer {
        OperationAnswer::Lease(LeaseAnswer::Register(RegistrationAnswer::Registered {
            lease,
            ..
        })) => lease,
        _ => return Err("copy fixture registration failed".into()),
    };
    let started = SystemClock.now();
    let renewal = session
        .requests
        .request(OperationRequest::Lease(LeaseRequest::Renew(RenewRequest {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database: writer.database(),
            epoch: lease.epoch,
        })))
        .await?;
    match renewal {
        OperationAnswer::Lease(LeaseAnswer::Renew(answer)) => {
            writer.observe_renewal(answer, started).await?
        }
        _ => return Err("copy fixture renewal failed".into()),
    };
    Ok(lease)
}
fn copy_path(root: &Path, receiver: ReceiverId, database: DatabaseId) -> std::path::PathBuf {
    root.join(format!(
        "copy-{}-{}.db",
        receiver.as_uuid(),
        database.as_uuid()
    ))
}

pub async fn run(root: &Path, observations: &mut Observations) -> TestResult {
    let peer = new_peer()?;
    let receiver = ReceiverId::new(durable_steps::Uuid::now_v7())?;
    let service_root = root.join("service");
    let first = ServiceProcess::start_as(&service_root, receiver.as_uuid().as_u128()).await?;
    let mut control = first.connect("control", "Bearer consumer-control").await?;
    let session = ObservedSession::start(
        &format!("ws://127.0.0.1:{}/session", first.port),
        peer,
        &root.join("initial-client.db"),
    )
    .await?;
    let setup: TestResult<Vec<Arc<ReplicatedDatabase<SampleSchema>>>> = async {
        let mut writers = Vec::new();
        for label in ["damaged", "healthy"] {
            let writer = Arc::new(
                ReplicatedDatabase::create(
                    &root.join(format!("{label}-writer.db")),
                    SampleSchema::new()?,
                    peer,
                    policy(),
                    WriterOptions::default(),
                )
                .await?,
            );
            let lease = register(&writer, peer, &session).await?;
            let sending = sender(writer.clone(), receiver, &session);
            sending
                .add_copy(
                    receiver,
                    CopyRole::Authoritative,
                    ReplicationPosition::new(lease.epoch, CommitSequence::new(0)),
                )
                .await?;
            sending.replicate_once(receiver).await?;
            write_sample(&writer, "before-fault").await?;
            sending.replicate_once(receiver).await?;
            writers.push(writer);
        }
        Ok(writers)
    }
    .await;
    let initial_wire = session.stop().await?;
    first.stop(&mut control).await?;
    println!(
        "{}",
        json!({"case":"copy-fixture-ready","wire":initial_wire})
    );
    let writers = setup?;
    let damaged = writers.first().ok_or("damaged writer missing")?;
    let healthy = writers.get(1).ok_or("healthy writer missing")?;
    let copy_directory = service_root.join("service");
    let damaged_path = copy_path(&copy_directory, receiver, damaged.database());
    let healthy_path = copy_path(&copy_directory, receiver, healthy.database());
    let before_damaged = read_snapshot(&damaged_path).await?;
    let before_healthy = read_snapshot(&healthy_path).await?;
    let original_marker = before_damaged
        .record
        .as_deref()
        .ok_or("known-good copy marker missing")?;
    write_record(&damaged_path, "{invalid-copy-record").await?;
    let faulted = read_snapshot(&damaged_path).await?;
    let second = ServiceProcess::start_as(&service_root, receiver.as_uuid().as_u128()).await?;
    let mut control = second.connect("control", "Bearer consumer-control").await?;
    let session = ObservedSession::start(
        &format!("ws://127.0.0.1:{}/session", second.port),
        peer,
        &root.join("fault-client.db"),
    )
    .await?;
    let driven:TestResult<Value>=async {
        write_sample(damaged,"after-fault-damaged").await?;
        write_sample(healthy,"after-fault-healthy").await?;
        let bad_sender=sender(damaged.clone(),receiver,&session);
        let prior=bad_sender.copies().await?;
        let bad_result=bad_sender.replicate_once(receiver).await;
        let after=bad_sender.copies().await?;
        let good_result=sender(healthy.clone(),receiver,&session).replicate_once(receiver).await;
        let frames=session.wire.frames.lock().await.clone();
        let pending_frame=frames.iter().find(|row|row["direction"]=="out" && row["frame"]["value"]["body"]["kind"]=="replication" && row["frame"]["value"]["body"]["value"]["kind"]=="piece" && row["frame"]["value"]["body"]["value"]["value"]["database"]==json!(damaged.database())).map(|row|row["frame"]["value"]["frame"].clone());
        let typed=pending_frame.as_ref().is_some_and(|frame|frames.iter().any(|row|row["direction"]=="in" && row["frame"]["kind"]=="answer" && row["frame"]["value"]["frame"]==*frame && row["frame"]["value"]["body"]["kind"]=="replication" && row["frame"]["value"]["body"]["value"]["kind"]=="copyUnavailable" && row["frame"]["value"]["body"]["value"]["value"]["reason"]["kind"]=="damaged")) && !matches!(&bad_result,Err(SenderError::Send(SendFailure::AnswerLost)));
        observations.check("damaged-copy-typed-answer",typed,json!({"answer":"copyUnavailable","reason":"damaged"}),json!({"sender":format!("{bad_result:?}"),"wire":frames}));
        observations.check("damaged-copy-keeps-identity",prior==after,json!({"unchanged":prior}),json!({"after":after}));
        let healthy_acked=good_result.as_ref().is_ok_and(|events|events.iter().any(|event|matches!(event,ReplicationEvents::Acknowledged {..})));
        observations.check("healthy-copy-same-connection",healthy_acked && session.wire.connections.load(std::sync::atomic::Ordering::SeqCst)==1,json!({"acknowledged":true,"connections":1}),json!({"result":format!("{good_result:?}"),"connections":session.wire.connections.load(std::sync::atomic::Ordering::SeqCst)}));
        let pieces=sent_pieces(&frames);
        pieces.into_iter().find(|piece|piece["database"]==json!(damaged.database())).ok_or_else(||"damaged piece was never sent".into())
    }.await;
    let fault_wire = session.stop().await?;
    second.stop(&mut control).await?;
    println!(
        "{}",
        json!({"case":"fault-wire-complete","wire":fault_wire})
    );
    let original_piece = driven?;
    let after_damaged = read_snapshot(&damaged_path).await?;
    let after_healthy = read_snapshot(&healthy_path).await?;
    observations.check(
        "damaged-copy-no-new-effect",
        after_damaged == faulted,
        json!(faulted),
        json!(after_damaged),
    );
    observations.check(
        "healthy-copy-durable-progress",
        after_healthy.rows.len() == before_healthy.rows.len() + 1
            && after_healthy.receipts.len() == before_healthy.receipts.len() + 1,
        json!({"rows":before_healthy.rows.len()+1,"receipts":before_healthy.receipts.len()+1}),
        json!(after_healthy),
    );
    write_record(&damaged_path, original_marker).await?;
    let repaired = read_snapshot(&damaged_path).await?;
    if repaired.record != before_damaged.record {
        return Err("exact marker repair readback failed".into());
    }
    let third = ServiceProcess::start_as(&service_root, receiver.as_uuid().as_u128()).await?;
    let mut control = third.connect("control", "Bearer consumer-control").await?;
    let session = ObservedSession::start(
        &format!("ws://127.0.0.1:{}/session", third.port),
        peer,
        &root.join("repair-client.db"),
    )
    .await?;
    let resumed: TestResult = async {
        let result = sender(damaged.clone(), receiver, &session)
            .replicate_once(receiver)
            .await?;
        let pieces = sent_pieces(&session.wire.frames.lock().await);
        observations.check(
            "repair-resends-identical-piece",
            pieces.first() == Some(&original_piece),
            original_piece.clone(),
            json!({"pieces":pieces,"result":format!("{result:?}")}),
        );
        let piece: Piece<String> = serde_json::from_value(original_piece.clone())?;
        let duplicate = serde_json::to_value(
            session
                .requests
                .request(OperationRequest::Replication(ReplicationRequest::Piece(
                    piece,
                )))
                .await?,
        )?;
        observations.check(
            "same-piece-already-applied",
            duplicate["kind"] == "replication" && duplicate["value"]["kind"] == "alreadyApplied",
            json!({"kind":"alreadyApplied"}),
            duplicate,
        );
        Ok(())
    }
    .await;
    let repair_wire = session.stop().await?;
    third.stop(&mut control).await?;
    println!(
        "{}",
        json!({"case":"repair-wire-complete","wire":repair_wire})
    );
    resumed?;
    let final_copy = read_snapshot(&damaged_path).await?;
    observations.check(
        "repair-applies-once",
        final_copy.rows.len() == before_damaged.rows.len() + 1
            && final_copy.receipts.len() == before_damaged.receipts.len() + 1,
        json!({"rows":before_damaged.rows.len()+1,"receipts":before_damaged.receipts.len()+1}),
        json!(final_copy),
    );
    answer_loss::observe(
        root,
        &service_root,
        peer,
        receiver,
        damaged,
        &damaged_path,
        observations,
    )
    .await?;
    for writer in writers {
        writer.close().await;
    }
    registry_resume::observe(root, observations).await?;
    for (case, detail) in [
        (
            "registry-copy-call-detail",
            "Claim repair is observed; C6's typed availability reason, journal CallPending/CallDone readback, and move acknowledgement/fence repair controls remain unobserved.",
        ),
        (
            "unknown-store-outcome",
            "C6's store-boundary wrapper must drive committed and rolled-back unknown outcomes with the healthy copy continuing.",
        ),
        (
            "post-commit-wake-failure",
            "C6's failing WakeScheduler must fail after a real committed apply; answer-loss is not a substitute.",
        ),
        (
            "reason-codec-controls",
            "C6's closed reason codec must reject corrupt codes and preserve every reason payload.",
        ),
        (
            "seed-copy-resumption",
            "Seed and snapshot unavailability/resumption must be driven through their C6-owned answer paths.",
        ),
    ] {
        observations.gap(case, detail);
    }
    Ok(())
}
