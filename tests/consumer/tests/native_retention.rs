#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::*;
use fleet_sync_tokio_runner::InstallStatus;
use fleet_sync_transport_websocket::WebSocketConnection;
use fleet_sync_writer::*;
use runtime_primitives::{Clock, SystemClock};
use sqlx_core::executor::Executor;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use support::*;
use tokio::sync::Mutex;

struct Annotation;
impl PieceAnnotation for Annotation {
    type Protocol = SampleProtocol;
    fn annotate(&self, _: &[CapturedTransaction<'_>]) -> Option<String> {
        None
    }
}
struct CopySockets(Mutex<BTreeMap<ReceiverId, WebSocketConnection>>);
impl ReplicationTransport<SampleProtocol> for CopySockets {
    async fn send_piece(
        &self,
        copy: ReceiverId,
        piece: Piece<String>,
    ) -> Result<ReplicationAnswer, SendFailure> {
        let mut sockets = self.0.lock().await;
        let socket = sockets
            .get_mut(&copy)
            .ok_or_else(|| SendFailure::Unavailable {
                message: "copy disconnected".into(),
            })?;
        match request(
            socket,
            OperationRequest::Replication(ReplicationRequest::Piece(piece)),
        )
        .await
        .map_err(|error| SendFailure::Unavailable {
            message: error.to_string(),
        })? {
            OperationAnswer::Replication(answer) => Ok(answer),
            _ => Err(SendFailure::AnswerLost),
        }
    }
}
async fn install(
    control: &mut WebSocketConnection,
    database: DatabaseId,
    source: ReceiverId,
) -> TestResult<ReplicationPosition> {
    let record = match control_request(
        control,
        ControlRequest::InstallSnapshot {
            attempt: durable_steps::Uuid::now_v7(),
            database,
            source,
        },
    )
    .await?
    {
        ControlAnswer::Install(record) => record,
        _ => return Err("install was not admitted".into()),
    };
    match control_request(
        control,
        ControlRequest::WaitInstall {
            install: record.install_id,
        },
    )
    .await?
    {
        ControlAnswer::Install(record) => match record.status {
            InstallStatus::Ready { snapshot } => Ok(snapshot.position),
            status => Err(format!("install did not become ready: {status:?}").into()),
        },
        _ => Err("install did not answer".into()),
    }
}
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(600),
        renew_interval: Duration::from_secs(30),
    }
}
fn options() -> SenderOptions {
    // The later migration spans 13 capture IDs. Keep that whole transaction;
    // the disconnected copy falls 18 IDs behind before its recovery below.
    SenderOptions {
        retention: Retention::Bounded {
            max_age: Duration::from_secs(3600),
            max_changes: 14,
        },
        ..SenderOptions::default()
    }
}

#[tokio::test]
async fn stalled_follower_passes_retention_restarts_from_snapshot_and_follows_the_same_copy()
-> TestResult {
    let root = tempfile::tempdir()?;
    let authority = ServiceProcess::start(&root.path().join("authority")).await?;
    let follower_path = root.path().join("follower");
    let mut follower = ServiceProcess::start_as(&follower_path, 4).await?;
    let receiver = ReceiverId::new(durable_steps::Uuid::from_u128(3))?;
    let follower_id = ReceiverId::new(durable_steps::Uuid::from_u128(4))?;
    let peer = PeerId::new(durable_steps::Uuid::from_u128(2))?;
    let writer_path = root.path().join("writer.db");
    let writer = Arc::new(
        ReplicatedDatabase::create(
            &writer_path,
            SampleSchema::new()?,
            peer,
            policy(),
            WriterOptions::default(),
        )
        .await?,
    );
    let database = writer.database();
    let mut socket = authority
        .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let started = SystemClock.now();
    let lease = match request(
        &mut socket,
        OperationRequest::Lease(LeaseRequest::Register(Registration {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database,
            writer: peer,
            origin: Origin::Empty,
        })),
    )
    .await?
    {
        OperationAnswer::Lease(LeaseAnswer::Register(RegistrationAnswer::Registered {
            lease,
            ..
        })) => lease,
        _ => return Err("authority registration failed".into()),
    };
    writer.observe_registration(lease.clone(), started).await?;
    let mut write = writer.begin_write().await?;
    insert_sample(
        &mut write,
        "019eef00-0000-7000-8000-000000000001".into(),
        "will be deleted".into(),
        9_007_199_254_740_993,
        vec![0, 255, 128],
    )
    .await?;
    write.commit().await?;
    let sockets = Arc::new(CopySockets(Mutex::new(BTreeMap::from([(
        receiver, socket,
    )]))));
    let sending = ReplicationSender::new(
        writer.clone(),
        Arc::new(Annotation),
        Arc::new(()),
        sockets.clone(),
        options(),
    );
    sending
        .add_copy(
            receiver,
            CopyRole::Authoritative,
            ReplicationPosition::new(lease.epoch, CommitSequence::new(0)),
        )
        .await?;
    sending.replicate_once(receiver).await?;
    sending.replicate_once(receiver).await?;
    let endpoint = format!("ws://127.0.0.1:{}/session", authority.port);
    let mut control = follower
        .connect("control", "Bearer consumer-control")
        .await?;
    control_request(
        &mut control,
        ControlRequest::AttachSource {
            receiver,
            endpoint: endpoint.clone(),
            peer: 6,
        },
    )
    .await?;
    let initial = install(&mut control, database, receiver).await?;
    sending
        .add_copy(follower_id, CopyRole::Follower, initial)
        .await?;
    let follower_session = SessionId::new(durable_steps::Uuid::now_v7())?;
    sockets
        .0
        .lock()
        .await
        .insert(follower_id, follower.peer(2, follower_session).await?);
    // An actual closed follower socket leaves its durable position behind.
    sockets
        .0
        .lock()
        .await
        .remove(&follower_id)
        .ok_or("follower socket missing")?
        .close(CloseReason::ClientClosed)
        .await;
    let mut write = writer.begin_write().await?;
    (&mut write)
        .execute("DELETE FROM samples WHERE id='019eef00-0000-7000-8000-000000000001'")
        .await?;
    write.commit().await?;
    sending.replicate_once(receiver).await?;
    assert!(matches!(
        sending.replicate_once(follower_id).await,
        Err(SenderError::Send(SendFailure::Unavailable { .. }))
    ));
    for index in 0..8 {
        let mut write = writer.begin_write().await?;
        insert_sample(
            &mut write,
            format!("019eef00-0000-7000-8000-{:012x}", index + 10),
            format!("kept-{index}"),
            9_007_199_254_740_993,
            vec![0, 255, 128],
        )
        .await?;
        write.commit().await?;
        sending.replicate_once(receiver).await?;
    }
    let copies = sending.copies().await?;
    let stalled = copies
        .iter()
        .find(|copy| copy.copy == follower_id)
        .ok_or("follower bookkeeping missing")?;
    assert!(stalled.needs_snapshot);
    assert_eq!(stalled.acked, initial);
    assert!(
        copies
            .iter()
            .any(|copy| copy.copy == receiver && copy.acked > initial && !copy.needs_snapshot)
    );
    assert!(sending.replicate_once(follower_id).await?.iter().any(
        |event| matches!(event, ReplicationEvents::CopyNeedsSnapshot { copy } if *copy==follower_id)
    ));
    let mut old_reader = follower
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    assert_eq!(
        copy_rows(&mut old_reader, database)
            .await?
            .get("samples")
            .map(Vec::len),
        Some(1)
    );
    follower.kill().await?;
    drop(control);
    follower = ServiceProcess::start_as(&follower_path, 4).await?;
    control = follower
        .connect("control", "Bearer consumer-control")
        .await?;
    control_request(
        &mut control,
        ControlRequest::AttachSource {
            receiver,
            endpoint,
            peer: 6,
        },
    )
    .await?;
    let recovered = install(&mut control, database, receiver).await?;
    assert!(recovered > initial);
    assert!(recovered.sequence.value() - initial.sequence.value() > 14);
    let resumed = sending.complete_snapshot(follower_id, recovered).await?;
    assert_eq!(resumed.start, initial);
    assert_eq!(resumed.acked, recovered);
    assert!(!resumed.needs_snapshot);
    sockets
        .0
        .lock()
        .await
        .insert(follower_id, follower.peer(2, follower_session).await?);
    let mut reader = follower
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let rows = copy_rows(&mut reader, database).await?;
    assert_eq!(rows.get("samples").map(Vec::len), Some(8));
    assert!(rows.get("samples").ok_or("samples missing")?.iter().all(|row| matches!(row.first(),Some(SqlValue::Text(id)) if id!="019eef00-0000-7000-8000-000000000001")));
    writer.close().await;
    drop(sending);
    drop(writer);
    let writer = Arc::new(
        ReplicatedDatabase::open_with(
            &writer_path,
            database,
            SampleSchema::extended()?,
            LeaseHolder::provisional(database, peer, policy()),
            Arc::new(SystemClock),
            WriterOptions::default(),
        )
        .await?,
    );
    let sending = ReplicationSender::new(
        writer.clone(),
        Arc::new(Annotation),
        Arc::new(()),
        sockets.clone(),
        options(),
    );
    assert!(
        writer.snapshot().await?.position().sequence.value() - recovered.sequence.value() <= 14
    );
    println!(
        "OBS retention recovered={recovered:?}, migration_head={:?}, copies={:?}",
        writer.snapshot().await?.position(),
        sending.copies().await?
    );
    for copy in [receiver, follower_id] {
        println!(
            "OBS retention migration copy={copy:?}, events={:?}",
            sending.replicate_once(copy).await?
        );
    }
    let mut write = writer.begin_write().await?;
    insert_sample(
        &mut write,
        "019eef00-0000-7000-8000-000000000020".into(),
        "after snapshot".into(),
        9_007_199_254_740_993,
        vec![0, 255, 128],
    )
    .await?;
    write.commit().await?;
    for copy in [receiver, follower_id] {
        println!(
            "OBS retention later copy={copy:?}, events={:?}",
            sending.replicate_once(copy).await?
        );
    }
    let mut authority_reader = authority
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let expected = copy_rows(&mut authority_reader, database).await?;
    let actual = copy_rows(&mut reader, database).await?;
    assert_eq!(actual, expected);
    let samples = actual.get("samples").ok_or("samples missing")?;
    assert_eq!(samples.len(), 9);
    assert_eq!(
        samples
            .iter()
            .filter(
                |row| matches!(row.last(),Some(SqlValue::Text(note)) if note=="after bootstrap")
            )
            .count(),
        8
    );
    assert_eq!(
        samples
            .iter()
            .filter(|row| matches!(row.last(), Some(SqlValue::Null)))
            .count(),
        1
    );
    assert_eq!(
        sending
            .copies()
            .await?
            .iter()
            .find(|copy| copy.copy == follower_id)
            .ok_or("follower disappeared")?
            .start,
        initial
    );
    let mut authority_control = authority
        .connect("control", "Bearer consumer-control")
        .await?;
    writer.close().await;
    follower.stop(&mut control).await?;
    authority.stop(&mut authority_control).await?;
    Ok(())
}
