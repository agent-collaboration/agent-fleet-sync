#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::*;
use fleet_sync_transport_websocket::WebSocketConnection;
use fleet_sync_writer::*;
use runtime_primitives::{Clock, SystemClock};
use sqlx_core::executor::Executor;
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};
use support::*;
use tokio::sync::Mutex;

struct Annotation;
impl PieceAnnotation for Annotation {
    type Protocol = SampleProtocol;
    fn annotate(&self, _: &[CapturedTransaction<'_>]) -> Option<String> {
        Some("capture restart".into())
    }
}
struct SocketTransport {
    socket: Mutex<Option<WebSocketConnection>>,
    close_after: AtomicU8,
    last: Mutex<Option<Piece<String>>>,
}
impl SocketTransport {
    fn new(socket: WebSocketConnection, close_after: u8) -> Self {
        Self {
            socket: Mutex::new(Some(socket)),
            close_after: AtomicU8::new(close_after),
            last: Mutex::new(None),
        }
    }
}
impl ReplicationTransport<SampleProtocol> for SocketTransport {
    async fn send_piece(
        &self,
        _: ReceiverId,
        piece: Piece<String>,
    ) -> Result<ReplicationAnswer, SendFailure> {
        *self.last.lock().await = Some(piece.clone());
        let mut owner = self.socket.lock().await;
        let socket = owner.as_mut().ok_or(SendFailure::AnswerLost)?;
        let answer = match request(
            socket,
            OperationRequest::Replication(ReplicationRequest::Piece(piece)),
        )
        .await
        .map_err(|error| SendFailure::Unavailable {
            message: error.to_string(),
        })? {
            OperationAnswer::Replication(answer) => answer,
            _ => return Err(SendFailure::AnswerLost),
        };
        let should_close = match self.close_after.load(Ordering::SeqCst) {
            1 => matches!(answer, ReplicationAnswer::PartStaged { .. }),
            2 => matches!(answer, ReplicationAnswer::Applied { .. }),
            _ => false,
        };
        if should_close {
            self.close_after.store(0, Ordering::SeqCst);
            // The actual receiver committed its stage or final part. Close the
            // real socket before this answer can reach ReplicationSender.
            if let Some(socket) = owner.take() {
                socket.close(CloseReason::ClientClosed).await;
            }
            return Err(SendFailure::AnswerLost);
        }
        Ok(answer)
    }
}
fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(600),
        renew_interval: Duration::from_secs(30),
    }
}
fn peer() -> TestResult<PeerId> {
    Ok(PeerId::new(durable_steps::Uuid::from_u128(2))?)
}
fn receiver() -> TestResult<ReceiverId> {
    Ok(ReceiverId::new(durable_steps::Uuid::from_u128(3))?)
}
type Sender = ReplicationSender<SampleSchema, Annotation, (), SocketTransport>;
fn sender(
    writer: Arc<ReplicatedDatabase<SampleSchema>>,
    transport: Arc<SocketTransport>,
) -> Sender {
    ReplicationSender::new(
        writer,
        Arc::new(Annotation),
        Arc::new(()),
        transport,
        SenderOptions {
            max_part_items: std::num::NonZeroUsize::MIN,
            ..SenderOptions::default()
        },
    )
}
async fn reopen(
    path: &Path,
    database: DatabaseId,
) -> TestResult<Arc<ReplicatedDatabase<SampleSchema>>> {
    Ok(Arc::new(
        ReplicatedDatabase::open_with(
            path,
            database,
            SampleSchema::new()?,
            LeaseHolder::provisional(database, peer()?, policy()),
            Arc::new(SystemClock),
            WriterOptions::default(),
        )
        .await?,
    ))
}

#[tokio::test]
async fn split_capture_survives_receiver_kill_and_lost_final_answer_then_updates_and_deletes()
-> TestResult {
    let root = tempfile::tempdir()?;
    let service_path = root.path().join("service");
    let writer_path = root.path().join("writer.db");
    let mut service = ServiceProcess::start(&service_path).await?;
    let session = SessionId::new(durable_steps::Uuid::now_v7())?;
    let mut socket = service.peer(2, session).await?;
    let writer = Arc::new(
        ReplicatedDatabase::create(
            &writer_path,
            SampleSchema::new()?,
            peer()?,
            policy(),
            WriterOptions::default(),
        )
        .await?,
    );
    let database = writer.database();
    let started = SystemClock.now();
    let lease = match request(
        &mut socket,
        OperationRequest::Lease(LeaseRequest::Register(Registration {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database,
            writer: peer()?,
            origin: Origin::Empty,
        })),
    )
    .await?
    {
        OperationAnswer::Lease(LeaseAnswer::Register(RegistrationAnswer::Registered {
            lease,
            ..
        })) => lease,
        _ => return Err("registration failed".into()),
    };
    writer.observe_registration(lease.clone(), started).await?;
    let transport = Arc::new(SocketTransport::new(socket, 0));
    let sending = sender(writer.clone(), transport.clone());
    sending
        .add_copy(
            receiver()?,
            CopyRole::Authoritative,
            ReplicationPosition::new(lease.epoch, CommitSequence::new(0)),
        )
        .await?;
    sending.replicate_once(receiver()?).await?;
    let baseline = sending.copies().await?.first().ok_or("copy missing")?.acked;
    let mut write = writer.begin_write().await?;
    insert_sample(
        &mut write,
        "019eef00-0000-7000-8000-000000000001".into(),
        "before update".into(),
        9_007_199_254_740_993,
        vec![0, 255, 128],
    )
    .await?;
    insert_source(
        &mut write,
        "019eef00-0000-7000-8000-000000000002".into(),
        "019eef00-0000-7000-8000-000000000003".into(),
        "019eef00-0000-7000-8000-000000000001".into(),
    )
    .await?;
    insert_reading(
        &mut write,
        "019eef00-0000-7000-8000-000000000004".into(),
        f64::from_bits(1),
    )
    .await?;
    write.commit().await?;
    let through = writer.snapshot().await?.position();
    transport.close_after.store(1, Ordering::SeqCst);
    assert!(matches!(
        sending.replicate_once(receiver()?).await,
        Err(SenderError::Send(SendFailure::AnswerLost))
    ));
    let staged = transport
        .last
        .lock()
        .await
        .clone()
        .ok_or("staged piece missing")?;
    assert!(
        staged
            .part
            .as_ref()
            .is_some_and(|part| part.index == 0 && part.count > 1)
    );
    assert_eq!(
        sending.copies().await?.first().ok_or("copy missing")?.acked,
        baseline
    );
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    assert!(
        copy_rows(&mut reader, database)
            .await?
            .values()
            .all(Vec::is_empty)
    );
    service.kill().await?;
    writer.close().await;
    drop(sending);
    drop(transport);
    drop(writer);
    service = ServiceProcess::start(&service_path).await?;
    let writer = reopen(&writer_path, database).await?;
    let transport = Arc::new(SocketTransport::new(service.peer(2, session).await?, 2));
    let sending = sender(writer.clone(), transport.clone());
    assert!(matches!(
        sending.replicate_once(receiver()?).await,
        Err(SenderError::Send(SendFailure::AnswerLost))
    ));
    let final_piece = transport
        .last
        .lock()
        .await
        .clone()
        .ok_or("final piece missing")?;
    assert!(
        final_piece
            .part
            .as_ref()
            .is_some_and(|part| part.index + 1 == part.count)
    );
    assert_eq!(
        sending.copies().await?.first().ok_or("copy missing")?.acked,
        baseline
    );
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let before = copy_rows(&mut reader, database).await?;
    assert_eq!(before.get("samples").map(Vec::len), Some(1));
    assert_eq!(before.get("samples_by_source").map(Vec::len), Some(1));
    assert!(
        matches!(before.get("readings").map(Vec::as_slice), Some([row]) if matches!(row.as_slice(),[SqlValue::Text(_),SqlValue::Real(value),SqlValue::Null] if value.to_bits()==1))
    );
    service.kill().await?;
    writer.close().await;
    drop(sending);
    drop(transport);
    drop(writer);
    service = ServiceProcess::start(&service_path).await?;
    let writer = reopen(&writer_path, database).await?;
    let mut socket = service.peer(2, session).await?;
    assert!(
        matches!(request(&mut socket, OperationRequest::Replication(ReplicationRequest::Piece(final_piece))).await?, OperationAnswer::Replication(ReplicationAnswer::AlreadyApplied { through: actual }) if actual==through)
    );
    let transport = Arc::new(SocketTransport::new(socket, 0));
    let sending = sender(writer.clone(), transport.clone());
    sending.replicate_once(receiver()?).await?;
    assert_eq!(
        sending.copies().await?.first().ok_or("copy missing")?.acked,
        through
    );
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    assert_eq!(copy_rows(&mut reader, database).await?, before);
    let mut write = writer.begin_write().await?;
    (&mut write).execute("UPDATE samples SET label='after update',exact_value=-9007199254740993,payload=x'ff00' WHERE id='019eef00-0000-7000-8000-000000000001'; UPDATE samples_by_source SET sample_id=NULL; DELETE FROM readings").await?;
    write.commit().await?;
    sending.replicate_once(receiver()?).await?;
    let updated = copy_rows(&mut reader, database).await?;
    assert!(
        matches!(updated.get("samples").map(Vec::as_slice), Some([row]) if matches!(row.as_slice(), [SqlValue::Text(_),SqlValue::Text(label),SqlValue::Integer(-9_007_199_254_740_993),SqlValue::Blob(bytes)] if label=="after update" && bytes==&[255,0]))
    );
    assert!(
        matches!(updated.get("samples_by_source").map(Vec::as_slice), Some([row]) if matches!(row.as_slice(), [SqlValue::Text(_),SqlValue::Text(_),SqlValue::Null]))
    );
    assert!(updated.get("readings").is_some_and(Vec::is_empty));
    let mut write = writer.begin_write().await?;
    (&mut write)
        .execute("DELETE FROM samples_by_source; DELETE FROM samples")
        .await?;
    write.commit().await?;
    sending.replicate_once(receiver()?).await?;
    assert!(
        copy_rows(&mut reader, database)
            .await?
            .values()
            .all(Vec::is_empty)
    );
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    assert_eq!(effects(&mut control, 4).await?.len(), 4);
    writer.close().await;
    service.stop(&mut control).await?;
    Ok(())
}

#[tokio::test]
async fn receiver_kill_after_copy_commit_before_delivery_retries_the_admitted_follow_up_once()
-> TestResult {
    let root = tempfile::tempdir()?;
    let service_path = root.path().join("service");
    let mut service = ServiceProcess::start(&service_path).await?;
    let session = SessionId::new(durable_steps::Uuid::now_v7())?;
    let mut socket = service.peer(2, session).await?;
    let writer = Arc::new(
        ReplicatedDatabase::create(
            &root.path().join("writer.db"),
            SampleSchema::new()?,
            peer()?,
            policy(),
            WriterOptions::default(),
        )
        .await?,
    );
    let database = writer.database();
    let started = SystemClock.now();
    let lease = match request(
        &mut socket,
        OperationRequest::Lease(LeaseRequest::Register(Registration {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database,
            writer: peer()?,
            origin: Origin::Empty,
        })),
    )
    .await?
    {
        OperationAnswer::Lease(LeaseAnswer::Register(RegistrationAnswer::Registered {
            lease,
            ..
        })) => lease,
        _ => return Err("registration failed".into()),
    };
    writer.observe_registration(lease.clone(), started).await?;
    let transport = Arc::new(SocketTransport::new(socket, 0));
    let sending = sender(writer.clone(), transport.clone());
    sending
        .add_copy(
            receiver()?,
            CopyRole::Authoritative,
            ReplicationPosition::new(lease.epoch, CommitSequence::new(0)),
        )
        .await?;
    sending.replicate_once(receiver()?).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    effects(&mut control, 1).await?;
    control_request(&mut control, ControlRequest::HoldNextFollowUpBeforeEffect).await?;
    let mut write = writer.begin_write().await?;
    insert_sample(
        &mut write,
        "019eef00-0000-7000-8000-000000000001".into(),
        "committed before delivery".into(),
        9_007_199_254_740_993,
        vec![0, 255, 128],
    )
    .await?;
    write.commit().await?;
    sending.replicate_once(receiver()?).await?;
    let call = match control_request(&mut control, ControlRequest::WaitFollowUpBeforeEffect).await?
    {
        ControlAnswer::FollowUpCall(call) => call,
        _ => return Err("before-delivery call missing".into()),
    };
    assert!(
        matches!(control_request(&mut control, ControlRequest::Effects).await?, ControlAnswer::Effects(receipts) if receipts.len()==1)
    );
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    assert_eq!(
        copy_rows(&mut reader, database)
            .await?
            .get("samples")
            .map(Vec::len),
        Some(1)
    );
    let last = transport
        .last
        .lock()
        .await
        .clone()
        .ok_or("committed piece missing")?;
    service.kill().await?;
    writer.close().await;
    drop(control);
    service = ServiceProcess::start(&service_path).await?;
    control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut resumed = service.peer(2, session).await?;
    assert!(matches!(
        request(
            &mut resumed,
            OperationRequest::Replication(ReplicationRequest::Piece(last))
        )
        .await?,
        OperationAnswer::Replication(ReplicationAnswer::AlreadyApplied { .. })
    ));
    let receipt = match control_request(
        &mut control,
        ControlRequest::WaitAttempts {
            call: call.clone(),
            count: 1,
        },
    )
    .await?
    {
        ControlAnswer::Effect(receipt) => receipt,
        _ => return Err("admitted follow-up did not resume".into()),
    };
    assert_eq!(receipt.saved.call, call);
    assert_eq!(receipt.saved.effect.database, database);
    assert_eq!(receipt.saved.effect.event_count, 2);
    assert_eq!(receipt.attempts, 1);
    assert_eq!(effects(&mut control, 2).await?.len(), 2);
    assert_eq!(
        copy_rows(&mut resumed, database)
            .await?
            .get("samples")
            .map(Vec::len),
        Some(1)
    );
    service.stop(&mut control).await?;
    Ok(())
}
