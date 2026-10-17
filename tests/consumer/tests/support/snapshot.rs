use super::support::{ServiceProcess, copy_rows, request};
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::*;
use fleet_sync_transport_websocket::WebSocketConnection;
use fleet_sync_writer::*;
use runtime_primitives::{Clock, SystemClock};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

struct Annotation;
impl PieceAnnotation for Annotation {
    type Protocol = SampleProtocol;
    fn annotate(&self, _: &[CapturedTransaction<'_>]) -> Option<String> {
        None
    }
}
struct SourceTransport(Mutex<WebSocketConnection>);
impl ReplicationTransport<SampleProtocol> for SourceTransport {
    async fn send_piece(
        &self,
        _: ReceiverId,
        piece: Piece<String>,
    ) -> Result<ReplicationAnswer, SendFailure> {
        match request(
            &mut *self.0.lock().await,
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
pub async fn prepare_source(
    process: &ServiceProcess,
    root: &std::path::Path,
) -> TestResult<(DatabaseId, BTreeMap<String, Vec<Vec<SqlValue>>>)> {
    let peer = PeerId::new(durable_steps::Uuid::from_u128(2))?;
    let receiver = ReceiverId::new(durable_steps::Uuid::from_u128(3))?;
    let writer = Arc::new(
        ReplicatedDatabase::create(
            &root.join("source-writer.db"),
            SampleSchema::new()?,
            peer,
            LeasePolicy {
                lease_duration: Duration::from_secs(600),
                renew_interval: Duration::from_secs(30),
            },
            WriterOptions::default(),
        )
        .await?,
    );
    let database = writer.database();
    let mut socket = process
        .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
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
        _ => return Err("source registration did not complete".into()),
    };
    let started = SystemClock.now();
    let renewed = match request(
        &mut socket,
        OperationRequest::Lease(LeaseRequest::Renew(RenewRequest {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database,
            epoch: lease.epoch,
        })),
    )
    .await?
    {
        OperationAnswer::Lease(LeaseAnswer::Renew(answer)) => answer,
        _ => return Err("source renewal did not complete".into()),
    };
    writer.observe_renewal(renewed, started).await?;
    let mut write = writer.begin_write().await?;
    for index in 0..300 {
        insert_sample(
            &mut write,
            format!("019eef00-0000-7000-8000-{index:012x}"),
            format!("sample-{index}"),
            9_007_199_254_740_993,
            vec![0, 255, 128],
        )
        .await?;
    }
    insert_reading(
        &mut write,
        "019eef00-0000-7000-8001-000000000001".into(),
        3.25,
    )
    .await?;
    write.commit().await?;
    let transport = Arc::new(SourceTransport(Mutex::new(socket)));
    let sender = ReplicationSender::new(
        writer,
        Arc::new(Annotation),
        Arc::new(()),
        transport.clone(),
        SenderOptions::default(),
    );
    sender
        .add_copy(
            receiver,
            CopyRole::Authoritative,
            ReplicationPosition::new(lease.epoch, CommitSequence::new(0)),
        )
        .await?;
    sender.replicate_once(receiver).await?;
    // The initial migration and the data write are two captured commits;
    // replicate_once owns one commit at a time.
    sender.replicate_once(receiver).await?;
    let expected = copy_rows(&mut *transport.0.lock().await, database).await?;
    assert_eq!(expected.get("samples").map(Vec::len), Some(300));
    Ok((database, expected))
}
