mod support;
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::*;
use fleet_sync_transport_websocket::WebSocketConnection;
use fleet_sync_writer::*;
use runtime_primitives::{Clock, SystemClock};
use std::{sync::Arc, time::Duration};
use support::*;
use tokio::sync::Mutex;

struct Annotation;
impl PieceAnnotation for Annotation {
    type Protocol = SampleProtocol;
    fn annotate(&self, _: &[CapturedTransaction<'_>]) -> Option<String> {
        Some("captured".into())
    }
}
struct SocketTransport {
    socket: Mutex<WebSocketConnection>,
    last: Mutex<Option<Piece<String>>>,
}
impl ReplicationTransport<SampleProtocol> for SocketTransport {
    async fn send_piece(
        &self,
        _: ReceiverId,
        piece: Piece<String>,
    ) -> Result<ReplicationAnswer, SendFailure> {
        *self.last.lock().await = Some(piece.clone());
        let result = request(
            &mut *self.socket.lock().await,
            OperationRequest::Replication(ReplicationRequest::Piece(piece)),
        )
        .await
        .map_err(|error| SendFailure::Unavailable {
            message: error.to_string(),
        })?;
        match result {
            OperationAnswer::Replication(answer) => Ok(answer),
            _ => Err(SendFailure::AnswerLost),
        }
    }
}
fn peer() -> TestResult<PeerId> {
    Ok(PeerId::new(durable_steps::Uuid::from_u128(2))?)
}
fn receiver() -> TestResult<ReceiverId> {
    Ok(ReceiverId::new(durable_steps::Uuid::from_u128(3))?)
}
async fn register(
    socket: &mut WebSocketConnection,
    database: DatabaseId,
) -> TestResult<LeaseStatus> {
    match request(
        socket,
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
        })) => Ok(lease),
        _ => Err("registration did not complete".into()),
    }
}

#[tokio::test]
async fn native_writer_to_service_preserves_cells_hook_rollback_and_follow_up_across_process_kill()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let service_path = directory.path().join("server");
    let mut process = ServiceProcess::start(&service_path).await?;
    let session = SessionId::new(durable_steps::Uuid::now_v7())?;
    let mut socket = process.peer(2, session).await?;
    let mut control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    let writer = Arc::new(
        ReplicatedDatabase::create(
            &directory.path().join("writer.db"),
            SampleSchema::new()?,
            peer()?,
            LeasePolicy {
                lease_duration: Duration::from_secs(600),
                renew_interval: Duration::from_secs(30),
            },
            WriterOptions::default(),
        )
        .await?,
    );
    let lease = register(&mut socket, writer.database()).await?;
    let started = SystemClock.now();
    let renewal = match request(
        &mut socket,
        OperationRequest::Lease(LeaseRequest::Renew(RenewRequest {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database: writer.database(),
            epoch: lease.epoch,
        })),
    )
    .await?
    {
        OperationAnswer::Lease(LeaseAnswer::Renew(answer)) => answer,
        _ => return Err("no renewal".into()),
    };
    writer.observe_renewal(renewal, started).await?;
    let transport = Arc::new(SocketTransport {
        socket: Mutex::new(socket),
        last: Mutex::new(None),
    });
    let sender = ReplicationSender::new(
        writer.clone(),
        Arc::new(Annotation),
        Arc::new(()),
        transport.clone(),
        SenderOptions {
            max_part_items: std::num::NonZeroUsize::MIN.saturating_add(1),
            ..SenderOptions::default()
        },
    );
    sender
        .add_copy(
            receiver()?,
            CopyRole::Authoritative,
            ReplicationPosition::new(lease.epoch, CommitSequence::new(0)),
        )
        .await?;
    sender.replicate_once(receiver()?).await?;
    assert_eq!(effects(&mut control, 1).await?.len(), 1);
    assert_key_refusals(
        &writer,
        &mut *transport.socket.lock().await,
        directory.path(),
        peer()?,
    )
    .await?;

    let values = [
        f64::from_bits(1),
        f64::MIN_POSITIVE,
        f64::MAX,
        -f64::MAX,
        f64::from_bits(3485510186621062260),
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];
    let mut write = writer.begin_write().await?;
    insert_sample(
        &mut write,
        "019eef00-0000-7000-8000-000000000001".into(),
        "measurement α".into(),
        9_007_199_254_740_993,
        vec![0, 255, 128],
    )
    .await?;
    insert_source(
        &mut write,
        "019eef00-0000-7000-8000-000000000002".into(),
        "019eef00-0000-7000-8000-000000000005".into(),
        "019eef00-0000-7000-8000-000000000001".into(),
    )
    .await?;
    for (index, value) in values.into_iter().enumerate() {
        insert_reading(
            &mut write,
            format!("019eef00-0000-7000-8001-{index:012x}"),
            value,
        )
        .await?;
    }
    write.commit().await?;
    sender.replicate_once(receiver()?).await?;
    let receipts = effects(&mut control, 2).await?;
    assert_eq!(receipts.len(), 2);
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.saved.effect.event_count)
            .max(),
        Some(2)
    );
    let mut socket = transport.socket.lock().await;
    let copied = copy_rows(&mut socket, writer.database()).await?;
    let readings = copied.get("readings").ok_or("no readings")?;
    assert_eq!(readings.len(), values.len());
    for (row, value) in readings.iter().zip(values) {
        assert!(
            matches!(row.as_slice(),[SqlValue::Text(id),SqlValue::Real(actual),SqlValue::Null] if canonical_uuid_v7(id) && actual.to_bits()==value.to_bits()),
            "REAL storage class or bits changed: {row:?}"
        );
    }
    assert!(
        matches!(copied.get("samples").map(Vec::as_slice),Some([row]) if matches!(row.as_slice(),[SqlValue::Text(id),SqlValue::Text(label),SqlValue::Integer(9_007_199_254_740_993),SqlValue::Blob(blob)] if id=="019eef00-0000-7000-8000-000000000001" && label=="measurement α" && blob==&[0,255,128]))
    );
    assert!(
        matches!(copied.get("samples_by_source").map(Vec::as_slice),Some([row]) if matches!(row.as_slice(),[SqlValue::Text(source),SqlValue::Text(event),SqlValue::Text(id)] if source=="019eef00-0000-7000-8000-000000000002" && event=="019eef00-0000-7000-8000-000000000005" && id=="019eef00-0000-7000-8000-000000000001"))
    );
    assert_eq!(copied.len(), 3, "hook local table leaked into snapshot");
    drop(socket);
    println!(
        "OBS real writer -> localhost service: single/composite UUIDv7 keys, binary/text/NULL, signed exact integers and seven REAL bit/storage-class cases preserved; split commits publish once"
    );

    let before = sender.copies().await?.first().ok_or("no copy")?.acked;
    control_request(
        &mut control,
        ControlRequest::SetHookFailure { enabled: true },
    )
    .await?;
    let mut write = writer.begin_write().await?;
    insert_sample(
        &mut write,
        "019eef00-0000-7000-8000-000000000003".into(),
        "retry".into(),
        42,
        vec![],
    )
    .await?;
    write.commit().await?;
    assert!(matches!(
        sender.replicate_once(receiver()?).await,
        Err(SenderError::Refused {
            reason: PieceRefusal::Hook(_),
            ..
        })
    ));
    assert!(
        matches!(control_request(&mut control,ControlRequest::CopyStatus{database:writer.database()}).await?,ControlAnswer::CopyStatus(CopyStatusAnswer::Status(status)) if status.acked==before)
    );
    let rolled_back = copy_rows(&mut *transport.socket.lock().await, writer.database()).await?;
    assert_eq!(rolled_back, copied, "hook refusal leaked replicated rows");
    control_request(
        &mut control,
        ControlRequest::SetHookFailure { enabled: false },
    )
    .await?;
    sender.replicate_once(receiver()?).await?;
    let receipts = effects(&mut control, 3).await?;
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.saved.effect.event_count)
            .max(),
        Some(3),
        "refused local hook row survived rollback"
    );
    println!(
        "OBS hook refusal rolls back rows, watermark and local hook write; same captured transaction succeeds when policy allows"
    );

    control_request(&mut control, ControlRequest::LoseNextFollowUpAnswer).await?;
    let mut write = writer.begin_write().await?;
    insert_sample(
        &mut write,
        "019eef00-0000-7000-8000-000000000004".into(),
        "kill".into(),
        43,
        vec![1],
    )
    .await?;
    write.commit().await?;
    sender.replicate_once(receiver()?).await?;
    let receipts = effects(&mut control, 4).await?;
    let lost = receipts
        .into_iter()
        .find(|receipt| receipt.saved.effect.event_count == 4)
        .ok_or("lost-answer effect absent")?;
    assert_eq!(lost.attempts, 1);
    let last = transport.last.lock().await.clone().ok_or("no last piece")?;
    process.kill().await?;
    drop(control);
    process = ServiceProcess::start(&service_path).await?;
    control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut resumed = process.peer(2, session).await?;
    assert!(matches!(
        request(
            &mut resumed,
            OperationRequest::Replication(ReplicationRequest::Piece(last))
        )
        .await?,
        OperationAnswer::Replication(ReplicationAnswer::AlreadyApplied { .. })
    ));
    match control_request(
        &mut control,
        ControlRequest::WaitAttempts {
            call: lost.saved.call.clone(),
            count: 2,
        },
    )
    .await?
    {
        ControlAnswer::Effect(receipt) => assert_eq!(receipt.saved, lost.saved),
        _ => return Err("no original-CallId retry".into()),
    }
    assert_eq!(
        effects(&mut control, 4).await?.len(),
        4,
        "restart duplicated an external effect"
    );
    let rows = copy_rows(&mut resumed, writer.database()).await?;
    assert_eq!(rows.get("samples").map(Vec::len), Some(3));
    println!(
        "OBS server killed after external commit before follow-up answer: restart retries original CallId, keeps one effect and one hook application; exact final piece replay is AlreadyApplied"
    );
    resumed.close(CloseReason::ClientClosed).await;
    writer.close().await;
    process.stop(&mut control).await
}

#[tokio::test]
async fn opening_revocation_blocks_late_hello_and_nonholder_cannot_start_seed() -> TestResult {
    let directory = tempfile::tempdir()?;
    let process = ServiceProcess::start(directory.path()).await?;
    let mut control = process
        .connect("control", "Bearer consumer-control")
        .await?;
    let session = SessionId::new(durable_steps::Uuid::now_v7())?;
    let mut owner = process.peer(2, session).await?;
    let database = DatabaseId::new(durable_steps::Uuid::now_v7())?;
    match request(
        &mut owner,
        OperationRequest::Lease(LeaseRequest::Register(Registration {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database,
            writer: peer()?,
            origin: Origin::Seeded {
                position: ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(0)),
            },
        })),
    )
    .await?
    {
        OperationAnswer::Lease(LeaseAnswer::Register(RegistrationAnswer::Registered {
            ..
        })) => {}
        _ => return Err("seeded registration failed".into()),
    }
    let mut stranger = process
        .peer(4, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    assert!(matches!(
        request(
            &mut stranger,
            OperationRequest::Replication(ReplicationRequest::SeedBegin {
                database,
                attempt: durable_steps::Uuid::now_v7()
            })
        )
        .await?,
        OperationAnswer::Replication(ReplicationAnswer::Refused(PieceRefusal::NotWriter { .. }))
    ));
    assert!(matches!(
        request(
            &mut owner,
            OperationRequest::Replication(ReplicationRequest::CopyStatus { database })
        )
        .await?,
        OperationAnswer::Replication(ReplicationAnswer::CopyStatus(
            CopyStatusAnswer::NotInitialized
        ))
    ));
    let owned_attempt = durable_steps::Uuid::now_v7();
    assert!(
        matches!(request(&mut owner,OperationRequest::Replication(ReplicationRequest::SeedBegin { database, attempt: owned_attempt })).await?,OperationAnswer::Replication(ReplicationAnswer::SeedStarted { seed }) if seed.generation == 1),
        "unauthorized begin changed the seed generation"
    );
    assert!(
        matches!(
            request(
                &mut stranger,
                OperationRequest::Replication(ReplicationRequest::SeedBegin {
                    database,
                    attempt: owned_attempt
                })
            )
            .await?,
            OperationAnswer::Replication(ReplicationAnswer::Refused(
                PieceRefusal::NotWriter { .. }
            ))
        ),
        "receipt replay bypassed seed authority"
    );
    println!(
        "OBS nonholder SeedBegin refused before mutation and receipt replay; legitimate holder receives first seed generation"
    );

    let opening_peer = PeerId::new(durable_steps::Uuid::from_u128(5))?;
    let mut opening = process.connect("session", "Bearer peer-5").await?;
    assert!(matches!(
        control_request(
            &mut control,
            ControlRequest::WaitOpening { peer: opening_peer }
        )
        .await?,
        ControlAnswer::Opening { .. }
    ));
    control_request(&mut control, ControlRequest::Revoke { peer: opening_peer }).await?;
    let late: ClientFrame<SampleProtocol> = ClientFrame::Hello {
        session_id: SessionId::new(durable_steps::Uuid::now_v7())?,
        schema_support: SchemaSupport { max_version: 100 },
    };
    let _ = opening.send(serde_json::to_vec(&late)?.into()).await;
    let late_request: ClientFrame<SampleProtocol> = ClientFrame::Request {
        frame: FrameId::new(durable_steps::Uuid::now_v7())?,
        body: OperationRequest::Consumer(SampleRequest::Echo {
            text: "late".into(),
        }),
    };
    let _ = opening
        .send(serde_json::to_vec(&late_request)?.into())
        .await;
    if let Some(Ok(bytes)) = tokio::time::timeout(Duration::from_secs(5), opening.receive()).await?
    {
        assert!(matches!(
            serde_json::from_slice::<ServerFrame<SampleProtocol>>(&bytes)?,
            ServerFrame::Closed {
                reason: CloseReason::CredentialRejected(AuthRefusal::Revoked)
            }
        ));
    }
    assert!(process.connect("session", "Bearer peer-5").await.is_err());
    assert!(
        matches!(
            control_request(&mut control, ControlRequest::RequestCount).await?,
            ControlAnswer::RequestCount(0)
        ),
        "late request reached the consumer after revocation"
    );
    println!(
        "OBS committed Opening observed through test-support owner watch -> revoke -> late Hello refused, future authentication refused"
    );
    owner.close(CloseReason::ClientClosed).await;
    stranger.close(CloseReason::ClientClosed).await;
    process.stop(&mut control).await
}
