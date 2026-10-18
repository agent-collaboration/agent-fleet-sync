#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_lease::{CancelAnswer, CancelHolderChange, CancelReason, LeasePolicy};
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::*;
use fleet_sync_transport_websocket::WebSocketConnection;
use fleet_sync_writer::*;
use runtime_primitives::{Clock, SystemClock, TokioWakeTimer, WakeScheduler};
use sqlx_core::{query::query, row::Row};
use std::{path::Path, sync::Arc, time::Duration};
use support::*;
use tokio::sync::Mutex;

struct Annotation;
impl PieceAnnotation for Annotation {
    type Protocol = SampleProtocol;
    fn annotate(&self, _: &[CapturedTransaction<'_>]) -> Option<String> {
        None
    }
}
struct SocketTransport(Mutex<Option<WebSocketConnection>>);
impl ReplicationTransport<SampleProtocol> for SocketTransport {
    async fn send_piece(
        &self,
        _: ReceiverId,
        piece: Piece<String>,
    ) -> Result<ReplicationAnswer, SendFailure> {
        let mut owner = self.0.lock().await;
        let socket = owner.as_mut().ok_or(SendFailure::AnswerLost)?;
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
type Sender = ReplicationSender<SampleSchema, Annotation, (), SocketTransport>;
struct ManualWriter {
    database: Arc<ReplicatedDatabase<SampleSchema>>,
    transport: Arc<SocketTransport>,
    sender: Sender,
    lease: LeaseStatus,
    started: SyncTime,
    session: SessionId,
    receiver: ReceiverId,
}
impl ManualWriter {
    async fn open(path: &Path, service: &ServiceProcess) -> TestResult<Self> {
        let peer = PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000002,
        ))?;
        let receiver = ReceiverId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000003,
        ))?;
        let database = Arc::new(
            ReplicatedDatabase::create(
                path,
                SampleSchema::new()?,
                peer,
                LeasePolicy {
                    lease_duration: Duration::from_secs(600),
                    renew_interval: Duration::from_secs(1),
                },
                WriterOptions::default(),
            )
            .await?,
        );
        let session = SessionId::new(durable_steps::Uuid::now_v7())?;
        let mut socket = service.peer(2, session).await?;
        let started = SystemClock.now();
        let lease = match request(
            &mut socket,
            OperationRequest::Lease(LeaseRequest::Register(Registration {
                attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
                database: database.database(),
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
            _ => return Err("registration failed".into()),
        };
        database
            .observe_registration(lease.clone(), started)
            .await?;
        let transport = Arc::new(SocketTransport(Mutex::new(Some(socket))));
        let sender = ReplicationSender::new(
            database.clone(),
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
        let fixture = Self {
            database,
            transport,
            sender,
            lease,
            started,
            session,
            receiver,
        };
        fixture.insert("accepted before transition").await?;
        fixture.sender.replicate_once(receiver).await?;
        Ok(fixture)
    }
    async fn insert(&self, label: &str) -> TestResult<ReplicationPosition> {
        let mut write = self.database.begin_write().await?;
        insert_sample(
            &mut write,
            durable_steps::Uuid::now_v7().to_string(),
            label.into(),
            9_007_199_254_740_993,
            vec![0, 255, 128],
        )
        .await?;
        write.commit().await?;
        Ok(self.database.snapshot().await?.position())
    }
}

#[tokio::test]
async fn withdrawal_keeps_original_real_clock_deadline_then_fresh_renewal_resumes_same_epoch()
-> TestResult {
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start_with_lease(&root.path().join("service"), 3, 5_000).await?;
    let writer = ManualWriter::open(&root.path().join("writer.db"), &service).await?;
    let database = writer.database.database();
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let attempt = TransferId::new(durable_steps::Uuid::now_v7())?;
    let _capability_change = service
        .peer(4, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let change = HolderChangeRequest::Move {
        attempt,
        database,
        from_epoch: writer.lease.epoch,
        to: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000004,
        ))?,
    };
    control_request(&mut control, ControlRequest::HolderChange(change.clone())).await?;
    {
        let mut owner = writer.transport.0.lock().await;
        let socket = owner.as_mut().ok_or("writer socket missing")?;
        let (frame, call, release) = match server_frame(socket).await? {
            ServerFrame::Request {
                frame,
                body: ServerRequest::Crate(ServerCall::Release { call, request }),
            } => (frame, call, request),
            _ => return Err("release RPC missing".into()),
        };
        let answer = writer.database.request_release(&release, call).await?;
        assert!(matches!(
            control_request(
                &mut control,
                ControlRequest::CancelHolderChange(CancelHolderChange {
                    attempt,
                    by: ActorRef::new("deadline-admin")?,
                    reason: CancelReason::new("withdraw without renewing")?
                })
            )
            .await?,
            ControlAnswer::Cancel(CancelAnswer::Accepted { .. })
        ));
        socket
            .send(
                serde_json::to_vec(&ClientFrame::<SampleProtocol>::Answer {
                    frame,
                    body: ClientAnswer::Crate(ServerCallAnswer::Release(answer)),
                })?
                .into(),
            )
            .await?;
        let (frame, call, withdrawal) = match server_frame(socket).await? {
            ServerFrame::Request {
                frame,
                body: ServerRequest::Crate(ServerCall::WithdrawRelease { call, request }),
            } => (frame, call, request),
            _ => return Err("withdrawal RPC missing".into()),
        };
        let answer = writer.database.withdraw_release(&withdrawal, call).await?;
        assert_eq!(answer, WithdrawAnswer::Withdrawn);
        socket
            .send(
                serde_json::to_vec(&ClientFrame::<SampleProtocol>::Answer {
                    frame,
                    body: ClientAnswer::Crate(ServerCallAnswer::WithdrawRelease(answer)),
                })?
                .into(),
            )
            .await?;
    }
    assert!(matches!(
        control_request(&mut control, ControlRequest::WaitHolderChange(change)).await?,
        ControlAnswer::HolderChange(HolderChangeAnswer::Cancelled { .. })
    ));
    let resumed = writer.insert("accepted after withdrawal").await?;
    assert_eq!(resumed.epoch, writer.lease.epoch);
    writer.sender.replicate_once(writer.receiver).await?;
    let original_deadline = writer
        .started
        .after(Duration::from_millis(writer.lease.remaining_ms));
    let timer = TokioWakeTimer::new(SystemClock);
    // This is the lease's actual recorded time event, not a proxy for work finishing.
    timer
        .wake_at(original_deadline.max(writer.lease.expires_at))
        .await?;
    timer.wait().await;
    assert!(
        matches!(writer.database.begin_write().await, Err(WriteRefusal::LeaseExpired { expired_at, .. }) if expired_at==original_deadline)
    );
    let old = writer
        .transport
        .0
        .lock()
        .await
        .take()
        .ok_or("writer socket missing")?;
    old.close(CloseReason::ClientClosed).await;
    let (mut socket, statuses) = service.peer_with_statuses(2, writer.session).await?;
    assert!(statuses.iter().any(|status| matches!(status, DatabaseStatus::Ready { lease, .. } if lease.epoch==writer.lease.epoch && lease.state==LeaseState::Expired)));
    let started = SystemClock.now();
    let answer = match request(
        &mut socket,
        OperationRequest::Lease(LeaseRequest::Renew(RenewRequest {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database,
            epoch: writer.lease.epoch,
        })),
    )
    .await?
    {
        OperationAnswer::Lease(LeaseAnswer::Renew(answer)) => answer,
        _ => return Err("fresh renewal missing".into()),
    };
    writer.database.observe_renewal(answer, started).await?;
    *writer.transport.0.lock().await = Some(socket);
    let renewed = writer.insert("accepted after renewal").await?;
    assert_eq!(renewed.epoch, writer.lease.epoch);
    writer.sender.replicate_once(writer.receiver).await?;
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    assert_eq!(
        copy_rows(&mut reader, database)
            .await?
            .get("samples")
            .map(Vec::len),
        Some(3)
    );
    assert!(
        matches!(control_request(&mut control, ControlRequest::CopyStatus { database }).await?, ControlAnswer::CopyStatus(CopyStatusAnswer::Status(status)) if status.fence==writer.lease.epoch && status.acked==renewed)
    );
    writer.database.close().await;
    service.stop(&mut control).await?;
    Ok(())
}

#[tokio::test]
async fn stale_holder_move_refuses_then_a_recently_renewed_holder_moves_over_its_session()
-> TestResult {
    let root = tempfile::tempdir()?;
    let service =
        ServiceProcess::start_with_policy(&root.path().join("service"), 3, 20_000, 1_000).await?;
    let writer = ManualWriter::open(&root.path().join("writer.db"), &service).await?;
    let database = writer.database.database();
    let renewed_at = SyncTime::from_unix_millis(writer.lease.expires_at.unix_millis() - 20_000);
    let timer = TokioWakeTimer::new(SystemClock);
    timer
        .wake_at(renewed_at.after(Duration::from_millis(1_001)))
        .await?;
    timer.wait().await;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let _capability_stale = service
        .peer(4, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let stale = HolderChangeRequest::Move {
        attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
        database,
        from_epoch: writer.lease.epoch,
        to: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000004,
        ))?,
    };
    control_request(&mut control, ControlRequest::HolderChange(stale.clone())).await?;
    assert!(
        matches!(control_request(&mut control, ControlRequest::WaitHolderChange(stale)).await?, ControlAnswer::HolderChange(HolderChangeAnswer::Refused(HolderChangeRefusal::HolderStale { last_renewed_at })) if last_renewed_at==renewed_at)
    );
    let accepted = writer
        .insert("still writing after stale move refusal")
        .await?;
    let mut owner = writer.transport.0.lock().await;
    let socket = owner.as_mut().ok_or("writer socket missing")?;
    let started = SystemClock.now();
    let renewal = match request(
        socket,
        OperationRequest::Lease(LeaseRequest::Renew(RenewRequest {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database,
            epoch: writer.lease.epoch,
        })),
    )
    .await?
    {
        OperationAnswer::Lease(LeaseAnswer::Renew(answer)) => answer,
        _ => return Err("fresh holder renewal missing".into()),
    };
    let fresh_at = match &renewal {
        RenewAnswer::Renewed { recorded, .. } => {
            SyncTime::from_unix_millis(recorded.expires_at.unix_millis() - 20_000)
        }
        _ => return Err("fresh holder renewal refused".into()),
    };
    writer.database.observe_renewal(renewal, started).await?;
    let attempt = TransferId::new(durable_steps::Uuid::now_v7())?;
    let _capability_fresh = service
        .peer(4, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let fresh = HolderChangeRequest::Move {
        attempt,
        database,
        from_epoch: writer.lease.epoch,
        to: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000004,
        ))?,
    };
    control_request(&mut control, ControlRequest::HolderChange(fresh.clone())).await?;
    let (frame, call, release) = match server_frame(socket).await? {
        ServerFrame::Request {
            frame,
            body: ServerRequest::Crate(ServerCall::Release { call, request }),
        } => (frame, call, request),
        _ => return Err("recently renewed holder was not asked to release".into()),
    };
    let answer = writer.database.request_release(&release, call).await?;
    socket
        .send(
            serde_json::to_vec(&ClientFrame::<SampleProtocol>::Answer {
                frame,
                body: ClientAnswer::Crate(ServerCallAnswer::Release(answer)),
            })?
            .into(),
        )
        .await?;
    drop(owner);
    writer.sender.replicate_once(writer.receiver).await?;
    assert!(
        matches!(control_request(&mut control, ControlRequest::WaitHolderChange(fresh)).await?, ControlAnswer::HolderChange(HolderChangeAnswer::Granted { epoch, start, .. }) if epoch>writer.lease.epoch && start==accepted)
    );
    let view =
        match control_request(&mut control, ControlRequest::HolderChangeView { attempt }).await? {
            ControlAnswer::HolderChangeView(Some(view)) => view,
            _ => return Err("fresh move history missing".into()),
        };
    assert!(view.history.iter().any(|entry| entry.phase
        == fleet_sync_lease::HolderChangePhase::Releasing
        && entry.entered_at <= fresh_at.after(Duration::from_millis(1_000))));
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    assert_eq!(
        copy_rows(&mut reader, database)
            .await?
            .get("samples")
            .map(Vec::len),
        Some(2)
    );
    writer.database.close().await;
    service.stop(&mut control).await?;
    Ok(())
}

#[tokio::test]
async fn authoritative_copy_refuses_the_takeover_tail_while_old_writer_keeps_it_readable()
-> TestResult {
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start(&root.path().join("service")).await?;
    let writer = ManualWriter::open(&root.path().join("writer.db"), &service).await?;
    let database = writer.database.database();
    let before = writer.database.snapshot().await?.position();
    let mut admitted = writer.database.begin_write().await?;
    assert_eq!(admitted.admitted_epoch(), writer.lease.epoch);
    insert_sample(
        &mut admitted,
        durable_steps::Uuid::now_v7().to_string(),
        "kept rejected tail".into(),
        9_007_199_254_740_993,
        vec![0, 255, 128],
    )
    .await?;
    // Hold the actual session transport so the writer cannot learn the takeover.
    let held_session = writer.transport.0.lock().await;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let preview = match control_request(
        &mut control,
        ControlRequest::PreviewTakeover {
            database,
            attempt: durable_steps::Uuid::now_v7(),
        },
    )
    .await?
    {
        ControlAnswer::TakeoverPreview(preview) => preview,
        _ => return Err("takeover preview missing".into()),
    };
    let _capability_takeover = service
        .peer(4, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let takeover = HolderChangeRequest::Takeover {
        attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
        database,
        preview,
        to: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000004,
        ))?,
    };
    control_request(&mut control, ControlRequest::HolderChange(takeover.clone())).await?;
    assert!(
        matches!(control_request(&mut control, ControlRequest::WaitHolderChange(takeover)).await?, ControlAnswer::HolderChange(HolderChangeAnswer::Granted { epoch, start, .. }) if epoch>writer.lease.epoch && start==before)
    );
    // The isolated holder has not learned the new authority yet. Its cached
    // lease still permits this write; only the copy can reject its epoch.
    assert_eq!(admitted.admitted_epoch(), writer.lease.epoch);
    admitted.commit().await?;
    drop(held_session);
    let tail = writer.database.snapshot().await?.position();
    let events = writer.sender.replicate_once(writer.receiver).await?;
    assert!(events.iter().any(|event| matches!(event, ReplicationEvents::RejectedTail(rejected) if rejected.ended_at==before && rejected.last_commit==tail)));
    let rejected = writer.sender.rejected_tails().await?;
    assert_eq!(rejected.len(), 1);
    assert!(matches!(
        writer.database.begin_write().await,
        Err(WriteRefusal::Superseded { .. })
    ));
    let row = query::<sqlx_turso::Turso>("SELECT count(*) AS samples FROM samples")
        .fetch_one(writer.database.reader())
        .await?;
    assert_eq!(row.try_get::<i64, _>("samples")?, 2);
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
    writer.database.close().await;
    service.stop(&mut control).await?;
    Ok(())
}
