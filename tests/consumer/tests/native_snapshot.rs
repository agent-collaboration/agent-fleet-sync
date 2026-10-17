#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::*;
use fleet_sync_tokio_runner::{InstallRecord, InstallStatus};
use fleet_sync_transport_websocket::WebSocketConnection;
use fleet_sync_writer::*;
use runtime_primitives::{Clock, SystemClock};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
    row::Row,
};
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
async fn prepare_source(
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
async fn install_answer(
    control: &mut WebSocketConnection,
    request: ControlRequest,
) -> TestResult<InstallRecord> {
    match control_request(control, request).await? {
        ControlAnswer::Install(record) => Ok(record),
        _ => Err("missing canonical install record".into()),
    }
}

#[tokio::test]
async fn attached_websocket_source_resumes_same_install_after_process_kill_and_waits_for_reattachment()
-> TestResult {
    let root = tempfile::tempdir()?;
    let source = ServiceProcess::start_as(&root.path().join("source"), 3).await?;
    let (database, expected) = prepare_source(&source, root.path()).await?;
    let target_path = root.path().join("target");
    let mut target = ServiceProcess::start_as(&target_path, 4).await?;
    let mut control = target.connect("control", "Bearer consumer-control").await?;
    let receiver = ReceiverId::new(durable_steps::Uuid::from_u128(3))?;
    let attempt = durable_steps::Uuid::now_v7();
    let started = install_answer(
        &mut control,
        ControlRequest::InstallSnapshot {
            attempt,
            database,
            source: receiver,
        },
    )
    .await?;
    assert_eq!(started.install_id, attempt);
    control_request(
        &mut control,
        ControlRequest::WaitSourceUnattached { receiver },
    )
    .await?;
    assert!(matches!(
        install_answer(
            &mut control,
            ControlRequest::InstallRecord { install: attempt }
        )
        .await?
        .status,
        InstallStatus::Running { .. }
    ));
    control_request(&mut control, ControlRequest::HoldNextInstallPageAnswer).await?;
    control_request(
        &mut control,
        ControlRequest::AttachSource {
            receiver,
            peer: 2,
            endpoint: format!("ws://127.0.0.1:{}/session", source.port),
        },
    )
    .await?;
    let staged = match control_request(&mut control, ControlRequest::WaitStagedInstallPage).await? {
        ControlAnswer::Staged(receipt) => receipt,
        _ => return Err("page commit was not observed".into()),
    };
    assert_eq!(staged.install_id, attempt);
    assert_eq!(staged.page, 0);
    assert_eq!(staged.rows, 1);
    target.kill().await?;
    drop(control);

    let restarted = ServiceProcess::start_as(&target_path, 4).await?;
    let mut control = restarted
        .connect("control", "Bearer consumer-control")
        .await?;
    // The engine's existing drive lease remains authoritative for 30 seconds.
    // This waits for the actual next source call; no sleep substitutes for it.
    control_request(
        &mut control,
        ControlRequest::WaitSourceUnattached { receiver },
    )
    .await?;
    let waiting = install_answer(
        &mut control,
        ControlRequest::InstallRecord { install: attempt },
    )
    .await?;
    assert_eq!(waiting.install_id, attempt);
    assert!(matches!(waiting.status, InstallStatus::Running { .. }));
    control_request(
        &mut control,
        ControlRequest::AttachSource {
            receiver,
            peer: 2,
            endpoint: format!("ws://127.0.0.1:{}/session", source.port),
        },
    )
    .await?;
    let ready = install_answer(
        &mut control,
        ControlRequest::WaitInstall { install: attempt },
    )
    .await?;
    assert_eq!(ready.install_id, attempt);
    assert_eq!(ready.generation, 1);
    let installed_position = match ready.status {
        InstallStatus::Ready { snapshot } => snapshot.position,
        status => return Err(format!("install did not reach ready: {status:?}").into()),
    };
    let mut target_peer = restarted
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    assert_eq!(copy_rows(&mut target_peer, database).await?, expected);
    assert!(
        matches!(control_request(&mut control, ControlRequest::CopyStatus { database }).await?, ControlAnswer::CopyStatus(CopyStatusAnswer::Status(status)) if status.role == CopyRole::Follower && status.acked == installed_position)
    );
    restarted.stop(&mut control).await?;

    let live = target_path.join("service").join(format!(
        "copy-{}-{}.db",
        durable_steps::Uuid::from_u128(4),
        database.as_uuid()
    ));
    let mut file = sqlx_turso::TursoConnectOptions::new()
        .filename(live)
        .create_if_missing(false)
        .foreign_keys(false)
        .connect()
        .await?;
    let rows = query::<sqlx_turso::Turso>(
        "SELECT page,record FROM _fleet_sync_install_pages ORDER BY page",
    )
    .fetch_all(&mut file)
    .await?;
    assert_eq!(rows.len(), 4);
    let mut total_rows = 0;
    for (expected_page, row) in rows.iter().enumerate() {
        assert_eq!(
            row.try_get::<i64, _>("page")?,
            i64::try_from(expected_page)?
        );
        let receipt: fleet_sync_receiver::InstallPageReceipt =
            serde_json::from_str(&row.try_get::<String, _>("record")?)?;
        assert_eq!(receipt.install_id, attempt);
        total_rows += receipt.rows;
        if expected_page == 0 {
            assert_eq!(receipt, staged);
        }
    }
    assert_eq!(total_rows, 301);
    file.close().await?;
    let mut source_control = source.connect("control", "Bearer consumer-control").await?;
    source.stop(&mut source_control).await?;
    println!(
        "OBS real attached SessionClient/WebSocket source: same install and first page receipt survive process kill; unattached source returns retryable NotSent; reattachment completes four pages once with 301 exact rows"
    );
    Ok(())
}

#[tokio::test]
async fn failed_installs_project_file_owned_positions_in_status_and_fresh_welcome() -> TestResult {
    use fleet_sync_session::TransportConnection;
    let root = tempfile::tempdir()?;
    let source = ServiceProcess::start_as(&root.path().join("source"), 3).await?;
    let (database, _) = prepare_source(&source, root.path()).await?;
    let mut source_control = source.connect("control", "Bearer consumer-control").await?;
    let source_position = match control_request(
        &mut source_control,
        ControlRequest::CopyStatus { database },
    )
    .await?
    {
        ControlAnswer::CopyStatus(CopyStatusAnswer::Status(status)) => status.acked,
        _ => return Err("source has no position".into()),
    };
    let peer = PeerId::new(durable_steps::Uuid::from_u128(2))?;
    let source_receiver = ReceiverId::new(durable_steps::Uuid::from_u128(3))?;
    for initialized in [false, true] {
        let target = ServiceProcess::start_as(
            &root.path().join(if initialized {
                "initialized"
            } else {
                "never-initialized"
            }),
            if initialized { 10 } else { 11 },
        )
        .await?;
        let mut peer_socket = target
            .peer(2, SessionId::new(durable_steps::Uuid::now_v7())?)
            .await?;
        let registration = request(
            &mut peer_socket,
            OperationRequest::Lease(LeaseRequest::Register(Registration {
                attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
                database,
                writer: peer,
                origin: if initialized {
                    Origin::Empty
                } else {
                    Origin::Seeded {
                        position: source_position,
                    }
                },
            })),
        )
        .await?;
        assert!(matches!(
            registration,
            OperationAnswer::Lease(LeaseAnswer::Register(RegistrationAnswer::Registered { .. }))
        ));
        let mut control = target.connect("control", "Bearer consumer-control").await?;
        let before =
            match control_request(&mut control, ControlRequest::CopyStatus { database }).await? {
                ControlAnswer::CopyStatus(status) => status,
                _ => return Err("missing original copy status".into()),
            };
        control_request(
            &mut control,
            ControlRequest::AttachSource {
                receiver: source_receiver,
                peer: 2,
                endpoint: format!("ws://127.0.0.1:{}/session", source.port),
            },
        )
        .await?;
        let attempt = durable_steps::Uuid::now_v7();
        install_answer(
            &mut control,
            ControlRequest::InstallSnapshot {
                attempt,
                database,
                source: source_receiver,
            },
        )
        .await?;
        let failed = install_answer(
            &mut control,
            ControlRequest::WaitInstall { install: attempt },
        )
        .await?;
        assert!(
            matches!(failed.status, InstallStatus::Failed { .. }),
            "an authoritative target must refuse replacement"
        );
        let statuses =
            match control_request(&mut control, ControlRequest::DatabaseStatuses { peer }).await? {
                ControlAnswer::DatabaseStatuses(statuses) => statuses,
                _ => return Err("missing projected statuses".into()),
            };
        let expected = if initialized {
            let CopyStatusAnswer::Status(status) = before else {
                return Err("initialized copy has no ack".into());
            };
            DatabaseStatus::NeedsSnapshot {
                database,
                acked: status.acked,
            }
        } else {
            assert_eq!(before, CopyStatusAnswer::NotInitialized);
            let Some(DatabaseStatus::Unavailable {
                database: observed,
                lease,
                reason: CopyUnavailableReason::Uninitialized,
            }) = statuses.first()
            else {
                return Err("never-initialized failure fabricated an acknowledged position".into());
            };
            assert_eq!(*observed, database);
            assert!(lease.is_some());
            statuses
                .first()
                .cloned()
                .ok_or("no uninitialized projection")?
        };
        assert_eq!(statuses, vec![expected.clone()]);
        let mut welcome_socket = target.connect("session", "Bearer peer-2").await?;
        let hello: ClientFrame<SampleProtocol> = ClientFrame::Hello {
            session_id: SessionId::new(durable_steps::Uuid::now_v7())?,
            schema_support: SchemaSupport { max_version: 100 },
        };
        welcome_socket
            .send(serde_json::to_vec(&hello)?.into())
            .await?;
        let databases = match server_frame(&mut welcome_socket).await? {
            ServerFrame::Welcome { databases, .. } => databases,
            _ => return Err("fresh connection did not receive Welcome".into()),
        };
        assert_eq!(databases.len(), 1);
        match (databases.first(), expected) {
            (
                Some(DatabaseStatus::NeedsSnapshot {
                    database: observed,
                    acked,
                }),
                DatabaseStatus::NeedsSnapshot {
                    database,
                    acked: expected,
                },
            ) => {
                assert_eq!((*observed, *acked), (database, expected));
            }
            (
                Some(DatabaseStatus::Unavailable {
                    database: observed,
                    lease: Some(actual),
                    reason: CopyUnavailableReason::Uninitialized,
                }),
                DatabaseStatus::Unavailable {
                    database,
                    lease: Some(prior),
                    reason: CopyUnavailableReason::Uninitialized,
                },
            ) => {
                assert_eq!(*observed, database);
                assert_eq!(
                    (
                        actual.database,
                        actual.holder,
                        actual.epoch,
                        actual.state,
                        actual.expires_at
                    ),
                    (
                        prior.database,
                        prior.holder,
                        prior.epoch,
                        prior.state,
                        prior.expires_at
                    )
                );
                assert!(
                    actual.remaining_ms > 0 && actual.remaining_ms <= prior.remaining_ms,
                    "Welcome must freshly compute the remaining lease"
                );
            }
            _ => return Err("Welcome did not project the canonical install failure".into()),
        }
        target.stop(&mut control).await?;
    }
    source.stop(&mut source_control).await?;
    println!(
        "OBS failed-install status and real fresh Welcome: never-initialized target stays Unavailable(Uninitialized); initialized target reports NeedsSnapshot with its own prior ack, never the source position"
    );
    Ok(())
}
