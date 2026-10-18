#[path = "support/snapshot.rs"]
mod snapshot;
#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_test_consumer::*;
use fleet_sync_tokio_runner::{InstallRecord, InstallStatus};
use fleet_sync_transport_websocket::WebSocketConnection;
use snapshot::prepare_source;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
    row::Row,
};
use support::*;

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
    let receiver = ReceiverId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000003,
    ))?;
    let attempt = InstallAttemptId::new(durable_steps::Uuid::now_v7())?;
    let install_id = InstallId::new(attempt.as_uuid())?;
    let started = install_answer(
        &mut control,
        ControlRequest::InstallSnapshot {
            attempt,
            database,
            source: receiver,
        },
    )
    .await?;
    assert_eq!(started.install_id, install_id);
    control_request(
        &mut control,
        ControlRequest::WaitSourceUnattached { receiver },
    )
    .await?;
    assert!(matches!(
        install_answer(
            &mut control,
            ControlRequest::InstallRecord {
                install: install_id
            }
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
    assert_eq!(staged.install_id, install_id);
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
        ControlRequest::InstallRecord {
            install: install_id,
        },
    )
    .await?;
    assert_eq!(waiting.install_id, install_id);
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
    assert_eq!(ready.install_id, install_id);
    assert_eq!(ready.generation, 1);
    let installed_position = match ready.status {
        InstallStatus::Ready { snapshot } => snapshot.position,
        status => return Err(format!("install did not reach ready: {status:?}").into()),
    };
    // No peer read/status request may supply the copy's missing wake.
    let installed_effect =
        match control_request(&mut control, ControlRequest::WaitEffects { count: 1 }).await? {
            ControlAnswer::Effects(receipts) if receipts.len() == 1 => receipts
                .into_iter()
                .next()
                .ok_or("snapshot effect missing")?,
            _ => return Err("snapshot hook follow-up did not run while target was idle".into()),
        };
    assert_eq!(installed_effect.saved.effect.database, database);
    assert_eq!(installed_effect.saved.effect.position, installed_position);
    assert_eq!(installed_effect.saved.effect.annotation, "snapshot");
    assert_eq!(installed_effect.attempts, 1);
    control_request(
        &mut control,
        ControlRequest::ReplaySnapshotReady {
            install: install_id,
        },
    )
    .await?;
    assert!(
        matches!(control_request(&mut control, ControlRequest::Effects).await?, ControlAnswer::Effects(receipts) if receipts.len()==1 && receipts.first().is_some_and(|receipt| receipt.saved==installed_effect.saved))
    );
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
        durable_steps::Uuid::from_u128(0x019eef00000070008000000000000004),
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
        assert_eq!(receipt.install_id, install_id);
        total_rows += receipt.rows;
        if expected_page == 0 {
            assert_eq!(receipt, staged);
        }
    }
    assert_eq!(total_rows, 301);
    file.close().await?;
    let resumed = ServiceProcess::start_as(&target_path, 4).await?;
    let mut control = resumed
        .connect("control", "Bearer consumer-control")
        .await?;
    control_request(
        &mut control,
        ControlRequest::ReplaySnapshotReady {
            install: install_id,
        },
    )
    .await?;
    assert!(
        matches!(control_request(&mut control, ControlRequest::Effects).await?, ControlAnswer::Effects(receipts) if receipts.len()==1 && receipts.first().is_some_and(|receipt| receipt.saved==installed_effect.saved))
    );
    resumed.stop(&mut control).await?;
    let mut source_control = source.connect("control", "Bearer consumer-control").await?;
    source.stop(&mut source_control).await?;
    println!(
        "OBS real attached SessionClient/WebSocket source: same install and first page receipt survive process kill; unattached source returns retryable NotSent; reattachment completes four pages once with 301 exact rows"
    );
    Ok(())
}

#[tokio::test]
async fn refused_snapshot_ready_rolls_back_hook_rows_and_follow_up_admission() -> TestResult {
    let root = tempfile::tempdir()?;
    let source = ServiceProcess::start_as(&root.path().join("source"), 3).await?;
    let (database, _) = prepare_source(&source, root.path()).await?;
    let target_path = root.path().join("refusing-target");
    let target = ServiceProcess::start_as(&target_path, 4).await?;
    let mut control = target.connect("control", "Bearer consumer-control").await?;
    control_request(
        &mut control,
        ControlRequest::SetHookFailure { enabled: true },
    )
    .await?;
    let source_receiver = ReceiverId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000003,
    ))?;
    control_request(
        &mut control,
        ControlRequest::AttachSource {
            receiver: source_receiver,
            peer: 6,
            endpoint: format!("ws://127.0.0.1:{}/session", source.port),
        },
    )
    .await?;
    let attempt = InstallAttemptId::new(durable_steps::Uuid::now_v7())?;

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
    assert!(matches!(failed.status, InstallStatus::Failed { .. }));
    assert!(
        matches!(control_request(&mut control, ControlRequest::Effects).await?, ControlAnswer::Effects(receipts) if receipts.is_empty())
    );
    target.stop(&mut control).await?;
    let live = target_path.join("service").join(format!(
        "copy-{}-{}.db",
        durable_steps::Uuid::from_u128(0x019eef00000070008000000000000004),
        database.as_uuid()
    ));
    let mut file = sqlx_turso::TursoConnectOptions::new()
        .filename(live)
        .create_if_missing(false)
        .foreign_keys(false)
        .connect()
        .await?;
    for sql in [
        "SELECT count(*) AS total FROM hook_events",
        "SELECT count(*) AS total FROM _fleet_sync_operations WHERE kind='fleet-sync.apply-follow-up'",
    ] {
        assert_eq!(
            query::<sqlx_turso::Turso>(sql)
                .fetch_one(&mut file)
                .await?
                .try_get::<i64, _>("total")?,
            0
        );
    }
    file.close().await?;
    let store = ConsumerStore::open(&target_path.join("consumer.db")).await?;
    assert!(store.effects().await?.is_empty());
    store.close().await;
    let mut control = source.connect("control", "Bearer consumer-control").await?;
    source.stop(&mut control).await?;
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
    let peer = PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000002,
    ))?;
    let source_receiver = ReceiverId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000003,
    ))?;
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
        let attempt = InstallAttemptId::new(durable_steps::Uuid::now_v7())?;

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
