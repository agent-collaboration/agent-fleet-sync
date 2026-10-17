#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::*;
use fleet_sync_transport_websocket::WebSocketConnection;
use fleet_sync_writer::{ReplicatedDatabase, WriterOptions};
use std::{path::Path, time::Duration};
use support::*;

async fn import_backup(
    service: &ServiceProcess,
    control: &mut WebSocketConnection,
    path: &Path,
) -> TestResult<(Import, ReplicationPosition)> {
    let writer = ReplicatedDatabase::create(
        path,
        SampleSchema::new()?,
        PeerId::new(durable_steps::Uuid::from_u128(2))?,
        LeasePolicy {
            lease_duration: Duration::from_secs(600),
            renew_interval: Duration::from_secs(30),
        },
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?;
    let mut write = writer.begin_write().await?;
    insert_sample(
        &mut write,
        "019eef00-0000-7000-8000-000000000001".into(),
        "imported backup".into(),
        9_007_199_254_740_993,
        vec![0, 255, 128],
    )
    .await?;
    write.commit().await?;
    let mut snapshot = writer.snapshot().await?;
    let manifest = snapshot.manifest().await?;
    let import = Import {
        attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
        database: writer.database(),
        digest: ImportDigest::of_canonical(&manifest)?,
    };
    assert!(matches!(
        control_request(control, ControlRequest::Import(import.clone())).await?,
        ControlAnswer::Import(ImportAnswer::Imported)
    ));
    let seed = match control_request(
        control,
        ControlRequest::SeedBegin {
            database: writer.database(),
            attempt: durable_steps::Uuid::now_v7(),
        },
    )
    .await?
    {
        ControlAnswer::Replication(answer) => match *answer {
            ReplicationAnswer::SeedStarted { seed } => seed,
            _ => return Err("import seed did not begin".into()),
        },
        _ => return Err("import seed answer missing".into()),
    };
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let contents = snapshot.page(cursor).await?;
        let next = contents.next.clone();
        let page = SeedPage {
            seed,
            page: pages,
            digest: PageDigest::of(&contents)?,
            contents,
        };
        assert!(
            matches!(control_request(control, ControlRequest::SeedPage(Box::new(page))).await?, ControlAnswer::Replication(answer) if matches!(*answer, ReplicationAnswer::SeedPage(SeedPageAnswer::Staged)))
        );
        pages += 1;
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    assert!(
        matches!(control_request(control, ControlRequest::SeedComplete(SeedComplete {
        database: writer.database(), seed, position: manifest.position, pages, schema: manifest.schema,
        table_digests: manifest.tables.into_iter().map(|table| (table.table, table.digest)).collect(),
    })).await?, ControlAnswer::Replication(answer) if matches!(*answer, ReplicationAnswer::SeedComplete(SeedAnswer::Published { position }) if position==manifest.position))
    );
    let mut reader = service
        .peer(5, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let rows = copy_rows(&mut reader, writer.database()).await?;
    assert_eq!(rows.get("samples").map(Vec::len), Some(1));
    assert!(rows.get("samples").ok_or("samples missing")?.iter().all(|row| matches!(row.as_slice(),[SqlValue::Text(_),SqlValue::Text(label),SqlValue::Integer(9_007_199_254_740_993),SqlValue::Blob(bytes)] if label=="imported backup" && bytes==&[0,255,128])));
    reader.close(CloseReason::ClientClosed).await;
    snapshot.close().await?;
    writer.close().await;
    Ok((import, manifest.position))
}

#[tokio::test]
async fn import_claim_settlement_and_renewal_replay_survive_registry_restart_and_revocation()
-> TestResult {
    let root = tempfile::tempdir()?;
    let service_path = root.path().join("service");
    let mut service = ServiceProcess::start(&service_path).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let (import, position) =
        import_backup(&service, &mut control, &root.path().join("backup.db")).await?;
    let database = import.database;
    assert!(matches!(
        control_request(&mut control, ControlRequest::Import(import.clone())).await?,
        ControlAnswer::Import(ImportAnswer::Imported)
    ));
    let mut reimport = import.clone();
    reimport.attempt = RequestId::new(durable_steps::Uuid::now_v7())?;
    assert!(matches!(
        control_request(&mut control, ControlRequest::Import(reimport)).await?,
        ControlAnswer::Import(ImportAnswer::AlreadyImported)
    ));
    let mut conflicting = import.clone();
    conflicting.attempt = RequestId::new(durable_steps::Uuid::now_v7())?;
    conflicting.digest = ImportDigest::of_canonical(&"another backup")?;
    assert!(matches!(
        control_request(&mut control, ControlRequest::Import(conflicting)).await?,
        ControlAnswer::Import(ImportAnswer::ImportConflict { .. })
    ));
    control_request(&mut control, ControlRequest::LoseNextClaimSettlementAnswer).await?;
    let session = SessionId::new(durable_steps::Uuid::now_v7())?;
    let mut peer = service.peer(4, session).await?;
    let claim_attempt = RequestId::new(durable_steps::Uuid::now_v7())?;
    request(
        &mut peer,
        OperationRequest::Lease(LeaseRequest::Claim {
            attempt: claim_attempt,
            database,
        }),
    )
    .await?;
    let transfer = TransferId::new(claim_attempt.as_uuid())?;
    let first = match control_request(
        &mut control,
        ControlRequest::WaitClaimSettlement {
            attempt: transfer,
            count: 1,
        },
    )
    .await?
    {
        ControlAnswer::ClaimReceipt(receipt) => receipt,
        _ => return Err("claim settlement missing".into()),
    };
    let settlement = first.settlement.ok_or("claim did not settle")?;
    let SettleOutcome::Granted { epoch } = settlement.outcome else {
        return Err("claim was not granted".into());
    };
    assert_eq!(epoch, Epoch::new(2)?);
    let view = match control_request(
        &mut control,
        ControlRequest::HolderChangeView { attempt: transfer },
    )
    .await?
    {
        ControlAnswer::HolderChangeView(Some(view)) => view,
        _ => return Err("claim admin view missing".into()),
    };
    assert!(
        matches!(view.outcome, Some(HolderChangeAnswer::Granted { epoch: current, .. }) if current==epoch)
    );
    assert!(view.settlement_owed);
    assert!(
        matches!(request(&mut peer, OperationRequest::Lease(LeaseRequest::Claim { attempt: claim_attempt, database })).await?, OperationAnswer::Lease(LeaseAnswer::Claim(HolderChangeAnswer::Granted { epoch: current, .. })) if current==epoch)
    );
    service.kill().await?;
    drop(control);
    service = ServiceProcess::start(&service_path).await?;
    control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let retried = match control_request(
        &mut control,
        ControlRequest::WaitClaimSettlement {
            attempt: transfer,
            count: 2,
        },
    )
    .await?
    {
        ControlAnswer::ClaimReceipt(receipt) => receipt,
        _ => return Err("settlement did not retry".into()),
    };
    assert_eq!(retried.settlement, Some(settlement));
    let view = match control_request(
        &mut control,
        ControlRequest::WaitHolderChangeSettled { attempt: transfer },
    )
    .await?
    {
        ControlAnswer::HolderChangeView(Some(view)) => view,
        _ => return Err("claim reconciliation missing".into()),
    };
    assert!(!view.settlement_owed);
    assert!(view.follow_up_failure.is_none());
    let (resumed, statuses) = service.peer_with_statuses(4, session).await?;
    assert!(statuses.iter().any(|status| matches!(status, DatabaseStatus::Ready { database: current_database, acked, lease } if *current_database==database && *acked==position && lease.epoch==epoch)));
    peer = resumed;
    let renewal = RenewRequest {
        attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
        database,
        epoch,
    };
    let first_renewal = match request(
        &mut peer,
        OperationRequest::Lease(LeaseRequest::Renew(renewal.clone())),
    )
    .await?
    {
        OperationAnswer::Lease(LeaseAnswer::Renew(RenewAnswer::Renewed { recorded, .. })) => {
            recorded
        }
        _ => return Err("claimant renewal missing".into()),
    };
    let later = RenewRequest {
        attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
        ..renewal.clone()
    };
    request(
        &mut peer,
        OperationRequest::Lease(LeaseRequest::Renew(later)),
    )
    .await?;
    assert!(
        matches!(request(&mut peer, OperationRequest::Lease(LeaseRequest::Renew(renewal.clone()))).await?, OperationAnswer::Lease(LeaseAnswer::Renew(RenewAnswer::Renewed { recorded, current, replayed: true, .. })) if recorded==first_renewal && current.epoch==epoch)
    );
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
    let takeover = HolderChangeRequest::Takeover {
        attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
        database,
        preview,
        to: PeerId::new(durable_steps::Uuid::from_u128(6))?,
    };
    control_request(&mut control, ControlRequest::HolderChange(takeover.clone())).await?;
    let (next_epoch, grant_deadline) =
        match control_request(&mut control, ControlRequest::WaitHolderChange(takeover)).await? {
            ControlAnswer::HolderChange(HolderChangeAnswer::Granted {
                epoch, expires_at, ..
            }) => (epoch, expires_at),
            _ => return Err("takeover failed".into()),
        };
    assert!(next_epoch > epoch);
    assert!(
        matches!(request(&mut peer, OperationRequest::Lease(LeaseRequest::Renew(renewal.clone()))).await?, OperationAnswer::Lease(LeaseAnswer::Renew(RenewAnswer::Renewed { recorded, current, replayed: true, .. })) if recorded==first_renewal && current.epoch==next_epoch && current.expires_at==grant_deadline)
    );
    assert!(
        matches!(request(&mut peer, OperationRequest::Lease(LeaseRequest::Renew(RenewRequest { attempt: RequestId::new(durable_steps::Uuid::now_v7())?, ..renewal }))).await?, OperationAnswer::Lease(LeaseAnswer::Renew(RenewAnswer::Refused(LeaseRefusal::StaleEpoch { current_epoch, epoch_ended_at }))) if current_epoch==next_epoch && epoch_ended_at==position)
    );
    let (mut revoked, statuses) = service
        .peer_with_statuses(6, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    assert!(statuses.iter().any(|status| matches!(status, DatabaseStatus::Ready { database: current_database, acked, lease } if *current_database==database && *acked==position && lease.epoch==next_epoch && lease.expires_at==grant_deadline)));
    control_request(
        &mut control,
        ControlRequest::Revoke {
            peer: PeerId::new(durable_steps::Uuid::from_u128(6))?,
        },
    )
    .await?;
    if let Some(Ok(frame)) = tokio::time::timeout(Duration::from_secs(5), revoked.receive()).await?
    {
        assert!(matches!(
            serde_json::from_slice::<ServerFrame<SampleProtocol>>(&frame)?,
            ServerFrame::Closed {
                reason: CloseReason::CredentialRejected(AuthRefusal::Revoked)
            }
        ));
    }
    assert!(service.connect("session", "Bearer peer-6").await.is_err());
    assert!(
        matches!(control_request(&mut control, ControlRequest::Renew { request: RenewRequest { attempt: RequestId::new(durable_steps::Uuid::now_v7())?, database, epoch: next_epoch }, holder: PeerId::new(durable_steps::Uuid::from_u128(6))? }).await?, ControlAnswer::Renewal(RenewAnswer::Renewed { current, .. }) if current.epoch==next_epoch)
    );
    service.kill().await?;
    drop(control);
    service = ServiceProcess::start(&service_path).await?;
    control = service
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
        _ => return Err("restarted registry preview missing".into()),
    };
    let takeover = HolderChangeRequest::Takeover {
        attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
        database,
        preview,
        to: PeerId::new(durable_steps::Uuid::from_u128(8))?,
    };
    control_request(&mut control, ControlRequest::HolderChange(takeover.clone())).await?;
    assert!(
        matches!(control_request(&mut control, ControlRequest::WaitHolderChange(takeover)).await?, ControlAnswer::HolderChange(HolderChangeAnswer::Granted { epoch, .. }) if epoch>next_epoch)
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
    service.stop(&mut control).await?;
    Ok(())
}

#[tokio::test]
async fn retirement_of_an_imported_unheld_database_refuses_new_requests_and_replays() -> TestResult
{
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start(&root.path().join("service")).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let (import, position) =
        import_backup(&service, &mut control, &root.path().join("backup.db")).await?;
    let database = import.database;
    let retired = match control_request(
        &mut control,
        ControlRequest::Retire(Retirement {
            attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
            database,
            expect_fence: Epoch::new(1)?,
            expect_acked: position,
        }),
    )
    .await?
    {
        ControlAnswer::Retirement(answer @ RetirementAnswer::Retired { .. }) => answer,
        _ => return Err("retirement failed".into()),
    };
    assert!(matches!(
        control_request(&mut control, ControlRequest::Import(import)).await?,
        ControlAnswer::Import(ImportAnswer::Retired)
    ));
    let mut peer = service
        .peer(4, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    assert!(matches!(
        request(
            &mut peer,
            OperationRequest::Lease(LeaseRequest::Claim {
                attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
                database
            })
        )
        .await?,
        OperationAnswer::Lease(LeaseAnswer::Claim(HolderChangeAnswer::Refused(
            HolderChangeRefusal::Retired
        )))
    ));
    assert!(matches!(
        request(
            &mut peer,
            OperationRequest::Lease(LeaseRequest::Renew(RenewRequest {
                attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
                database,
                epoch: Epoch::new(1)?
            }))
        )
        .await?,
        OperationAnswer::Lease(LeaseAnswer::Renew(RenewAnswer::Refused(
            LeaseRefusal::Retired
        )))
    ));
    assert!(matches!(
        request(
            &mut peer,
            OperationRequest::Lease(LeaseRequest::Register(Registration {
                attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
                database,
                writer: PeerId::new(durable_steps::Uuid::from_u128(4))?,
                origin: Origin::Empty
            }))
        )
        .await?,
        OperationAnswer::Lease(LeaseAnswer::Register(RegistrationAnswer::Refused(
            RegistrationRefusal::Retired
        )))
    ));
    assert!(matches!(
        request(
            &mut peer,
            OperationRequest::Replication(ReplicationRequest::CopyStatus { database })
        )
        .await?,
        OperationAnswer::Replication(ReplicationAnswer::CopyStatus(CopyStatusAnswer::Retired))
    ));
    assert!(matches!(
        request(
            &mut peer,
            OperationRequest::Replication(ReplicationRequest::SeedBegin {
                database,
                attempt: durable_steps::Uuid::now_v7()
            })
        )
        .await?,
        OperationAnswer::Replication(ReplicationAnswer::Refused(PieceRefusal::Retired))
    ));
    assert!(
        matches!(control_request(&mut control, ControlRequest::Retire(Retirement { attempt: RequestId::new(durable_steps::Uuid::now_v7())?, database, expect_fence: Epoch::new(1)?, expect_acked: position })).await?, ControlAnswer::Retirement(answer) if answer==retired)
    );
    service.stop(&mut control).await?;
    Ok(())
}
