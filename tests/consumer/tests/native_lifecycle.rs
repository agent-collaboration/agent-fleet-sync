#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::*;
use std::time::Duration;
use support::lifecycle_import::{assert_post_claim_imports, import_backup};
use support::*;

#[tokio::test]
async fn claimed_import_origin_survives_fresh_attempts_and_registry_restart() -> TestResult {
    let root = tempfile::tempdir()?;
    let service_path = root.path().join("service");
    let mut service = ServiceProcess::start(&service_path).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let (import, _) = import_backup(&service, &mut control, &root.path().join("backup.db")).await?;
    let mut target = service
        .peer(4, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let attempt = RequestId::new(durable_steps::Uuid::now_v7())?;
    let transfer = TransferId::new(attempt.as_uuid())?;
    request(
        &mut target,
        OperationRequest::Lease(LeaseRequest::Claim {
            attempt,
            database: import.database,
        }),
    )
    .await?;
    let change = HolderChangeRequest::Claim {
        attempt: transfer,
        database: import.database,
        claimant: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000004,
        ))?,
    };
    assert!(matches!(
        control_request(&mut control, ControlRequest::WaitHolderChange(change)).await?,
        ControlAnswer::HolderChange(HolderChangeAnswer::Granted { .. })
    ));
    assert_post_claim_imports(&mut control, &import, "before registry restart").await?;
    service.kill().await?;
    drop(control);
    service = ServiceProcess::start(&service_path).await?;
    control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    assert_post_claim_imports(&mut control, &import, "after registry restart").await?;
    service.stop(&mut control).await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_claim_dispatches_terminal_settlement_before_the_reservation_answers()
-> TestResult {
    use fleet_sync_lease::{CancelAnswer, CancelHolderChange, CancelReason, HolderChangePhase};
    let root = tempfile::tempdir()?;
    let service_path = root.path().join("service");
    let mut service = ServiceProcess::start(&service_path).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let (import, _) = import_backup(&service, &mut control, &root.path().join("backup.db")).await?;
    let database = import.database;
    control_request(&mut control, ControlRequest::HoldNextClaimReserve).await?;
    let session = SessionId::new(durable_steps::Uuid::now_v7())?;
    let mut peer = service.peer(4, session).await?;
    let attempt = RequestId::new(durable_steps::Uuid::now_v7())?;
    let transfer = TransferId::new(attempt.as_uuid())?;
    let claim = LeaseRequest::Claim { attempt, database };
    assert!(
        matches!(request(&mut peer, OperationRequest::Lease(claim.clone())).await?, OperationAnswer::Lease(LeaseAnswer::Claim(HolderChangeAnswer::Pending { transfer: current })) if current==transfer)
    );
    let reserve_call = match control_request(&mut control, ControlRequest::WaitClaimReserve).await?
    {
        ControlAnswer::ClaimReserveCall(call) => call,
        _ => return Err("consumer reserve was not dispatched".into()),
    };
    let pending = match control_request(
        &mut control,
        ControlRequest::HolderChangeView { attempt: transfer },
    )
    .await?
    {
        ControlAnswer::HolderChangeView(Some(view)) => view,
        _ => return Err("pending claim view missing".into()),
    };
    assert_eq!(pending.phase, HolderChangePhase::Requested);
    assert!(pending.outcome.is_none());
    assert!(!pending.settlement_owed);
    let by = ActorRef::new("claim-cancel-admin")?;
    assert!(matches!(
        control_request(
            &mut control,
            ControlRequest::CancelHolderChange(CancelHolderChange {
                attempt: transfer,
                by: by.clone(),
                reason: CancelReason::new("cancel the unanswered reservation")?,
            })
        )
        .await?,
        ControlAnswer::Cancel(CancelAnswer::Accepted {
            phase: HolderChangePhase::Requested,
            ..
        })
    ));
    let receipt = match control_request(
        &mut control,
        ControlRequest::WaitClaimSettlement {
            attempt: transfer,
            count: 1,
        },
    )
    .await?
    {
        ControlAnswer::ClaimReceipt(receipt) => receipt,
        _ => return Err("cancelled settlement was not dispatched".into()),
    };
    let settlement = receipt.settlement.ok_or("cancelled settlement missing")?;
    assert_eq!(settlement.outcome, SettleOutcome::Cancelled);
    assert_ne!(settlement.call, reserve_call);
    assert!(receipt.reserved_by.is_none());
    assert_eq!(receipt.reserve_attempts, 0);
    assert!(
        matches!(receipt.request, HolderChangeRequest::Claim { attempt: current, database: current_database, claimant } if current==transfer && current_database==database && claimant==PeerId::new(durable_steps::Uuid::from_u128(0x019eef00000070008000000000000004))?)
    );
    let ended = match control_request(
        &mut control,
        ControlRequest::WaitHolderChangeSettled { attempt: transfer },
    )
    .await?
    {
        ControlAnswer::HolderChangeView(Some(view)) => view,
        _ => return Err("cancelled claim did not reconcile".into()),
    };
    assert!(
        matches!(ended.outcome, Some(HolderChangeAnswer::Cancelled { by: ref actual }) if *actual==by)
    );
    assert!(!ended.settlement_owed);
    assert!(ended.follow_up_failure.is_none());
    assert!(
        matches!(request(&mut peer, OperationRequest::Lease(claim.clone())).await?, OperationAnswer::Lease(LeaseAnswer::Claim(HolderChangeAnswer::Cancelled { by: actual })) if actual==by)
    );
    // The actual reserve call is still unanswered when this process dies.
    service.kill().await?;
    drop(control);
    service = ServiceProcess::start(&service_path).await?;
    control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    peer = service.peer(4, session).await?;
    assert!(
        matches!(request(&mut peer, OperationRequest::Lease(claim)).await?, OperationAnswer::Lease(LeaseAnswer::Claim(HolderChangeAnswer::Cancelled { by: actual })) if actual==by)
    );
    let restored = match control_request(
        &mut control,
        ControlRequest::HolderChangeView { attempt: transfer },
    )
    .await?
    {
        ControlAnswer::HolderChangeView(Some(view)) => view,
        _ => return Err("cancelled claim did not survive restart".into()),
    };
    assert_eq!(restored.request, ended.request);
    assert_eq!(restored.requested_by, ended.requested_by);
    assert_eq!(restored.phase, ended.phase);
    assert_eq!(restored.history, ended.history);
    assert_eq!(restored.cancel, ended.cancel);
    assert_eq!(restored.outcome, ended.outcome);
    assert!(!restored.settlement_owed);
    let receipts = match control_request(&mut control, ControlRequest::ClaimReceipts).await? {
        ControlAnswer::ClaimReceipts(receipts) => receipts,
        _ => return Err("consumer receipts missing".into()),
    };
    assert_eq!(receipts.len(), 1);
    assert!(
        matches!(receipts.first(), Some(saved) if saved.settlement.as_ref()==Some(&settlement) && saved.reserved_by.is_none())
    );
    let change = HolderChangeRequest::Claim {
        attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
        database,
        claimant: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000004,
        ))?,
    };
    control_request(&mut control, ControlRequest::HolderChange(change.clone())).await?;
    let answer =
        match control_request(&mut control, ControlRequest::WaitHolderChange(change)).await? {
            ControlAnswer::HolderChange(answer) => answer,
            _ => return Err("next claim answer missing".into()),
        };
    // The cancelled attempt reserved epoch 2 before its external call;
    // registry epochs are never reused after cancellation.
    assert!(
        matches!(answer, HolderChangeAnswer::Granted { epoch, .. } if epoch==Epoch::new(3)?),
        "{answer:?}"
    );
    service.stop(&mut control).await?;
    Ok(())
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
    let _capability_takeover = service
        .peer(6, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let takeover = HolderChangeRequest::Takeover {
        reason: fleet_sync_protocol::HolderChangeReason::new("exercise holder change")?,
        attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
        database,
        preview,
        to: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000006,
        ))?,
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
            peer: PeerId::new(durable_steps::Uuid::from_u128(
                0x019eef00000070008000000000000006,
            ))?,
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
        matches!(control_request(&mut control, ControlRequest::Renew { request: RenewRequest { attempt: RequestId::new(durable_steps::Uuid::now_v7())?, database, epoch: next_epoch }, holder: PeerId::new(durable_steps::Uuid::from_u128(0x019eef00000070008000000000000006))? }).await?, ControlAnswer::Renewal(RenewAnswer::Renewed { current, .. }) if current.epoch==next_epoch)
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
    let _capability_takeover = service
        .peer(8, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let takeover = HolderChangeRequest::Takeover {
        reason: fleet_sync_protocol::HolderChangeReason::new("exercise holder change")?,
        attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
        database,
        preview,
        to: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000008,
        ))?,
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
                writer: PeerId::new(durable_steps::Uuid::from_u128(
                    0x019eef00000070008000000000000004
                ))?,
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
                attempt: SeedAttemptId::new(durable_steps::Uuid::now_v7())?
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
