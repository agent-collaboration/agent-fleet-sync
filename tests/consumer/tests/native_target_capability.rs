#[allow(dead_code, unused_imports)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::*;
use support::{lifecycle_import::import_backup, *};

#[tokio::test]
async fn claim_uses_the_targets_accepted_support_and_replays_its_frozen_refusal() -> TestResult {
    let root = tempfile::tempdir()?;
    let service = ServiceProcess::start(&root.path().join("service")).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let (import, position) =
        import_backup(&service, &mut control, &root.path().join("backup.db")).await?;
    let store = match control_request(
        &mut control,
        ControlRequest::CopyStatus {
            database: import.database,
        },
    )
    .await?
    {
        ControlAnswer::CopyStatus(CopyStatusAnswer::Status(status)) => status,
        _ => return Err("imported copy status missing".into()),
    };
    assert!(store.schema.version > 0);
    let session = SessionId::new(durable_steps::Uuid::now_v7())?;
    let mut target = service.connect("session", "Bearer peer-4").await?;
    target
        .send(
            serde_json::to_vec(&ClientFrame::<SampleProtocol>::Hello {
                session_id: session,
                schema_support: SchemaSupport { max_version: 0 },
            })?
            .into(),
        )
        .await?;
    assert!(
        matches!(server_frame(&mut target).await?, ServerFrame::Welcome { session_id, .. } if session_id == session)
    );
    let attempt = RequestId::new(durable_steps::Uuid::now_v7())?;
    let transfer = TransferId::new(attempt.as_uuid())?;
    let claim = HolderChangeRequest::Claim {
        attempt: transfer,
        database: import.database,
        claimant: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000004,
        ))?,
    };
    request(
        &mut target,
        OperationRequest::Lease(LeaseRequest::Claim {
            attempt,
            database: import.database,
        }),
    )
    .await?;
    let expected = HolderChangeAnswer::Refused(HolderChangeRefusal::SchemaTooNew {
        store: store.schema.clone(),
        supported: Some(0),
    });
    let answer = match control_request(
        &mut control,
        ControlRequest::WaitHolderChange(claim.clone()),
    )
    .await?
    {
        ControlAnswer::HolderChange(answer) => answer,
        _ => return Err("claim answer missing".into()),
    };
    assert_eq!(answer, expected);
    let unchanged = match control_request(
        &mut control,
        ControlRequest::CopyStatus {
            database: import.database,
        },
    )
    .await?
    {
        ControlAnswer::CopyStatus(CopyStatusAnswer::Status(status)) => status,
        _ => return Err("copy disappeared after refusal".into()),
    };
    assert_eq!(unchanged, store);
    target.close(CloseReason::ClientClosed).await;
    let mut target = service.peer(4, session).await?;
    assert!(
        matches!(request(&mut target, OperationRequest::Lease(LeaseRequest::Claim { attempt, database: import.database })).await?, OperationAnswer::Lease(LeaseAnswer::Claim(answer)) if answer == expected)
    );
    let fresh = HolderChangeRequest::Claim {
        attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
        database: import.database,
        claimant: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000004,
        ))?,
    };
    control_request(&mut control, ControlRequest::HolderChange(fresh.clone())).await?;
    assert!(
        matches!(control_request(&mut control, ControlRequest::WaitHolderChange(fresh)).await?, ControlAnswer::HolderChange(HolderChangeAnswer::Granted { epoch, start, .. }) if epoch == Epoch::new(3)? && start == position)
    );
    service.stop(&mut control).await?;
    Ok(())
}
