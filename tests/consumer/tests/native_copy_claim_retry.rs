#[path = "support/copy_fixture.rs"]
#[allow(dead_code)]
mod fixture;
#[allow(dead_code, unused_imports)]
mod support;
use fixture::{copy_file_path, read_copy_snapshot, replace_marker};
use fleet_sync_lease::{HolderChangeView, WaitingOn};
use fleet_sync_protocol::*;
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::*;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::path::Path;
use support::{ServiceProcess, control_request, lifecycle_import::import_backup};

fn claim_attempt(change: &HolderChangeRequest) -> TestResult<TransferId> {
    match change {
        HolderChangeRequest::Claim { attempt, .. } => Ok(*attempt),
        _ => Err("expected a claim".into()),
    }
}
async fn waiting(
    control: &mut fleet_sync_transport_websocket::WebSocketConnection,
    change: &HolderChangeRequest,
) -> TestResult<HolderChangeView> {
    match control_request(
        control,
        ControlRequest::CopyFault(CopyFaultRequest::WaitHolderChange {
            attempt: claim_attempt(change)?,
        }),
    )
    .await?
    {
        ControlAnswer::HolderChangeView(Some(view)) => {
            assert_eq!(&view.request, change);
            assert!(view.outcome.is_none());
            let waiting = view.waiting.as_ref().ok_or("missing pending copy")?;
            assert!(matches!(waiting.on, WaitingOn::Copy(_)));
            assert_eq!(waiting.reach, durable_steps::storage::CallReach::NoEffect);
            assert!(matches!(
                waiting.copy_unavailable,
                Some(CopyUnavailableReason::Damaged { .. })
            ));
            Ok(view)
        }
        _ => Err("copy wait was not observed".into()),
    }
}
async fn registry_snapshot(root: &Path) -> TestResult<(String, String, String)> {
    let mut connection = TursoConnectOptions::new()
        .filename(root.join("service/registry.db"))
        .create_if_missing(false)
        .connect()
        .await?;
    let mut values = Vec::new();
    for sql in [
        "SELECT epoch AS value FROM _fleet_sync_epoch_floor",
        "SELECT intent AS value FROM _fleet_sync_transfers",
        "SELECT kind AS value FROM _fleet_sync_steps WHERE intent LIKE '%fleet-sync.copy-status%'",
    ] {
        values.push(
            query::<Turso>(sql)
                .fetch_one(&mut connection)
                .await?
                .try_get::<String, _>("value")?,
        );
    }
    connection.close().await?;
    Ok((values[0].clone(), values[1].clone(), values[2].clone()))
}

#[tokio::test]
async fn a_real_damaged_copy_keeps_the_claim_reserved_across_restart_then_grants_the_same_attempt()
-> TestResult {
    let root = tempfile::tempdir()?;
    let service_root = root.path().join("service");
    let first = ServiceProcess::start(&service_root).await?;
    let mut control = first.connect("control", "Bearer consumer-control").await?;
    let prepared = import_backup(&first, &mut control, &root.path().join("backup.db")).await;
    first.stop(&mut control).await?;
    let (import, position) = prepared?;
    let path = copy_file_path(&service_root, import.database)?;
    let before = read_copy_snapshot(&path).await?;
    let original = before.record.as_deref().ok_or("copy marker missing")?;
    replace_marker(&path, "{damaged-copy-record").await?;
    let change = HolderChangeRequest::Claim {
        attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
        database: import.database,
        claimant: PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000004,
        ))?,
    };
    let broken = ServiceProcess::start(&service_root).await?;
    let mut control = broken.connect("control", "Bearer consumer-control").await?;
    let peer = broken
        .peer(4, SessionId::new(durable_steps::Uuid::now_v7())?)
        .await?;
    let observed: TestResult<_> = async {
        assert!(matches!(control_request(&mut control, ControlRequest::HolderChange(change.clone())).await?, ControlAnswer::HolderChange(HolderChangeAnswer::Pending { transfer }) if transfer == claim_attempt(&change)?));
        let view = waiting(&mut control, &change).await?;
        let receipts = match control_request(&mut control, ControlRequest::ClaimReceipts).await? {
            ControlAnswer::ClaimReceipts(receipts) => receipts,
            _ => return Err("reservation receipts missing".into()),
        };
        assert_eq!(receipts.len(), 1);
        assert!(receipts[0].settlement.is_none());
        Ok((view, receipts))
    }.await;
    peer.close(CloseReason::ClientClosed).await;
    broken.stop(&mut control).await?;
    let (before_view, receipts) = observed?;
    let persisted = registry_snapshot(&service_root).await?;
    assert_eq!(persisted.0, "2");
    assert_eq!(persisted.2, "call_pending");

    let reopened = ServiceProcess::start(&service_root).await?;
    let mut control = reopened
        .connect("control", "Bearer consumer-control")
        .await?;
    let resumed: TestResult = async {
        let view = waiting(&mut control, &change).await?;
        assert_eq!(view.phase, before_view.phase);
        assert!(matches!(control_request(&mut control, ControlRequest::ClaimReceipts).await?, ControlAnswer::ClaimReceipts(current) if current == receipts));
        Ok(())
    }.await;
    reopened.stop(&mut control).await?;
    resumed?;
    assert_eq!(registry_snapshot(&service_root).await?, persisted);
    replace_marker(&path, original).await?;

    let repaired = ServiceProcess::start(&service_root).await?;
    let mut control = repaired
        .connect("control", "Bearer consumer-control")
        .await?;
    let granted: TestResult = async {
        let answer = match control_request(&mut control, ControlRequest::WaitHolderChange(change.clone())).await? {
            ControlAnswer::HolderChange(answer) => answer,
            _ => return Err("repaired claim did not return an answer".into()),
        };
        assert!(matches!(answer, HolderChangeAnswer::Granted { epoch, start, .. } if epoch == Epoch::new(2)? && start == position));
        assert!(matches!(control_request(&mut control, ControlRequest::HolderChange(change.clone())).await?, ControlAnswer::HolderChange(replay) if replay == answer));
        control_request(&mut control, ControlRequest::WaitHolderChangeSettled { attempt: claim_attempt(&change)? }).await?;
        match control_request(&mut control, ControlRequest::ClaimReceipts).await? {
            ControlAnswer::ClaimReceipts(current) => {
                assert_eq!(current.len(), 1);
                assert_eq!(current[0].reserved_by, receipts[0].reserved_by);
                assert!(current[0].settlement.is_some());
            }
            _ => return Err("settled reservation missing".into()),
        }
        Ok(())
    }.await;
    repaired.stop(&mut control).await?;
    granted?;
    let after = read_copy_snapshot(&path).await?;
    assert_eq!(after.rows, before.rows);
    assert_eq!(after.hooks, before.hooks);
    assert_eq!(after.receipts, before.receipts);
    assert_eq!(after.fences.len(), before.fences.len() + 1);
    assert_eq!(
        serde_json::from_str::<TransferId>(&after.fences[0].0)?,
        claim_attempt(&change)?
    );
    Ok(())
}
