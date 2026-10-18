//! A copy-blocked claim must preserve its attempt across repair and restart.
use super::*;
use crate::{fixture_service::*, import_fixture::*, support::*};
use fleet_sync_lease::{HolderChangeView, WaitingOn};
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::{ControlAnswer, ControlRequest};
use fleet_sync_transport_websocket::WebSocketConnection;

async fn read_marker(path: &Path) -> TestResult<String> {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .connect()
        .await?;
    let row = query::<Turso>("SELECT record FROM _fleet_sync_copy WHERE singleton=1")
        .fetch_one(&mut connection)
        .await?;
    let record = row.try_get("record")?;
    connection.close().await?;
    Ok(record)
}

async fn remove_marker(path: &Path) -> TestResult {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .connect()
        .await?;
    let deleted = query::<Turso>("DELETE FROM _fleet_sync_copy WHERE singleton=1")
        .execute(&mut connection)
        .await?;
    connection.close().await?;
    if deleted.rows_affected() != 1 {
        return Err("copy fault did not remove exactly one marker".into());
    }
    Ok(())
}

async fn restore_marker(path: &Path, marker: &str) -> TestResult {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .connect()
        .await?;
    let inserted = query::<Turso>("INSERT INTO _fleet_sync_copy(singleton,record) VALUES(1,?)")
        .bind(marker.to_owned())
        .execute(&mut connection)
        .await?;
    connection.close().await?;
    if inserted.rows_affected() != 1 {
        return Err("copy repair did not restore exactly one marker".into());
    }
    Ok(())
}

async fn read_reserved_epoch(path: &Path, attempt: TransferId) -> TestResult<Option<Epoch>> {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .connect()
        .await?;
    let row = query::<Turso>("SELECT intent FROM _fleet_sync_transfers WHERE attempt=?")
        .bind(attempt.as_uuid().to_string())
        .fetch_one(&mut connection)
        .await?;
    let intent: String = row.try_get("intent")?;
    // RegistryStorage::save_transfer writes only new_epoch into this column.
    let epoch = serde_json::from_str(&intent)?;
    connection.close().await?;
    Ok(epoch)
}

async fn wait_for_copy_or_outcome(
    control: &mut WebSocketConnection,
    attempt: TransferId,
) -> TestResult<HolderChangeView> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let view = match control_request(control, ControlRequest::HolderChangeView { attempt })
                .await?
            {
                ControlAnswer::HolderChangeView(Some(view)) => view,
                _ => return Err("claim view missing".into()),
            };
            if view.outcome.is_some()
                || view.waiting.as_ref().is_some_and(|waiting| {
                    matches!(waiting.on, WaitingOn::Copy(_))
                        && waiting.attempt > 0
                        && waiting.next_attempt_at > SystemClock.now()
                })
            {
                return Ok(view);
            }
        }
    })
    .await?
}

pub(super) async fn observe(root: &Path, observations: &mut Observations) -> TestResult {
    let receiver = ReceiverId::new(durable_steps::Uuid::now_v7())?;
    let service_root = root.join("registry-service");
    let claimant = new_peer()?;
    let service = ServiceProcess::start_as(&service_root, receiver.as_uuid().as_u128()).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let imported = publish_unheld(&service, &mut control, &root.join("registry-backup.db")).await;
    service.stop(&mut control).await?;
    let (database, _) = imported?;
    let copy_file = copy_path(&service_root.join("service"), receiver, database);
    let marker = read_marker(&copy_file).await?;
    remove_marker(&copy_file).await?;

    let service = ServiceProcess::start_as(&service_root, receiver.as_uuid().as_u128()).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut socket = attach_peer(&service, claimant, 5).await?;
    let request_id = RequestId::new(durable_steps::Uuid::now_v7())?;
    let attempt = TransferId::new(request_id.as_uuid())?;
    let driven: TestResult<_> = async {
        request(&mut socket, OperationRequest::Lease(LeaseRequest::Claim { attempt: request_id, database })).await?;
        let view = wait_for_copy_or_outcome(&mut control, attempt).await?;
        let receipts = match control_request(&mut control, ControlRequest::ClaimReceipts).await? {
            ControlAnswer::ClaimReceipts(receipts) => receipts,
            _ => return Err("claim reservation receipts missing".into()),
        };
        let receipt = receipts.into_iter().find(|receipt| matches!(receipt.request, HolderChangeRequest::Claim { attempt: current, .. } if current == attempt)).ok_or("claim reservation receipt missing")?;
        Ok((view, receipt))
    }.await;
    socket.close(CloseReason::ClientClosed).await;
    service.stop(&mut control).await?;
    let (blocked, reservation) = driven?;
    let reserved_epoch =
        read_reserved_epoch(&service_root.join("service/registry.db"), attempt).await?;
    observations.check(
        "copy-blocked-claim-keeps-reserved-epoch",
        reserved_epoch == Some(Epoch::new(2)?),
        json!({"epoch":2}),
        json!({"epoch":reserved_epoch}),
    );
    let waiting = blocked.outcome.is_none()
        && blocked
            .waiting
            .as_ref()
            .is_some_and(|waiting| matches!(waiting.on, WaitingOn::Copy(_)));
    observations.check(
        "copy-blocked-claim-stays-pending",
        waiting && reservation.reserved_by.is_some() && reservation.settlement.is_none(),
        json!({"attempt":attempt,"outcome":null,"reservation":"retained"}),
        json!({"view":blocked,"reservation":reservation,"reservedEpoch":reserved_epoch}),
    );

    restore_marker(&copy_file, &marker).await?;
    if read_marker(&copy_file).await? != marker {
        return Err("registry copy repair readback differs".into());
    }
    let service = ServiceProcess::start_as(&service_root, receiver.as_uuid().as_u128()).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut socket = attach_peer(&service, claimant, 5).await?;
    let resumed: TestResult<_> = async {
        let answer = claim(&mut socket, &mut control, claimant, database, request_id).await?;
        let keeps_epoch = matches!(&answer, HolderChangeAnswer::Granted { epoch, .. } if Some(*epoch) == reserved_epoch);
        observations.check("repaired-claim-resumes-original-attempt", keeps_epoch, json!({"attempt":attempt,"grantedEpoch":reserved_epoch}), serde_json::to_value(answer)?);
        let receipts = match control_request(&mut control, ControlRequest::ClaimReceipts).await? {
            ControlAnswer::ClaimReceipts(receipts) => receipts,
            _ => return Err("repaired claim reservation receipts missing".into()),
        };
        let repaired_reservation = receipts.into_iter().find(|receipt| matches!(receipt.request, HolderChangeRequest::Claim { attempt: current, .. } if current == attempt)).ok_or("repaired claim reservation missing")?;
        observations.check("repaired-claim-keeps-reservation-identity", reservation.reserved_by.is_some() && repaired_reservation.reserved_by == reservation.reserved_by && repaired_reservation.request == reservation.request, json!({"reservedBy":reservation.reserved_by,"request":reservation.request}), serde_json::to_value(repaired_reservation)?);
        let (control_database, _) = publish_unheld(&service, &mut control, &root.join("registry-control.db")).await?;
        let fresh = claim(&mut socket, &mut control, claimant, control_database, RequestId::new(durable_steps::Uuid::now_v7())?).await?;
        observations.check("registry-repair-readiness-control", matches!(fresh, HolderChangeAnswer::Granted { .. }), json!({"kind":"granted"}), serde_json::to_value(fresh)?);
        let refusal_id = RequestId::new(durable_steps::Uuid::now_v7())?;
        let refusal = claim(&mut socket, &mut control, claimant, control_database, refusal_id).await?;
        Ok((control_database, refusal_id, refusal))
    }.await;
    socket.close(CloseReason::ClientClosed).await;
    service.stop(&mut control).await?;
    let (control_database, refusal_id, refusal) = resumed?;

    let service = ServiceProcess::start_as(&service_root, receiver.as_uuid().as_u128()).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut socket = attach_peer(&service, claimant, 5).await?;
    let repeated = claim(
        &mut socket,
        &mut control,
        claimant,
        control_database,
        refusal_id,
    )
    .await;
    socket.close(CloseReason::ClientClosed).await;
    service.stop(&mut control).await?;
    let repeated = repeated?;
    let final_refusal = serde_json::to_value(&refusal)?;
    observations.check(
        "final-refusal-stays-final-across-restart",
        final_refusal["kind"] == "refused"
            && final_refusal["value"]["kind"] == "alreadyHeld"
            && repeated == refusal,
        json!({"recorded":final_refusal}),
        serde_json::to_value(repeated)?,
    );
    Ok(())
}
