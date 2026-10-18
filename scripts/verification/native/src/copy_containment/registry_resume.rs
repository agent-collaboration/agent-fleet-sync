//! A copy-blocked claim must preserve its attempt across repair and restart.
use super::*;
use crate::{fixture_service::*, import_fixture::*, support::*};
use fleet_sync_lease::{HolderChangeView, WaitingOn};
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::{
    ControlAnswer, ControlRequest, CopyFaultRequest, SchemaControlRequest,
};
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

pub(super) async fn read_reserved_epoch(
    path: &Path,
    attempt: TransferId,
) -> TestResult<Option<Epoch>> {
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
    match control_request(
        control,
        ControlRequest::CopyFault(CopyFaultRequest::WaitHolderChange { attempt }),
    )
    .await?
    {
        ControlAnswer::HolderChangeView(Some(view)) => Ok(view),
        _ => Err("committed copy wait view missing".into()),
    }
}

pub(super) fn typed_pending(view: &HolderChangeView) -> bool {
    view.outcome.is_none()
        && view.waiting.as_ref().is_some_and(|waiting| {
            matches!(waiting.on, WaitingOn::Copy(_))
                && waiting.reach == durable_steps::storage::CallReach::NoEffect
                && matches!(
                    waiting.copy_unavailable,
                    Some(CopyUnavailableReason::Damaged { .. })
                )
        })
}

pub(super) async fn copy_journal(
    path: &Path,
    operation: durable_steps::OperationId,
    target: &str,
) -> TestResult<Value> {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .connect()
        .await?;
    let rows=query::<Turso>("SELECT kind,intent,may_have_reached FROM _fleet_sync_steps WHERE operation_id=? AND intent LIKE ? ORDER BY position")
        .bind(operation.as_uuid().to_string()).bind(format!("%{target}%")).fetch_all(&mut connection).await?;
    let mut states = Vec::new();
    for row in rows {
        let intent: String = row.try_get("intent")?;
        states.push(json!({"kind":row.try_get::<String,_>("kind")?,"intent":serde_json::from_str::<Value>(&intent)?,"mayHaveReached":row.try_get::<i64,_>("may_have_reached")?}));
    }
    connection.close().await?;
    Ok(json!(states))
}

pub(super) fn no_effect_journal(states: &Value) -> bool {
    let Some(states) = states.as_array() else {
        return false;
    };
    states.len() == 1
        && states.iter().all(|state| {
            let reason = &state["intent"]["retry_reason"];
            state["kind"] == "call_pending"
                && state["intent"]["reach"] == "no_effect"
                && state["mayHaveReached"] == 1
                && reason["code"]
                    .as_str()
                    .zip(reason["detail"].as_str())
                    .is_some_and(|(code, detail)| {
                        matches!(
                            CopyUnavailableReason::decode_retry(code, detail),
                            Ok(CopyUnavailableReason::Damaged { .. })
                        )
                    })
        })
}

pub(super) async fn observe(root: &Path, observations: &mut Observations) -> TestResult {
    let receiver = new_receiver().await?;
    let service_root = root.join("registry-service");
    let claimant = new_peer().await?;
    let service = ServiceProcess::start_as(&service_root, receiver.as_uuid().as_u128()).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let imported = publish_unheld(&service, &mut control, &root.join("registry-backup.db")).await;
    service.stop(&mut control).await?;
    let (database, _) = imported?;
    let copy_file = copy_path(&service_root.join("service"), receiver, database);
    let marker = read_marker(&copy_file).await?;
    write_record(&copy_file, "{damaged-registry-copy").await?;

    let mut service = ServiceProcess::start_as(&service_root, receiver.as_uuid().as_u128()).await?;
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
        schema_request(&mut control,SchemaControlRequest::HoldRegistryDrive).await?;
        Ok((view, receipt))
    }.await;
    socket.close(CloseReason::ClientClosed).await;
    service.kill().await?;
    let (blocked, reservation) = driven?;
    let reserved_epoch =
        read_reserved_epoch(&service_root.join("service/registry.db"), attempt).await?;
    observations.check(
        "copy-blocked-claim-keeps-reserved-epoch",
        reserved_epoch == Some(Epoch::new(2)?),
        json!({"epoch":2}),
        json!({"epoch":reserved_epoch}),
    );
    let waiting = typed_pending(&blocked);
    observations.check(
        "copy-blocked-claim-stays-pending",
        waiting && reservation.reserved_by.is_some() && reservation.settlement.is_none(),
        json!({"attempt":attempt,"outcome":null,"reservation":"retained"}),
        json!({"view":blocked,"reservation":reservation,"reservedEpoch":reserved_epoch}),
    );

    let operation = reservation
        .reserved_by
        .as_ref()
        .ok_or("reserved operation identity missing")?
        .operation;
    let journal = copy_journal(
        &service_root.join("service/registry.db"),
        operation,
        "fleet-sync.copy-status",
    )
    .await?;
    observations.check(
        "blocked-claim-journal-is-no-effect-pending",
        no_effect_journal(&journal),
        json!({"state":"call_pending","reach":"no_effect","mayHaveReached":1,"reason":"damaged"}),
        journal.clone(),
    );
    let mut reopened =
        ServiceProcess::start_as(&service_root, receiver.as_uuid().as_u128()).await?;
    let mut reopened_control = reopened
        .connect("control", "Bearer consumer-control")
        .await?;
    let restarted = async {
        let view = wait_for_copy_or_outcome(&mut reopened_control, attempt).await?;
        schema_request(
            &mut reopened_control,
            SchemaControlRequest::HoldRegistryDrive,
        )
        .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(view)
    }
    .await;
    reopened.kill().await?;
    let restarted = restarted?;
    let reopened_journal = copy_journal(
        &service_root.join("service/registry.db"),
        operation,
        "fleet-sync.copy-status",
    )
    .await?;
    let reopened_epoch =
        read_reserved_epoch(&service_root.join("service/registry.db"), attempt).await?;
    observations.check(
        "blocked-claim-reopens-with-typed-wait",
        typed_pending(&restarted)
            && reopened_journal == journal
            && reopened_epoch == reserved_epoch,
        json!({"epoch":reserved_epoch,"journal":journal}),
        json!({"view":restarted,"epoch":reopened_epoch,"journal":reopened_journal}),
    );
    write_record(&copy_file, &marker).await?;
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
        control_request(&mut control,ControlRequest::WaitHolderChangeSettled {attempt}).await?;
        let receipts = match control_request(&mut control, ControlRequest::ClaimReceipts).await? {
            ControlAnswer::ClaimReceipts(receipts) => receipts,
            _ => return Err("repaired claim reservation receipts missing".into()),
        };
        let repaired_reservation = receipts.into_iter().find(|receipt| matches!(receipt.request, HolderChangeRequest::Claim { attempt: current, .. } if current == attempt)).ok_or("repaired claim reservation missing")?;
        observations.check("repaired-claim-keeps-reservation-identity", reservation.reserved_by.is_some() && repaired_reservation.reserved_by == reservation.reserved_by && repaired_reservation.request == reservation.request && repaired_reservation.settlement.is_some(), json!({"reservedBy":reservation.reserved_by,"request":reservation.request,"settled":true}), serde_json::to_value(repaired_reservation)?);
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
