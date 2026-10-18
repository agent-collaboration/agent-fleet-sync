//! Damage the production ack/fence call before SQL, then resume its original intent.
use super::{registry_resume::*, *};
use crate::{fixture_service::attach_peer, support::*};
use fleet_sync_session::TransportConnection;
use fleet_sync_test_consumer::{
    ControlAnswer, ControlRequest, CopyFaultRequest, SchemaControlRequest,
};

async fn fence_count(path: &Path) -> TestResult<i64> {
    let mut connection = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .connect()
        .await?;
    let row = query::<Turso>("SELECT count(*) AS count FROM _fleet_sync_fence_receipts")
        .fetch_one(&mut connection)
        .await?;
    let count = row.try_get("count")?;
    connection.close().await?;
    Ok(count)
}

async fn waiting(
    control: &mut fleet_sync_transport_websocket::WebSocketConnection,
    attempt: TransferId,
) -> TestResult<fleet_sync_lease::HolderChangeView> {
    match control_request(
        control,
        ControlRequest::CopyFault(CopyFaultRequest::WaitHolderChange { attempt }),
    )
    .await?
    {
        ControlAnswer::HolderChangeView(Some(view)) => Ok(view),
        _ => Err("move copy wait missing".into()),
    }
}

async fn case(
    root: &Path,
    observations: &mut Observations,
    kind: &str,
    step: &str,
    call_kind: &str,
) -> TestResult {
    let root = root.join(format!("move-{kind}"));
    tokio::fs::create_dir_all(&root).await?;
    let service_root = root.join("service");
    let receiver = new_receiver().await?;
    let holder = new_peer().await?;
    let target = new_peer().await?;
    let database = DatabaseId::new(durable_steps::Uuid::now_v7())?;
    let attempt = TransferId::new(durable_steps::Uuid::now_v7())?;
    let change = HolderChangeRequest::Move {
        attempt,
        database,
        from_epoch: Epoch::new(1)?,
        to: target,
    };
    let mut service = ServiceProcess::start_as(&service_root, receiver.as_uuid().as_u128()).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let mut holder_socket = attach_peer(&service, holder, 100).await?;
    let target_socket = attach_peer(&service, target, 100).await?;
    let driven: TestResult<_> = async {
        let registered = request(
            &mut holder_socket,
            OperationRequest::Lease(LeaseRequest::Register(Registration {
                attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
                database,
                writer: holder,
                origin: Origin::Empty,
            })),
        )
        .await?;
        if !matches!(
            registered,
            OperationAnswer::Lease(LeaseAnswer::Register(RegistrationAnswer::Registered { .. }))
        ) {
            return Err("move holder registration failed".into());
        }
        request(
            &mut holder_socket,
            OperationRequest::Lease(LeaseRequest::Renew(RenewRequest {
                attempt: RequestId::new(durable_steps::Uuid::now_v7())?,
                database,
                epoch: Epoch::new(1)?,
            })),
        )
        .await?;
        // CopyCallKind is an existing native control enum with this declared wire form.
        control_request(
            &mut control,
            ControlRequest::CopyFault(CopyFaultRequest::HoldCall {
                database,
                kind: serde_json::from_value(json!(kind))?,
            }),
        )
        .await?;
        control_request(&mut control, ControlRequest::HolderChange(change.clone())).await?;
        loop {
            match server_frame(&mut holder_socket).await? {
                ServerFrame::Notice { .. } => {}
                ServerFrame::Request {
                    frame,
                    body: ServerRequest::Crate(ServerCall::Release { request, .. }),
                } => {
                    if request.database != database
                        || request.transfer != attempt
                        || request.epoch != Epoch::new(1)?
                    {
                        return Err("move fixture Release correlation mismatch".into());
                    }
                    let answer: ClientFrame<SampleProtocol> = ClientFrame::Answer {
                        frame,
                        body: ClientAnswer::Crate(ServerCallAnswer::Release(
                            ReleaseAnswer::Released {
                                final_position: ReplicationPosition::new(
                                    Epoch::new(1)?,
                                    CommitSequence::new(0),
                                ),
                            },
                        )),
                    };
                    holder_socket
                        .send(serde_json::to_vec(&answer)?.into())
                        .await?;
                    break;
                }
                _ => return Err("move did not request holder release".into()),
            }
        }
        let call = match control_request(
            &mut control,
            ControlRequest::CopyFault(CopyFaultRequest::WaitCall { database }),
        )
        .await?
        {
            ControlAnswer::CopyCall(call) if call.step.as_str() == step => call,
            _ => return Err("actual copy call did not reach held step".into()),
        };
        let marker = match control_request(
            &mut control,
            ControlRequest::CopyFault(CopyFaultRequest::ReplaceMarker {
                database,
                replacement: "{damaged-at-held-copy-call".into(),
            }),
        )
        .await?
        {
            ControlAnswer::CopyMarker(marker) => marker,
            _ => return Err("actual owner did not replace marker".into()),
        };
        control_request(
            &mut control,
            ControlRequest::CopyFault(CopyFaultRequest::ReleaseCall { database }),
        )
        .await?;
        let view = waiting(&mut control, attempt).await?;
        schema_request(&mut control, SchemaControlRequest::HoldRegistryDrive).await?;
        Ok((marker, call, view))
    }
    .await;
    holder_socket.close(CloseReason::ClientClosed).await;
    target_socket.close(CloseReason::ClientClosed).await;
    service.kill().await?;
    let (marker, call, view) = driven?;
    let registry = service_root.join("service/registry.db");
    let copy = copy_path(&service_root.join("service"), receiver, database);
    let journal = copy_journal(&registry, call.operation, call_kind).await?;
    let epoch = read_reserved_epoch(&registry, attempt).await?;
    let fences = fence_count(&copy).await?;
    observations.check(
        &format!("move-{kind}-pending-no-effect"),
        typed_pending(&view)
            && no_effect_journal(&journal)
            && epoch == Some(Epoch::new(2)?)
            && fences == 0,
        json!({"attempt":attempt,"call":call,"epoch":2,"fences":0,"reach":"no_effect"}),
        json!({"view":view,"journal":journal,"epoch":epoch,"fences":fences}),
    );
    let mut reopened =
        ServiceProcess::start_as(&service_root, receiver.as_uuid().as_u128()).await?;
    let mut control = reopened
        .connect("control", "Bearer consumer-control")
        .await?;
    let retained: TestResult<_> = async {
        let view = waiting(&mut control, attempt).await?;
        schema_request(&mut control, SchemaControlRequest::HoldRegistryDrive).await?;
        Ok(view)
    }
    .await;
    reopened.kill().await?;
    let retained = retained?;
    let reopened_journal = copy_journal(&registry, call.operation, call_kind).await?;
    let reopened_epoch = read_reserved_epoch(&registry, attempt).await?;
    observations.check(
        &format!("move-{kind}-reopens-original-call"),
        typed_pending(&retained)
            && reopened_journal == journal
            && reopened_epoch == epoch
            && fence_count(&copy).await? == 0,
        json!({"call":call,"epoch":epoch,"journal":journal}),
        json!({"view":retained,"epoch":reopened_epoch,"journal":reopened_journal}),
    );
    write_record(&copy, &marker).await?;
    let repaired = ServiceProcess::start_as(&service_root, receiver.as_uuid().as_u128()).await?;
    let mut control = repaired
        .connect("control", "Bearer consumer-control")
        .await?;
    let resumed:TestResult=async {
        let granted=match control_request(&mut control,ControlRequest::WaitHolderChange(change.clone())).await? {
            ControlAnswer::HolderChange(answer)=>answer,
            _=>return Err("repaired move answer missing".into()),
        };
        let repeated=match control_request(&mut control,ControlRequest::HolderChange(change)).await? {
            ControlAnswer::HolderChange(answer)=>answer,
            _=>return Err("move replay missing".into()),
        };
        let status=control_request(&mut control,ControlRequest::CopyStatus {database}).await?;
        observations.check(&format!("move-{kind}-grants-and-replays"),matches!(granted,HolderChangeAnswer::Granted {epoch:actual,..} if Some(actual)==epoch) && repeated==granted && matches!(status,ControlAnswer::CopyStatus(CopyStatusAnswer::Status(ref copy)) if copy.fence==Epoch::new(2)?),
            json!({"epoch":epoch,"fence":2,"sameOutcome":true}),json!({"granted":granted,"repeated":repeated,"status":status}));
        Ok(())
    }.await;
    repaired.stop(&mut control).await?;
    resumed?;
    let final_fences = fence_count(&copy).await?;
    observations.check(
        &format!("move-{kind}-fences-once"),
        final_fences == 1,
        json!({"fences":1}),
        json!({"fences":final_fences}),
    );
    Ok(())
}

pub(super) async fn observe(root: &Path, observations: &mut Observations) -> TestResult {
    case(
        root,
        observations,
        "acknowledged",
        "await_acked",
        "fleet-sync.copy-acknowledged",
    )
    .await?;
    case(
        root,
        observations,
        "fence",
        "fence",
        "fleet-sync.copy-fence",
    )
    .await
}
