//! Freeze the target before check, including a real process restart.
use super::controls::*;

async fn move_window(root: &Path, observations: &mut Observations, restart: bool) -> TestResult {
    let path = root.join(if restart {
        "admission-restart"
    } else {
        "admission-live"
    });
    let receiver = new_receiver().await?;
    let mut service = ServiceProcess::start_as(&path, receiver.as_uuid().as_u128()).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let result: TestResult = async {
        let (database, position) =
            publish_unheld(&service, &mut control, &path.join("backup.db")).await?;
        let holder = new_peer().await?;
        let target = new_peer().await?;
        let mut holder_socket = attach_peer(&service, holder, 5).await?;
        let high = attach_peer(&service, target, 5).await?;
        let epoch = match claim(
            &mut holder_socket,
            &mut control,
            holder,
            database,
            RequestId::new(durable_steps::Uuid::now_v7())?,
        )
        .await?
        {
            HolderChangeAnswer::Granted { epoch, .. } => epoch,
            _ => return Err("admission fixture holder did not grant".into()),
        };
        schema_request(&mut control, SchemaControlRequest::HoldRegistryDrive).await?;
        let change = HolderChangeRequest::Move {
            attempt: TransferId::new(durable_steps::Uuid::now_v7())?,
            database,
            from_epoch: epoch,
            to: target,
        };
        admit(&mut control, &change).await?;
        let admitted = frozen(&mut control, &change).await?;
        let low = attach_peer(&service, target, 4).await?;
        let accepted = projection(&mut control, target).await?;
        let still_frozen = frozen(&mut control, &change).await?;
        observations.check(
            &format!("admission-freezes-before-check-{restart}"),
            admitted == Some(SchemaSupport { max_version: 5 })
                && still_frozen == admitted
                && accepted
                    .as_ref()
                    .is_some_and(|r| r.attach_seq.value() == 2 && r.support.max_version == 4),
            json!({"admitted":5,"acceptedAfterAttach":4,"attachSequence":2}),
            json!({"admitted":admitted,"afterAttach":still_frozen,"projection":accepted}),
        );
        high.close(CloseReason::ClientClosed).await;
        low.close(CloseReason::ClientClosed).await;
        if restart {
            service.kill().await?;
            holder_socket.close(CloseReason::ClientClosed).await;
            service = ServiceProcess::start_as(&path, receiver.as_uuid().as_u128()).await?;
            control = service
                .connect("control", "Bearer consumer-control")
                .await?;
            holder_socket = attach_peer(&service, holder, 5).await?;
        } else {
            schema_request(&mut control, SchemaControlRequest::ReleaseRegistryDrive).await?;
        }
        let answer = answer_move_from_holder(
            &mut control,
            &mut holder_socket,
            change.clone(),
            ReleaseAnswer::Released {
                final_position: position,
            },
        )
        .await?;
        let after = frozen(&mut control, &change).await?;
        observations.check(
            &format!("admitted-move-grants-from-frozen-five-{restart}"),
            matches!(answer, HolderChangeAnswer::Granted { start, .. } if start==position)
                && after == admitted,
            json!({"kind":"granted","start":position,"admitted":5}),
            json!({"answer":answer,"admitted":after}),
        );
        let replay = outcome(&mut control, &change).await?;
        observations.check(
            &format!("admitted-move-replay-{restart}"),
            replay == answer,
            json!(answer),
            json!(replay),
        );
        let (fresh_database, _) =
            publish_unheld(&service, &mut control, &path.join("fresh.db")).await?;
        let fresh = claim_change(fresh_database, target)?;
        admit(&mut control, &fresh).await?;
        let answer = outcome(&mut control, &fresh).await?;
        let schema = frozen(&mut control, &fresh).await?;
        observations.check(
            &format!("fresh-admission-freezes-four-{restart}"),
            is_too_new(&answer, 4) && schema == Some(SchemaSupport { max_version: 4 }),
            json!({"kind":"schemaTooNew","store":5,"supported":4}),
            json!({"answer":answer,"admitted":schema}),
        );
        holder_socket.close(CloseReason::ClientClosed).await;
        Ok(())
    }
    .await;
    // A failed case may leave a registry gate held; stopping the owned process
    // does not need its registry runner to make progress.
    service.stop(&mut control).await?;
    result
}

pub(super) async fn run(root: &Path, observations: &mut Observations) -> TestResult {
    move_window(root, observations, false).await?;
    move_window(root, observations, true).await
}
