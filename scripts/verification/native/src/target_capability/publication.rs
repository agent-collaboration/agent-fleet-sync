//! Observe publication windows on real sockets and the native transaction owner.
use super::controls::*;

async fn barrier(root: &Path, observations: &mut Observations) -> TestResult {
    let path = root.join("publication-barrier");
    let service = start_service(&path).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let result: TestResult = async {
        let target = new_peer().await?;
        let session = new_session()?;
        let mut old = hello(&service, target, session, 5).await?;
        welcome(&mut old, session).await?;
        schema_request(
            &mut control,
            SchemaControlRequest::HoldReport {
                peer: target,
                attach_seq: AttachSequence::new(2)?,
            },
        )
        .await?;
        let mut next = hello(&service, target, session, 4).await?;
        let report = held_report(&mut control).await?;
        let before = projection(&mut control, target).await?;
        let pending = snapshot(&mut control, &report).await?;
        let connection = attaching(&pending, &report)?;
        let no_welcome = quiet(&mut next).await?;
        observations.check(
            "held-new-report-before-publication",
            no_welcome
                && report.support.max_version == 4
                && before
                    .as_ref()
                    .is_some_and(|r| r.support.max_version == 5 && r.attach_seq.value() == 1),
            json!({"welcome":false,"accepted":5,"pending":4}),
            json!({"quiet":no_welcome,"accepted":before,"report":report,"snapshot":pending}),
        );
        let (database, _) = publish_unheld(&service, &mut control, &path.join("before.db")).await?;
        let before_change = claim_change(database, target)?;
        admit(&mut control, &before_change).await?;
        let before_schema = frozen(&mut control, &before_change).await?;
        let before_answer = outcome(&mut control, &before_change).await?;
        observations.check(
            "admin-admitted-before-new-report",
            before_schema == Some(SchemaSupport { max_version: 5 })
                && matches!(before_answer, HolderChangeAnswer::Granted { .. }),
            json!({"admitted":5,"kind":"granted"}),
            json!({"admitted":before_schema,"answer":before_answer}),
        );
        schema_request(&mut control, SchemaControlRequest::ReleaseReport).await?;
        welcome(&mut next, session).await?;
        schema_request(
            &mut control,
            SchemaControlRequest::WaitPublication(report.clone()),
        )
        .await?;
        let accepted = projection(&mut control, target).await?;
        let after = snapshot(&mut control, &report).await?;
        observations.check(
            "publication-commits-registration-and-welcome",
            accepted == Some(report.clone())
                && after.registrations.iter().any(|(id, r)| {
                    *id == connection
                        && *r
                            == ConnectionRegistration::Attached {
                                session_id: session,
                                schema_support: SchemaSupport { max_version: 4 },
                            }
                }),
            json!({"accepted":report,"registration":"attached"}),
            json!({"accepted":accepted,"snapshot":after}),
        );
        for own in [false, true] {
            let (database, _) = publish_unheld(
                &service,
                &mut control,
                &path.join(format!("after-{own}.db")),
            )
            .await?;
            let change = claim_change(database, target)?;
            let answer = if own {
                claim(
                    &mut next,
                    &mut control,
                    target,
                    database,
                    RequestId::new(attempt(&change).as_uuid())?,
                )
                .await?
            } else {
                admit(&mut control, &change).await?;
                outcome(&mut control, &change).await?
            };
            let schema = frozen(&mut control, &change).await?;
            observations.check(
                if own {
                    "own-claim-after-welcome-freezes-four"
                } else {
                    "admin-after-publication-freezes-four"
                },
                is_too_new(&answer, 4) && schema == Some(SchemaSupport { max_version: 4 }),
                json!({"kind":"schemaTooNew","store":5,"supported":4}),
                json!({"answer":answer,"admitted":schema}),
            );
        }
        old.close(CloseReason::ClientClosed).await;
        next.close(CloseReason::ClientClosed).await;
        Ok(())
    }
    .await;
    service.stop(&mut control).await?;
    result
}

async fn closed_or_replaced(
    root: &Path,
    observations: &mut Observations,
    replace: bool,
) -> TestResult {
    let path = root.join(if replace {
        "old-report-late"
    } else {
        "closed-publication"
    });
    let receiver = new_receiver().await?;
    let mut service = ServiceProcess::start_as(&path, receiver.as_uuid().as_u128()).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let result: TestResult = async {
        let target = new_peer().await?;
        let session = new_session()?;
        schema_request(
            &mut control,
            SchemaControlRequest::HoldReport {
                peer: target,
                attach_seq: AttachSequence::new(1)?,
            },
        )
        .await?;
        let mut first = hello(&service, target, session, 5).await?;
        let old = held_report(&mut control).await?;
        let connection = attaching(&snapshot(&mut control, &old).await?, &old)?;
        let no_early = quiet(&mut first).await?;
        let mut replacement = if replace {
            let mut next = hello(&service, target, session, 4).await?;
            let closed = server_frame(&mut first).await?;
            observations.check(
                "replacement-closes-old-pending-connection",
                matches!(
                    closed,
                    ServerFrame::Closed {
                        reason: CloseReason::ConnectionReplaced
                    }
                ),
                json!({"close":"connectionReplaced"}),
                json!(closed),
            );
            welcome(&mut next, session).await?;
            Some(next)
        } else {
            // Other input progresses while the first report owns its hold.
            let other = new_peer().await?;
            let mut opening = service
                .connect(
                    "session",
                    &format!("Bearer peer-{}", other.as_uuid().as_u128()),
                )
                .await?;
            control_request(&mut control, ControlRequest::WaitOpening { peer: other }).await?;
            control_request(&mut control, ControlRequest::Revoke { peer: other }).await?;
            let closed = server_frame(&mut opening).await?;
            observations.check(
                "held-report-does-not-block-other-input",
                matches!(
                    closed,
                    ServerFrame::Closed {
                        reason: CloseReason::CredentialRejected(AuthRefusal::Revoked)
                    }
                ),
                json!({"close":"revoked"}),
                json!(closed),
            );
            opening.close(CloseReason::ClientClosed).await;
            let frame: ClientFrame<SampleProtocol> = ClientFrame::Request {
                frame: FrameId::new(durable_steps::Uuid::now_v7())?,
                body: OperationRequest::Consumer(fleet_sync_test_consumer::SampleRequest::Echo {
                    text: "blocked before publication".into(),
                }),
            };
            first.send(serde_json::to_vec(&frame)?.into()).await?;
            let closed = server_frame(&mut first).await?;
            let count = control_request(&mut control, ControlRequest::RequestCount).await?;
            observations.check(
                "raw-request-before-welcome-is-not-served",
                matches!(
                    closed,
                    ServerFrame::Closed {
                        reason: CloseReason::MalformedFrame
                    }
                ) && matches!(count, ControlAnswer::RequestCount(0)),
                json!({"close":"malformedFrame","requests":0}),
                json!({"close":closed,"count":count}),
            );
            None
        };
        schema_request(
            &mut control,
            SchemaControlRequest::WaitClosed { connection },
        )
        .await?;
        let accepted_before = projection(&mut control, target).await?;
        schema_request(&mut control, SchemaControlRequest::ReleaseReport).await?;
        schema_request(
            &mut control,
            SchemaControlRequest::WaitPublication(old.clone()),
        )
        .await?;
        let state = snapshot(&mut control, &old).await?;
        let accepted = projection(&mut control, target).await?;
        let expected = if replace {
            accepted_before
        } else {
            Some(old.clone())
        };
        let ordered = accepted == expected
            && (!replace
                || accepted
                    .as_ref()
                    .is_some_and(|r| r.attach_seq.value() == 2 && r.support.max_version == 4));
        let no_extra = if let Some(next) = replacement.as_mut() {
            quiet(next).await?
        } else {
            true
        };
        observations.check(
            if replace {
                "old-report-late-keeps-newer-row"
            } else {
                "closed-attach-is-not-resurrected"
            },
            no_early
                && ordered
                && no_extra
                && state.registrations.iter().all(|(id, _)| *id != connection),
            json!({"accepted":expected,"oldConnectionPresent":false,"extraWelcome":false}),
            json!({"accepted":accepted,"snapshot":state,"quiet":no_extra}),
        );
        if let Some(next) = replacement {
            next.close(CloseReason::ClientClosed).await;
        }
        if replace {
            service.kill().await?;
            service = ServiceProcess::start_as(&path, receiver.as_uuid().as_u128()).await?;
            control = service
                .connect("control", "Bearer consumer-control")
                .await?;
            let reopened = projection(&mut control, target).await?;
            let (database, _) =
                publish_unheld(&service, &mut control, &path.join("ordering-fence.db")).await?;
            let change = claim_change(database, target)?;
            admit(&mut control, &change).await?;
            let answer = outcome(&mut control, &change).await?;
            observations.check(
                "old-report-ordering-persists-and-fences",
                reopened == accepted && is_too_new(&answer, 4),
                json!({"projection":accepted,"kind":"schemaTooNew","supported":4}),
                json!({"projection":reopened,"answer":answer}),
            );
        }
        Ok(())
    }
    .await;
    service.stop(&mut control).await?;
    result
}

async fn rollback(root: &Path, observations: &mut Observations) -> TestResult {
    let service = start_service(&root.join("finalize-rollback")).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let result: TestResult = async {
        let peer = new_peer().await?;
        let session = new_session()?;
        schema_request(&mut control, SchemaControlRequest::FailFinalize { peer }).await?;
        let mut socket = hello(&service, peer, session, 5).await?;
        let rolled =
            schema_request(&mut control, SchemaControlRequest::WaitFinalizeRollback).await?;
        let report = projection(&mut control, peer)
            .await?
            .ok_or("accepted report missing at rollback")?;
        let before = snapshot(&mut control, &report).await?;
        let connection = attaching(&before, &report)?;
        let no_welcome = quiet(&mut socket).await?;
        observations.check(
            "finalize-rollback-has-no-registration-or-welcome",
            matches!(rolled,SchemaControlAnswer::RolledBack {peer:actual} if actual==peer)
                && before.attach_steps == 0
                && no_welcome,
            json!({"attachSteps":0,"welcome":false,"registration":"attaching"}),
            json!({"snapshot":before,"quiet":no_welcome}),
        );
        schema_request(&mut control, SchemaControlRequest::ReleaseFinalize).await?;
        welcome(&mut socket, session).await?;
        schema_request(
            &mut control,
            SchemaControlRequest::WaitPublication(report.clone()),
        )
        .await?;
        let after = snapshot(&mut control, &report).await?;
        let no_extra = quiet(&mut socket).await?;
        observations.check(
            "finalize-retry-commits-once",
            after.attach_steps == 0
                && no_extra
                && after.registrations.iter().any(|(id, r)| {
                    *id == connection
                        && *r
                            == ConnectionRegistration::Attached {
                                session_id: session,
                                schema_support: SchemaSupport { max_version: 5 },
                            }
                }),
            json!({"registration":"attached","extraWelcome":false,"attachStepsAfterPruning":0}),
            json!({"snapshot":after,"quiet":no_extra}),
        );
        socket.close(CloseReason::ClientClosed).await;
        Ok(())
    }
    .await;
    service.stop(&mut control).await?;
    result
}

async fn restart_pending(root: &Path, observations: &mut Observations) -> TestResult {
    let path = root.join("publication-restart");
    let receiver = new_receiver().await?;
    let mut service = ServiceProcess::start_as(&path, receiver.as_uuid().as_u128()).await?;
    let mut control = service
        .connect("control", "Bearer consumer-control")
        .await?;
    let result:TestResult=async {
        let peer=new_peer().await?;
        let session=new_session()?;
        schema_request(&mut control,SchemaControlRequest::HoldReport {peer,attach_seq:AttachSequence::new(1)?}).await?;
        let mut first=hello(&service,peer,session,5).await?;
        let old=held_report(&mut control).await?;
        let connection=attaching(&snapshot(&mut control,&old).await?,&old)?;
        let no_early=quiet(&mut first).await?;
        service.kill().await?;
        first.close(CloseReason::ClientClosed).await;
        service=ServiceProcess::start_as(&path,receiver.as_uuid().as_u128()).await?;
        control=service.connect("control","Bearer consumer-control").await?;
        let mut next=hello(&service,peer,session,4).await?;
        welcome(&mut next,session).await?;
        schema_request(&mut control,SchemaControlRequest::WaitPublication(old.clone())).await?;
        let accepted=projection(&mut control,peer).await?;
        let state=snapshot(&mut control,&old).await?;
        let no_extra=quiet(&mut next).await?;
        observations.check("restart-delayed-publication-does-not-resurrect",no_early && no_extra && accepted.as_ref().is_some_and(|r|r.attach_seq.value()==2 && r.support.max_version==4) && state.registrations.iter().all(|(id,r)|*id!=connection || !matches!(r,ConnectionRegistration::Attached {..}|ConnectionRegistration::Attaching {..})),json!({"attachSequence":2,"support":4,"oldConnectionActive":false,"extraWelcome":false}),json!({"projection":accepted,"snapshot":state,"quiet":no_extra}));
        next.close(CloseReason::ClientClosed).await;
        Ok(())
    }.await;
    service.stop(&mut control).await?;
    result
}

pub(super) async fn run(root: &Path, observations: &mut Observations) -> TestResult {
    barrier(root, observations).await?;
    closed_or_replaced(root, observations, false).await?;
    closed_or_replaced(root, observations, true).await?;
    rollback(root, observations).await?;
    restart_pending(root, observations).await
}
