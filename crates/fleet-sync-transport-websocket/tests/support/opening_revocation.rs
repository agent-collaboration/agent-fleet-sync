use crate::*;

#[tokio::test]
async fn revocation_closes_opening_sockets_and_prevents_late_attachment_and_work() -> TestResult {
    for with_attached_session in [false, true] {
        let temporary = tempfile::tempdir()?;
        let mut process = FixtureProcess::start(&temporary.path().join("revocation.db")).await?;
        let mut admin = connect(&process, "control", "Bearer control").await?;
        let mut attached = if with_attached_session {
            let mut socket = connect(&process, "session", "Bearer fixture").await?;
            let id = SessionId::new(Uuid::now_v7())?;
            assert_welcome(hello(&mut socket, id).await?, id, 0, 1_000);
            Some(socket)
        } else {
            None
        };
        let mut opening = connect(&process, "session", "Bearer fixture").await?;
        // The host's committed registration change is the setup-completion signal.
        let before = control(&mut admin, ControlRequest::WaitForOpening).await?;
        assert_eq!(before.sessions.len(), usize::from(with_attached_session));
        control(&mut admin, ControlRequest::Revoke).await?;
        if let Some(attached) = &mut attached {
            assert!(matches!(
                server_frame(attached).await?,
                ServerFrame::Closed {
                    reason: CloseReason::CredentialRejected(AuthRefusal::Revoked)
                }
            ));
        }

        let attempted_id = SessionId::new(Uuid::now_v7())?;
        let late_hello: ClientFrame<FixtureProtocol> = ClientFrame::Hello {
            session_id: attempted_id,
            schema_support: SchemaSupport { max_version: 1 },
        };
        let late_request: ClientFrame<FixtureProtocol> = ClientFrame::Request {
            frame: FrameId::new(Uuid::now_v7())?,
            body: OperationRequest::Consumer(Increment {
                operation: RequestId::new(Uuid::now_v7())?,
                amount: 7,
            }),
        };
        // Either a closed transport rejects these writes or the host must ignore them.
        let _ = opening.send(serde_json::to_vec(&late_hello)?.into()).await;
        let _ = opening
            .send(serde_json::to_vec(&late_request)?.into())
            .await;
        let incoming = timeout(Duration::from_secs(5), opening.receive()).await?;
        let mut saw_revoked_close = true;
        let mut observed = "transport closed";
        if let Some(Ok(bytes)) = incoming {
            match serde_json::from_slice::<ServerFrame<FixtureProtocol>>(&bytes)? {
                ServerFrame::Closed {
                    reason: CloseReason::CredentialRejected(AuthRefusal::Revoked),
                } => {
                    saw_revoked_close = matches!(
                        timeout(Duration::from_secs(5), opening.receive()).await?,
                        None | Some(Err(_))
                    );
                }
                ServerFrame::Welcome { .. } => {
                    observed = "Welcome after revocation";
                    saw_revoked_close = false;
                    // Await the late request's reply before reading its durable effect.
                    let _ = server_frame(&mut opening).await?;
                }
                _ => {
                    observed = "unexpected frame";
                    saw_revoked_close = false;
                }
            }
        }
        let after = control(&mut admin, ControlRequest::Snapshot).await?;
        let new_connection_refused = connect(&process, "session", "Bearer fixture")
            .await
            .is_err();
        control(&mut admin, ControlRequest::Stop).await?;
        assert!(
            timeout(Duration::from_secs(10), process.child.wait())
                .await??
                .success()
        );
        println!(
            "OBS pre-Hello revocation: attached={with_attached_session}, {observed}, value={}, receipts={}, active={}",
            after.value,
            after.receipt_count,
            after
                .sessions
                .iter()
                .filter(|record| matches!(record.state, SessionState::Active { .. }))
                .count()
        );
        assert!(
            saw_revoked_close,
            "revoked Opening socket remained usable: {observed}; value={}, receipts={}",
            after.value, after.receipt_count
        );
        assert!(
            !after
                .sessions
                .iter()
                .any(|record| record.session_id == attempted_id)
        );
        assert!(
            after
                .sessions
                .iter()
                .all(|record| matches!(record.state, SessionState::Ended { .. }))
        );
        assert_eq!((after.value, after.receipt_count), (0, 0));
        assert!(new_connection_refused);
    }
    Ok(())
}
