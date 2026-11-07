use super::*;
use crate::{SessionDropReason, SessionLinkStatus};

#[tokio::test(start_paused = true)]
async fn attached_session_ended_is_terminal_replacement_not_restart() {
    let (connection, mut server) = socket();
    let (requests, mut incoming, pump, attempts, _) = setup(vec![Ok(connection)]).await;
    let state = pump.link_state();
    let (_stop, stopped) = watch::channel(false);
    let task = tokio::spawn(
        pump.with_recovery_policy(policy())
            .run_with_clock(stopped, TestClock(Instant::now())),
    );
    server.welcome().await;
    welcomed(&mut incoming).await;
    server
        .send(ServerFrame::Closed {
            reason: CloseReason::SessionEnded,
        })
        .await;
    assert!(matches!(
        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap(),
        Err(ClientError::ReplacedByNewerSession)
    ));
    assert_eq!(
        state.borrow().status,
        SessionLinkStatus::Refused {
            reason: SessionDropReason::ReplacedByNewerSession
        }
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(requests.is_closed());
}

#[tokio::test(start_paused = true)]
async fn stale_notice_ack_is_refused_and_in_flight_request_is_not_replayed_after_restart() {
    let (first, mut first_server) = socket();
    let (second, mut second_server) = socket();
    let (third, mut third_server) = socket();
    let (requests, mut incoming, pump, attempts, store) =
        setup(vec![Ok(first), Ok(second), Ok(third)]).await;
    let (stop, stopped) = watch::channel(false);
    let clock = TestClock(Instant::now());
    let task = tokio::spawn(
        pump.with_recovery_policy(policy())
            .run_with_clock(stopped, clock.clone()),
    );
    let old = first_server.welcome().await;
    welcomed(&mut incoming).await;
    first_server
        .send(ServerFrame::Notice {
            position: NoticePosition::new(1),
            body: Notice::Consumer(()),
        })
        .await;
    assert!(
        matches!(incoming.recv().await.unwrap(),ClientEvent::Notice {session_id, ..} if session_id==old)
    );
    let request = requests.request(OperationRequest::Consumer(()));
    tokio::pin!(request);
    tokio::select! { biased; _=&mut request=>panic!("request returned before transport drop"), frame=first_server.receive()=>assert!(matches!(frame,ClientFrame::Request {..})) }
    first_server
        .send
        .send(Err(ConnectionClosed::Transport))
        .await
        .unwrap();
    assert!(matches!(request.await, Err(CallFailure::AnswerLost)));
    assert!(
        matches!(second_server.receive().await,ClientFrame::Hello {session_id,..} if session_id==old)
    );
    assert_eq!(clock.now(), SyncTime::from_unix_millis(1000));
    second_server
        .send(ServerFrame::Closed {
            reason: CloseReason::SessionEnded,
        })
        .await;
    let new = third_server.welcome().await;
    assert_ne!(old, new);
    loop {
        if matches!(incoming.recv().await.unwrap(), ClientEvent::Welcome { .. }) {
            break;
        }
    }
    third_server
        .send(ServerFrame::Notice {
            position: NoticePosition::new(1),
            body: Notice::Consumer(()),
        })
        .await;
    assert!(
        matches!(incoming.recv().await.unwrap(),ClientEvent::Notice {session_id,..} if session_id==new)
    );
    assert!(
        matches!(requests.acknowledge_notice(old,NoticePosition::new(1)).await,Err(ClientError::StaleNoticeAcknowledgement {expected,received}) if expected==new&&received==old)
    );
    assert_eq!(
        store.load().await.unwrap().unwrap().notice_cursor,
        NoticePosition::default()
    );
    assert!(matches!(
        third_server.received.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    requests
        .acknowledge_notice(new, NoticePosition::new(1))
        .await
        .unwrap();
    assert_eq!(
        store.load().await.unwrap().unwrap().notice_cursor,
        NoticePosition::new(1)
    );
    third_server.answer_request(&requests).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn terminal_refusal_after_transport_retry_has_no_retry_deadline() {
    let (requests, _incoming, pump, attempts, _) = setup(vec![
        Err(ConnectFailure::Unavailable),
        Err(ConnectFailure::CredentialRejected(AuthRefusal::Expired)),
    ])
    .await;
    let state = pump.link_state();
    let (_stop, stopped) = watch::channel(false);
    assert!(matches!(
        pump.run_with_clock(stopped, TestClock(Instant::now()))
            .await,
        Err(ClientError::CredentialRejected {
            reason: AuthRefusal::Expired
        })
    ));
    assert_eq!(
        state.borrow().status,
        SessionLinkStatus::Refused {
            reason: SessionDropReason::CredentialRejected {
                reason: AuthRefusal::Expired
            }
        }
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert!(requests.is_closed());
}

#[tokio::test(start_paused = true)]
async fn wrong_kind_is_terminal_even_under_retry_policy() {
    let (requests, _incoming, pump, attempts, _) = setup(vec![Err(
        ConnectFailure::CredentialRejected(AuthRefusal::WrongKind),
    )])
    .await;
    let state = pump.link_state();
    let (_stop, stopped) = watch::channel(false);
    assert!(matches!(
        timeout(
            Duration::from_secs(1),
            pump.with_recovery_policy(policy())
                .run_with_clock(stopped, TestClock(Instant::now()))
        )
        .await
        .unwrap(),
        Err(ClientError::CredentialRejected {
            reason: AuthRefusal::WrongKind
        })
    ));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(requests.is_closed());
    assert_eq!(
        state.borrow().status,
        SessionLinkStatus::Refused {
            reason: SessionDropReason::CredentialRejected {
                reason: AuthRefusal::WrongKind
            }
        }
    );
}

#[test]
fn explicit_default_recovery_policy_pins_the_documented_contract() {
    assert_eq!(
        SessionRecoveryPolicy::default(),
        SessionRecoveryPolicy {
            connection_replaced: crate::ConnectionReplacedRecovery::Reconnect,
            credential_rejected: CredentialRejectedRecovery::Retry {
                backoff: Backoff {
                    initial: Duration::from_secs(1),
                    max: Duration::from_secs(300),
                    factor: 2
                }
            },
            session_ended: SessionEndedRecovery::StartNewSession,
            welcome_timeout: None,
        }
    );
}

#[tokio::test(start_paused = true)]
async fn prewelcome_session_ended_restart_waits_for_connection_backoff() {
    let (first, mut first_server) = socket();
    let (second, mut second_server) = socket();
    let (_requests, _incoming, pump, attempts, _) = setup(vec![Ok(first), Ok(second)]).await;
    let (stop, stopped) = watch::channel(false);
    let clock = TestClock(Instant::now());
    let task = tokio::spawn(
        pump.with_recovery_policy(policy())
            .run_with_clock(stopped, clock.clone()),
    );
    assert!(matches!(
        first_server.receive().await,
        ClientFrame::Hello { .. }
    ));
    first_server
        .send(ServerFrame::Closed {
            reason: CloseReason::SessionEnded,
        })
        .await;
    assert!(matches!(
        second_server.receive().await,
        ClientFrame::Hello { .. }
    ));
    assert_eq!(
        clock.now(),
        SyncTime::from_unix_millis(1000),
        "session renewal skipped backoff"
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn constructor_without_policy_preserves_attached_expiry_retry() {
    let (first, mut first_server) = socket();
    let (second, mut second_server) = socket();
    let (requests, mut incoming, pump, attempts, _) = setup(vec![Ok(first), Ok(second)]).await;
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(pump.run_with_clock(stopped, TestClock(Instant::now())));
    let old = first_server.welcome().await;
    welcomed(&mut incoming).await;
    first_server
        .send(ServerFrame::Closed {
            reason: CloseReason::CredentialRejected(AuthRefusal::Expired),
        })
        .await;
    assert_eq!(second_server.welcome().await, old);
    loop {
        if matches!(incoming.recv().await.unwrap(), ClientEvent::Welcome { .. }) {
            break;
        }
    }
    second_server.answer_request(&requests).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}
