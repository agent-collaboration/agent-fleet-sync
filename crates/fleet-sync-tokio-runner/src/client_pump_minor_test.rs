use super::*;
use crate::{SessionConnectFailureKind, SessionDropReason, SessionLinkStatus};

#[tokio::test(start_paused = true)]
async fn new_pumps_have_distinct_retained_instance_identity() {
    let (_, _, first, _, _) = setup(vec![]).await;
    let (_, _, second, _, _) = setup(vec![]).await;
    let first_state = first.link_state();
    let second_state = second.link_state();
    assert_ne!(
        first_state.borrow().instance,
        second_state.borrow().instance
    );
    assert_eq!(first_state.borrow().revision, 0);
    let instance = first_state.borrow().instance;
    let (_stop, stopped) = watch::channel(true);
    first
        .run_with_clock(stopped, TestClock(Instant::now()))
        .await
        .unwrap();
    assert_eq!(first_state.borrow().instance, instance);
}

#[tokio::test(start_paused = true)]
async fn run_clock_initializes_since_before_the_first_status_transition() {
    let (_requests, _incoming, pump, _, _) = setup(vec![]).await;
    let state = pump.link_state();
    pump.initialize_link_clock(&FixedClock);
    assert_eq!(state.borrow().since, SyncTime::from_unix_millis(123000));
    assert_eq!(state.borrow().revision, 1);
}
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(123000)
    }
}

async fn assert_configuration_refusal(error: ConnectFailure, kind: SessionConnectFailureKind) {
    let (requests, _incoming, pump, attempts, _) = setup(vec![Err(error.clone())]).await;
    let state = pump.link_state();
    let (_stop, stopped) = watch::channel(false);
    let task = pump
        .with_recovery_policy(policy())
        .run_with_clock(stopped, TestClock(Instant::now()));
    let result = timeout(Duration::from_secs(2), task).await.unwrap();
    assert!(matches!(result,Err(ClientError::Connect(actual)) if actual==error));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(requests.is_closed());
    assert_eq!(
        state.borrow().status,
        SessionLinkStatus::Refused {
            reason: SessionDropReason::ConnectFailed { kind }
        }
    );
}
#[tokio::test(start_paused = true)]
async fn invalid_endpoint_is_terminal_configuration_refusal() {
    assert_configuration_refusal(
        ConnectFailure::InvalidEndpoint,
        SessionConnectFailureKind::InvalidEndpoint,
    )
    .await;
}
#[tokio::test(start_paused = true)]
async fn invalid_credential_is_terminal_configuration_refusal() {
    assert_configuration_refusal(
        ConnectFailure::InvalidCredential,
        SessionConnectFailureKind::InvalidCredential,
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn consecutive_prewelcome_session_restarts_escalate_until_backoff_cap() {
    let mut connections = Vec::new();
    let mut servers = Vec::new();
    for _ in 0..5 {
        let (connection, server) = socket();
        connections.push(Ok(connection));
        servers.push(server);
    }
    let (requests, _incoming, pump, attempts, _) = setup(connections).await;
    let (stop, stopped) = watch::channel(false);
    let clock = TestClock(Instant::now());
    let task = tokio::spawn(
        pump.with_recovery_policy(policy())
            .run_with_clock(stopped, clock.clone()),
    );
    for (server, expected_ms) in servers.iter_mut().zip([0, 1000, 3000, 7000, 11000]) {
        assert!(matches!(server.receive().await, ClientFrame::Hello { .. }));
        assert_eq!(clock.now(), SyncTime::from_unix_millis(expected_ms));
        if expected_ms < 11000 {
            server
                .send(ServerFrame::Closed {
                    reason: CloseReason::SessionEnded,
                })
                .await;
        } else {
            stop.send(true).unwrap();
            task.await.unwrap().unwrap();
            break;
        }
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 5);
    assert!(requests.is_closed());
}

#[tokio::test(start_paused = true)]
async fn prewelcome_session_ended_without_policy_finishes_refused() {
    let (connection, mut server) = socket();
    let (requests, _incoming, pump, attempts, _) = setup(vec![Ok(connection)]).await;
    let state = pump.link_state();
    let (_stop, stopped) = watch::channel(false);
    let task = tokio::spawn(pump.run_with_clock(stopped, TestClock(Instant::now())));
    assert!(matches!(server.receive().await, ClientFrame::Hello { .. }));
    server
        .send(ServerFrame::Closed {
            reason: CloseReason::SessionEnded,
        })
        .await;
    assert!(matches!(
        task.await.unwrap(),
        Err(ClientError::SessionEnded)
    ));
    assert_eq!(
        state.borrow().status,
        SessionLinkStatus::Refused {
            reason: SessionDropReason::Disconnected {
                reason: Some(CloseReason::SessionEnded)
            }
        }
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(requests.is_closed());
}

#[tokio::test(start_paused = true)]
async fn credential_terminal_policy_also_stops_an_attached_expiry() {
    let (connection, mut server) = socket();
    let (requests, mut incoming, pump, attempts, _) = setup(vec![Ok(connection)]).await;
    let mut links = pump.observe_link();
    let state = pump.link_state();
    let configured = SessionRecoveryPolicy {
        credential_rejected: CredentialRejectedRecovery::Terminal,
        ..policy()
    };
    let (_stop, stopped) = watch::channel(false);
    let task = tokio::spawn(
        pump.with_recovery_policy(configured)
            .run_with_clock(stopped, TestClock(Instant::now())),
    );
    server.welcome().await;
    welcomed(&mut incoming).await;
    server
        .send(ServerFrame::Closed {
            reason: CloseReason::CredentialRejected(AuthRefusal::Expired),
        })
        .await;
    let result = timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        result,
        Err(ClientError::CredentialRejected {
            reason: AuthRefusal::Expired
        })
    ));
    assert_eq!(
        state.borrow().status,
        crate::SessionLinkStatus::Refused {
            reason: crate::SessionDropReason::CredentialRejected {
                reason: AuthRefusal::Expired
            }
        }
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(requests.is_closed());
    let mut refusal_seen = false;
    while let Ok(event) = links.try_recv() {
        refusal_seen |= matches!(
            event.kind,
            SessionLinkEventKind::CredentialRejected {
                reason: AuthRefusal::Expired
            }
        );
    }
    assert!(refusal_seen);
}

#[tokio::test(start_paused = true)]
async fn attached_terminal_refusal_clears_the_clients_scheduled_retry_before_stop() {
    let (connection, mut server) = socket();
    let (_requests, _incoming, pump, _, _) = setup(vec![Ok(connection)]).await;
    let mut pump = pump.with_recovery_policy(SessionRecoveryPolicy::terminal());
    let state = pump.link_state();
    let host = PumpClock::new(TestClock(Instant::now()));
    pump.client
        .connect(&pump.endpoint, &pump.credential, &host)
        .await
        .unwrap();
    server.welcome().await;
    assert!(matches!(
        pump.client.receive(&host).await.unwrap(),
        ClientEvent::Welcome { .. }
    ));
    server
        .send(ServerFrame::Closed {
            reason: CloseReason::CredentialRejected(AuthRefusal::Expired),
        })
        .await;
    assert!(matches!(
        pump.client.receive(&host).await.unwrap(),
        ClientEvent::Disconnected {
            retry_at: Some(_),
            ..
        }
    ));
    let result = pump
        .recover_credential_refusal(
            AuthRefusal::Expired,
            CredentialRefusalOrigin::SessionFrame,
            &host,
        )
        .await;
    assert!(matches!(
        result,
        Err(ClientError::CredentialRejected {
            reason: AuthRefusal::Expired
        })
    ));
    // Even the diagnostic transition before final Refused must not advertise a retry.
    assert_eq!(
        state.borrow().status,
        SessionLinkStatus::CredentialRejected {
            reason: AuthRefusal::Expired,
            retry_at: None
        }
    );
    pump.client.close().await;
}
