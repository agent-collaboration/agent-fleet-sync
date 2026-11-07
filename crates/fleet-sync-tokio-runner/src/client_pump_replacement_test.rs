use super::*;
use crate::{ConnectionReplacedRecovery, SessionDropReason, SessionLinkStatus};

#[tokio::test(start_paused = true)]
async fn terminal_connection_replacement_stops_with_reason_before_or_after_welcome() {
    for attached in [false, true] {
        // Arrange: the replacement has a second socket available, which must stay unused.
        let (first, mut first_server) = socket();
        let (second, _second_server) = socket();
        let (requests, mut incoming, pump, attempts, _) = setup(vec![Ok(first), Ok(second)]).await;
        let state = pump.link_state();
        let mut links = pump.observe_link();
        let (_stop, stopped) = watch::channel(false);
        let task = tokio::spawn(
            pump.with_recovery_policy(SessionRecoveryPolicy {
                connection_replaced: ConnectionReplacedRecovery::Terminal,
                ..policy()
            })
            .run_with_clock(stopped, TestClock(Instant::now())),
        );
        if attached {
            first_server.welcome().await;
            welcomed(&mut incoming).await;
        } else {
            assert!(matches!(
                first_server.receive().await,
                ClientFrame::Hello { .. }
            ));
        }

        // Act: central transfers this same session's physical connection.
        first_server
            .send(ServerFrame::Closed {
                reason: CloseReason::ConnectionReplaced,
            })
            .await;
        let terminal = timeout(Duration::from_secs(1), task).await;

        // Assert: terminal means no retry, a typed cause, and an observable refusal reason.
        assert!(terminal.is_ok(), "replacement pump kept retrying");
        assert!(matches!(
            terminal.unwrap().unwrap(),
            Err(ClientError::ConnectionReplaced)
        ));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(requests.is_closed());
        let reason = SessionDropReason::Disconnected {
            reason: Some(CloseReason::ConnectionReplaced),
        };
        assert_eq!(
            state.borrow().status,
            SessionLinkStatus::Refused {
                reason: reason.clone()
            }
        );
        let mut observed = false;
        while let Ok(event) = links.try_recv() {
            observed |= event.kind
                == SessionLinkEventKind::Refused {
                    reason: reason.clone(),
                };
        }
        assert!(
            observed,
            "terminal replacement reason was absent from observation stream"
        );
        let mut disconnect = false;
        while let Some(event) = incoming.recv().await {
            if let ClientEvent::Disconnected {
                reason: Some(CloseReason::ConnectionReplaced),
                retry_at,
            } = event
            {
                assert_eq!(retry_at, None);
                disconnect = true;
            }
        }
        assert!(disconnect);
    }
}

#[tokio::test(start_paused = true)]
async fn default_connection_replacement_reconnects_with_the_same_session() {
    // Arrange: no opt-in policy, preserving the existing host contract.
    let (first, mut first_server) = socket();
    let (second, mut second_server) = socket();
    let (requests, mut incoming, pump, attempts, _) = setup(vec![Ok(first), Ok(second)]).await;
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(pump.run_with_clock(stopped, TestClock(Instant::now())));
    let original = first_server.welcome().await;
    welcomed(&mut incoming).await;

    // Act: same physical replacement under the unchanged default.
    first_server
        .send(ServerFrame::Closed {
            reason: CloseReason::ConnectionReplaced,
        })
        .await;
    let resumed = second_server.welcome().await;
    while !matches!(incoming.recv().await, Some(ClientEvent::Welcome { .. })) {}

    // Assert: same session resumes and remains operational.
    assert_eq!(resumed, original);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    second_server.answer_request(&requests).await;
    stop.send_replace(true);
    task.await.unwrap().unwrap();
}
