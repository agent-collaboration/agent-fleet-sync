use super::*;
use crate::{SessionDropReason, SessionLinkStatus};

async fn snapshot(
    state: &mut watch::Receiver<crate::SessionLinkState>,
    expected: impl Fn(&crate::SessionLinkState) -> bool,
) -> crate::SessionLinkState {
    timeout(Duration::from_secs(20), async {
        loop {
            let current = state.borrow_and_update().clone();
            if expected(&current) {
                return current;
            }
            state.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn repeated_identical_drops_have_distinct_times_connections_and_event_identity() {
    let (connection, _server) = socket();
    let (requests, _incoming, pump, _, _) = setup(vec![
        Err(ConnectFailure::CredentialRejected(AuthRefusal::Unknown)),
        Err(ConnectFailure::CredentialRejected(AuthRefusal::Unknown)),
        Ok(connection),
    ])
    .await;
    let mut state = pump.link_state();
    let mut events = pump.observe_link();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(
        pump.with_recovery_policy(policy())
            .run_with_clock(stopped, TestClock(Instant::now())),
    );
    let first = snapshot(&mut state, |value| {
        matches!(value.status, SessionLinkStatus::CredentialRejected { .. })
    })
    .await;
    assert_eq!(first.connection, 1);
    assert_eq!(first.since, SyncTime::from_unix_millis(0));
    let drop_one = first.last_drop.unwrap();
    assert_eq!(drop_one.connection, 1);
    assert_eq!(drop_one.at, SyncTime::from_unix_millis(0));
    tokio::time::advance(Duration::from_secs(1)).await;
    let second = snapshot(&mut state, |value| {
        value.connection == 2
            && matches!(value.status, SessionLinkStatus::CredentialRejected { .. })
    })
    .await;
    let drop_two = second.last_drop.unwrap();
    assert_eq!(drop_one.reason, drop_two.reason);
    assert_eq!(drop_two.connection, 2);
    assert_eq!(drop_two.at, SyncTime::from_unix_millis(1000));
    assert!(second.revision > first.revision);
    let mut connections = Vec::new();
    while let Ok(event) = events.try_recv() {
        connections.push(event.connection);
    }
    assert_eq!(connections, vec![1, 1, 2, 2]);
    assert!(!requests.is_closed());
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn inbound_frames_update_retained_contact_at_most_once_per_ten_seconds() {
    let (connection, mut server) = socket();
    let (requests, mut incoming, pump, _, _) = setup(vec![Ok(connection)]).await;
    let mut state = pump.link_state();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(pump.run_with_clock(stopped, TestClock(Instant::now())));
    let session_id = server.welcome().await;
    welcomed(&mut incoming).await;
    let original = snapshot(&mut state, |value| {
        matches!(value.status, SessionLinkStatus::Welcomed { .. })
    })
    .await;
    assert_eq!(original.last_frame_at, Some(SyncTime::from_unix_millis(0)));
    tokio::time::advance(Duration::from_secs(1)).await;
    server
        .send(ServerFrame::Welcome {
            session_id,
            databases: vec![],
        })
        .await;
    welcomed(&mut incoming).await;
    assert_eq!(
        state.borrow().revision,
        original.revision,
        "unchanged status bypassed contact coalescing"
    );
    tokio::time::advance(Duration::from_secs(2)).await;
    server
        .send(ServerFrame::Notice {
            position: NoticePosition::new(1),
            body: Notice::Consumer(()),
        })
        .await;
    assert!(matches!(
        incoming.recv().await.unwrap(),
        ClientEvent::Notice { .. }
    ));
    assert_eq!(state.borrow().revision, original.revision);
    tokio::time::advance(Duration::from_secs(7)).await;
    let updated = snapshot(&mut state, |value| value.revision > original.revision).await;
    assert_eq!(
        updated.last_frame_at,
        Some(SyncTime::from_unix_millis(3000))
    );
    assert_eq!(updated.since, original.since);
    assert_eq!(updated.connection, original.connection);
    // An uncorrelated answer is consumed inside SessionClient without a ClientEvent.
    tokio::time::advance(Duration::from_secs(1)).await;
    server
        .send(ServerFrame::Answer {
            frame: FrameId::new(Uuid::now_v7()).unwrap(),
            body: OperationAnswer::Consumer(()),
        })
        .await;
    server.answer_request(&requests).await;
    assert_eq!(state.borrow().revision, updated.revision);
    tokio::time::advance(Duration::from_secs(9)).await;
    let latest = snapshot(&mut state, |value| value.revision > updated.revision).await;
    assert_eq!(
        latest.last_frame_at,
        Some(SyncTime::from_unix_millis(11000))
    );
    assert_eq!(latest.since, original.since);
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn old_contact_flush_cannot_replace_a_new_connection_snapshot() {
    let (first, mut first_server) = socket();
    let (second, mut second_server) = socket();
    let (requests, _incoming, pump, _, _) = setup(vec![Ok(first), Ok(second)]).await;
    let mut state = pump.link_state();
    let (stop, stopped) = watch::channel(false);
    let clock = TestClock(Instant::now());
    let task = tokio::spawn(
        pump.with_recovery_policy(SessionRecoveryPolicy {
            welcome_timeout: None,
            ..policy()
        })
        .run_with_clock(stopped, clock.clone()),
    );
    assert!(matches!(
        first_server.receive().await,
        ClientFrame::Hello { .. }
    ));
    let old = snapshot(&mut state, |value| {
        matches!(value.status, SessionLinkStatus::Connecting { .. })
    })
    .await;
    tokio::time::advance(Duration::from_secs(3)).await;
    first_server
        .send(ServerFrame::Closed {
            reason: CloseReason::SessionEnded,
        })
        .await;
    assert!(matches!(
        second_server.receive().await,
        ClientFrame::Hello { .. }
    ));
    assert!(matches!(
        requests.request(OperationRequest::Consumer(())).await,
        Err(CallFailure::NotSent { .. })
    ));
    let connecting = snapshot(&mut state, |value| value.connection == 2).await;
    assert!(connecting.revision > old.revision);
    assert_eq!(connecting.last_frame_at, None);
    assert_eq!(connecting.since, SyncTime::from_unix_millis(4000));
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(matches!(
        requests.request(OperationRequest::Consumer(())).await,
        Err(CallFailure::NotSent { .. })
    ));
    assert_eq!(
        *state.borrow(),
        connecting,
        "old contact timer published a stale snapshot"
    );
    // Old snapshots remain immutable and have a lower revision than current state.
    assert!(old.revision < state.borrow().revision);
    assert_eq!(clock.now(), SyncTime::from_unix_millis(14000));
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

async fn assert_transport_timeout_drop(error: ConnectionClosed, expected: SessionDropReason) {
    let (connection, mut server) = socket();
    let (requests, mut incoming, pump, _, _) = setup(vec![Ok(connection)]).await;
    let mut state = pump.link_state();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(pump.run_with_clock(stopped, TestClock(Instant::now())));
    server.welcome().await;
    welcomed(&mut incoming).await;
    tokio::time::advance(Duration::from_secs(2)).await;
    server.send.send(Err(error)).await.unwrap();
    let dropped = snapshot(&mut state, |value| {
        matches!(value.status, SessionLinkStatus::Disconnected { .. })
    })
    .await;
    let last_drop = dropped.last_drop.unwrap();
    assert_eq!(last_drop.reason, expected);
    assert_eq!(last_drop.connection, 1);
    assert_eq!(last_drop.at, SyncTime::from_unix_millis(2000));
    assert!(!requests.is_closed());
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn idle_timeout_is_retained_as_its_typed_drop_reason() {
    assert_transport_timeout_drop(
        ConnectionClosed::IdleTimeout,
        SessionDropReason::IdleTimeout,
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn send_timeout_is_retained_as_its_typed_drop_reason() {
    assert_transport_timeout_drop(
        ConnectionClosed::SendTimeout,
        SessionDropReason::SendTimeout,
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn outbound_send_timeout_keeps_its_old_connection_identity_after_reconnect() {
    let (mut first, mut first_server) = socket();
    let (second, mut second_server) = socket();
    first.request_send_error = Some(ConnectionClosed::SendTimeout);
    let (requests, mut incoming, pump, _, _) = setup(vec![Ok(first), Ok(second)]).await;
    let mut state = pump.link_state();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(pump.run_with_clock(stopped, TestClock(Instant::now())));
    first_server.welcome().await;
    welcomed(&mut incoming).await;
    assert!(matches!(
        requests.request(OperationRequest::Consumer(())).await,
        Err(CallFailure::AnswerLost)
    ));
    let dropped = snapshot(&mut state, |value| {
        value
            .last_drop
            .as_ref()
            .is_some_and(|drop| drop.reason == SessionDropReason::SendTimeout)
    })
    .await;
    assert_eq!(dropped.last_drop.as_ref().unwrap().connection, 1);
    second_server.welcome().await;
    loop {
        if matches!(incoming.recv().await.unwrap(), ClientEvent::Welcome { .. }) {
            break;
        }
    }
    let connected = snapshot(&mut state, |value| {
        value.connection == 2 && matches!(value.status, SessionLinkStatus::Welcomed { .. })
    })
    .await;
    assert!(connected.revision > dropped.revision);
    assert_eq!(connected.last_drop.unwrap().connection, 1);
    second_server.answer_request(&requests).await;
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}
