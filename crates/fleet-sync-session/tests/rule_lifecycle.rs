#![allow(clippy::unwrap_used)]
mod support;
use fleet_sync_protocol::*;
use fleet_sync_session::*;
use support::*;
use uuid::Uuid;

#[tokio::test]
async fn every_connection_close_preserves_the_session_and_notice_history() {
    for cause in [
        CloseCause::Dropped,
        CloseCause::Replaced,
        CloseCause::CredentialExpired,
        CloseCause::MalformedFrame,
        CloseCause::PeerRevoked,
    ] {
        let mut scope = MemoryScope::default();
        let connection = binding(2);
        let id = session(3);
        attach(&mut scope, &connection, id).await;
        let key = SessionKey {
            peer: connection.peer,
            session_id: id,
        };
        publish_notice(
            &mut scope,
            &key,
            Notice::LeaseChanged {
                database: DatabaseId::new(Uuid::from_u128(4)).unwrap(),
            },
        )
        .await
        .unwrap();
        connection_closed(&mut scope, &connection, cause, now(5))
            .await
            .unwrap();
        assert_eq!(
            scope.sessions.get(&key).unwrap().state,
            SessionState::Detached { since: now(5) }
        );
        assert_eq!(
            notices_after(&mut scope, &key, NoticePosition::default())
                .await
                .unwrap()
                .len(),
            1
        );
        attach(&mut scope, &binding(5), id).await;
        assert_eq!(scope.sessions.len(), 1);
        assert_eq!(scope.sessions.get(&key).unwrap().notice_head.value(), 1);
    }
}

#[tokio::test]
async fn replacement_ignores_old_frames_hello_and_late_close() {
    let mut scope = MemoryScope::default();
    let first = binding(2);
    let next = binding(3);
    let id = session(4);
    attach(&mut scope, &first, id).await;
    let effects = attach(&mut scope, &next, id).await;
    assert!(effects.iter().any(|effect| matches!(effect, SessionEffect::Close { connection, reason: CloseReason::ConnectionReplaced } if *connection == first.connection)));
    assert!(
        frame_received(&mut scope, &first, &hello(id, 1), now(2))
            .await
            .unwrap()
            .is_empty()
    );
    let request: ClientFrame = ClientFrame::Request {
        frame: FrameId::new(Uuid::from_u128(5)).unwrap(),
        body: OperationRequest::Consumer(()),
    };
    assert!(
        frame_received(
            &mut scope,
            &first,
            &serde_json::to_vec(&request).unwrap(),
            now(2)
        )
        .await
        .unwrap()
        .is_empty()
    );
    connection_closed(&mut scope, &first, CloseCause::Dropped, now(3))
        .await
        .unwrap();
    assert_eq!(
        scope
            .sessions
            .get(&SessionKey {
                peer: next.peer,
                session_id: id
            })
            .unwrap()
            .state,
        SessionState::Active {
            connection: next.connection
        }
    );
}

#[tokio::test]
async fn repeat_hello_changes_no_session_or_attachment_and_reads_fresh_status() {
    let mut scope = MemoryScope::default();
    let connection = binding(2);
    let id = session(3);
    attach(&mut scope, &connection, id).await;
    let sessions = scope.sessions.clone();
    let registrations = scope.registrations.clone();
    let database = DatabaseId::new(Uuid::from_u128(4)).unwrap();
    let acked = ReplicationPosition::new(Epoch::new(2).unwrap(), CommitSequence::new(9));
    scope.statuses = vec![DatabaseStatus::NeedsSnapshot { database, acked }];
    let effects = frame_received(&mut scope, &connection, &hello(id, 1), now(2))
        .await
        .unwrap();
    assert_eq!(scope.sessions, sessions);
    assert_eq!(scope.registrations, registrations);
    assert!(
        matches!(effects.as_slice(), [SessionEffect::Send { frame: ServerFrame::Welcome { databases, .. }, .. }] if databases == &scope.statuses)
    );
}

#[tokio::test]
async fn expired_and_malformed_connections_detach_instead_of_ending() {
    for expired in [false, true] {
        let mut scope = MemoryScope::default();
        let connection = binding(2);
        let id = session(3);
        attach(&mut scope, &connection, id).await;
        let effects = frame_received(
            &mut scope,
            &connection,
            b"not json",
            now(if expired { 100 } else { 2 }),
        )
        .await
        .unwrap();
        assert!(effects.iter().any(|effect| matches!(
            effect,
            SessionEffect::Close {
                reason: CloseReason::CredentialRejected(AuthRefusal::Expired),
                ..
            }
        ) && expired
            || matches!(
                effect,
                SessionEffect::Close {
                    reason: CloseReason::MalformedFrame,
                    ..
                }
            ) && !expired));
        assert!(matches!(
            scope
                .sessions
                .get(&SessionKey {
                    peer: connection.peer,
                    session_id: id
                })
                .unwrap()
                .state,
            SessionState::Detached { .. }
        ));
    }
}

#[tokio::test]
async fn new_session_and_revocation_are_terminal_and_ended_ids_never_resume() {
    let mut scope = MemoryScope::default();
    let first = binding(2);
    let second = binding(3);
    let old_id = session(4);
    let new_id = session(5);
    attach(&mut scope, &first, old_id).await;
    attach(&mut scope, &second, new_id).await;
    assert_eq!(
        scope
            .sessions
            .get(&SessionKey {
                peer: first.peer,
                session_id: old_id
            })
            .unwrap()
            .state,
        SessionState::Ended {
            reason: EndReason::NewSession
        }
    );
    let refused = attach(&mut scope, &binding(6), old_id).await;
    assert!(refused.iter().any(|effect| matches!(
        effect,
        SessionEffect::Close {
            reason: CloseReason::SessionEnded,
            ..
        }
    )));
    assert_eq!(
        scope
            .sessions
            .get(&SessionKey {
                peer: second.peer,
                session_id: new_id
            })
            .unwrap()
            .state,
        SessionState::Active {
            connection: second.connection
        }
    );
    let revoked = end_sessions(&mut scope, &first.peer, EndReason::PeerRevoked, now(3))
        .await
        .unwrap();
    assert!(revoked.iter().any(|effect| matches!(
        effect,
        SessionEffect::Close {
            reason: CloseReason::CredentialRejected(AuthRefusal::Revoked),
            ..
        }
    )));
    let refused = attach(&mut scope, &binding(7), new_id).await;
    assert!(refused.iter().any(|effect| matches!(
        effect,
        SessionEffect::Close {
            reason: CloseReason::SessionEnded,
            ..
        }
    )));
}

#[tokio::test]
async fn a_connection_attaches_once_and_both_operation_directions_use_the_binding() {
    let mut scope = MemoryScope::default();
    let connection = binding(2);
    let id = session(3);
    attach(&mut scope, &connection, id).await;
    let frame = FrameId::new(Uuid::from_u128(4)).unwrap();
    let request: ClientFrame = ClientFrame::Request {
        frame,
        body: OperationRequest::Consumer(()),
    };
    let effects = frame_received(
        &mut scope,
        &connection,
        &serde_json::to_vec(&request).unwrap(),
        now(2),
    )
    .await
    .unwrap();
    assert!(
        matches!(effects.as_slice(), [SessionEffect::Route { session, frame: routed, .. }] if session.peer == connection.peer && session.session_id == id && *routed == frame)
    );
    let answer: ClientFrame = ClientFrame::Answer {
        frame,
        body: ClientAnswer::Consumer(()),
    };
    assert!(matches!(
        frame_received(
            &mut scope,
            &connection,
            &serde_json::to_vec(&answer).unwrap(),
            now(2)
        )
        .await
        .unwrap()
        .as_slice(),
        [SessionEffect::RouteAnswer { .. }]
    ));
    let changed = frame_received(&mut scope, &connection, &hello(session(5), 1), now(2))
        .await
        .unwrap();
    assert!(changed.iter().any(|effect| matches!(
        effect,
        SessionEffect::Close {
            reason: CloseReason::MalformedFrame,
            ..
        }
    )));
    assert_eq!(scope.sessions.len(), 1);
}

#[tokio::test]
async fn equal_session_ids_on_different_peers_do_not_share_state() {
    let mut scope = MemoryScope::default();
    let first = binding(2);
    let second = ConnectionBinding {
        peer: PeerId::new(Uuid::from_u128(9)).unwrap(),
        ..binding(3)
    };
    attach(&mut scope, &first, session(4)).await;
    attach(&mut scope, &second, session(4)).await;
    end_sessions(&mut scope, &first.peer, EndReason::PeerRevoked, now(2))
        .await
        .unwrap();
    assert_eq!(scope.sessions.len(), 2);
    assert_eq!(
        scope
            .sessions
            .get(&SessionKey {
                peer: second.peer,
                session_id: session(4)
            })
            .unwrap()
            .state,
        SessionState::Active {
            connection: second.connection
        }
    );
}
