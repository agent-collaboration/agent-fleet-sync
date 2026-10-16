#![allow(clippy::unwrap_used)]
use crate::rule_test_support::*;
use crate::*;
use fleet_sync_protocol::*;
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
        let mut scope = SqlScope::new().await;
        let connection = binding(2);
        let id = session(3);
        attach(&mut scope, &connection, id).await;
        let key = SessionKey {
            peer: connection.peer,
            session_id: id,
        };
        publish_notice::<_, NoConsumer>(
            &mut scope,
            &key,
            Notice::LeaseChanged {
                database: DatabaseId::new(Uuid::from_u128(4)).unwrap(),
            },
        )
        .await
        .unwrap();
        connection_closed::<_, NoConsumer>(&mut scope, &connection, cause, now(5))
            .await
            .unwrap();
        assert_eq!(
            scope.record(&key).await.state,
            SessionState::Detached { since: now(5) }
        );
        assert_eq!(
            notices_after::<_, NoConsumer>(&mut scope, &key, NoticePosition::default())
                .await
                .unwrap()
                .len(),
            1
        );
        attach(&mut scope, &binding(5), id).await;
        assert_eq!(scope.records().await.len(), 1);
        assert_eq!(scope.record(&key).await.notice_head.value(), 1);
    }
}

#[tokio::test]
async fn replacement_ignores_old_frames_hello_and_late_close() {
    let mut scope = SqlScope::new().await;
    let first = binding(2);
    let next = binding(3);
    let id = session(4);
    attach(&mut scope, &first, id).await;
    let effects = attach(&mut scope, &next, id).await;
    assert!(effects.iter().any(|effect| matches!(effect, SessionEffect::Close { connection, reason: CloseReason::ConnectionReplaced } if *connection == first.connection)));
    assert!(
        frame_received::<_, NoConsumer>(&mut scope, &first, &hello(id, 1), now(2))
            .await
            .unwrap()
            .is_empty()
    );
    let request: ClientFrame = ClientFrame::Request {
        frame: FrameId::new(Uuid::from_u128(5)).unwrap(),
        body: OperationRequest::Consumer(()),
    };
    assert!(
        frame_received::<_, NoConsumer>(
            &mut scope,
            &first,
            &serde_json::to_vec(&request).unwrap(),
            now(2)
        )
        .await
        .unwrap()
        .is_empty()
    );
    connection_closed::<_, NoConsumer>(&mut scope, &first, CloseCause::Dropped, now(3))
        .await
        .unwrap();
    assert_eq!(
        scope
            .record(&SessionKey {
                peer: next.peer,
                session_id: id
            })
            .await
            .state,
        SessionState::Active {
            connection: next.connection
        }
    );
}

#[tokio::test]
async fn repeat_hello_changes_no_session_or_attachment_and_requests_a_fresh_welcome() {
    let mut scope = SqlScope::new().await;
    let connection = binding(2);
    let id = session(3);
    attach(&mut scope, &connection, id).await;
    let sessions = scope.records().await;
    let registrations = scope.registrations.clone();
    let effects = frame_received::<_, NoConsumer>(&mut scope, &connection, &hello(id, 1), now(2))
        .await
        .unwrap();
    assert_eq!(scope.records().await, sessions);
    assert_eq!(scope.registrations, registrations);
    assert!(
        matches!(effects.as_slice(), [SessionEffect::Welcome { connection: target, peer, session_id }] if *target == connection.connection && *peer == connection.peer && *session_id == id)
    );
}

#[tokio::test]
async fn expired_and_malformed_connections_detach_instead_of_ending() {
    for expired in [false, true] {
        let mut scope = SqlScope::new().await;
        let connection = binding(2);
        let id = session(3);
        attach(&mut scope, &connection, id).await;
        let effects = frame_received::<_, NoConsumer>(
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
                .record(&SessionKey {
                    peer: connection.peer,
                    session_id: id
                })
                .await
                .state,
            SessionState::Detached { .. }
        ));
    }
}

#[tokio::test]
async fn new_session_and_revocation_are_terminal_and_ended_ids_never_resume() {
    let mut scope = SqlScope::new().await;
    let first = binding(2);
    let second = binding(3);
    let old_id = session(4);
    let new_id = session(5);
    attach(&mut scope, &first, old_id).await;
    attach(&mut scope, &second, new_id).await;
    assert_eq!(
        scope
            .record(&SessionKey {
                peer: first.peer,
                session_id: old_id
            })
            .await
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
            .record(&SessionKey {
                peer: second.peer,
                session_id: new_id
            })
            .await
            .state,
        SessionState::Active {
            connection: second.connection
        }
    );
    let revoked =
        end_sessions::<_, NoConsumer>(&mut scope, &first.peer, EndReason::PeerRevoked, now(3))
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
    let mut scope = SqlScope::new().await;
    let connection = binding(2);
    let id = session(3);
    attach(&mut scope, &connection, id).await;
    let frame = FrameId::new(Uuid::from_u128(4)).unwrap();
    let request: ClientFrame = ClientFrame::Request {
        frame,
        body: OperationRequest::Consumer(()),
    };
    let effects = frame_received::<_, NoConsumer>(
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
        frame_received::<_, NoConsumer>(
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
    let changed =
        frame_received::<_, NoConsumer>(&mut scope, &connection, &hello(session(5), 1), now(2))
            .await
            .unwrap();
    assert!(changed.iter().any(|effect| matches!(
        effect,
        SessionEffect::Close {
            reason: CloseReason::MalformedFrame,
            ..
        }
    )));
    assert_eq!(scope.records().await.len(), 1);
}

#[tokio::test]
async fn equal_session_ids_on_different_peers_do_not_share_state() {
    let mut scope = SqlScope::new().await;
    let first = binding(2);
    let second = ConnectionBinding {
        peer: PeerId::new(Uuid::from_u128(9)).unwrap(),
        ..binding(3)
    };
    attach(&mut scope, &first, session(4)).await;
    attach(&mut scope, &second, session(4)).await;
    end_sessions::<_, NoConsumer>(&mut scope, &first.peer, EndReason::PeerRevoked, now(2))
        .await
        .unwrap();
    assert_eq!(scope.records().await.len(), 1);
    assert_eq!(
        scope
            .record(&SessionKey {
                peer: second.peer,
                session_id: session(4)
            })
            .await
            .state,
        SessionState::Active {
            connection: second.connection
        }
    );
}

#[tokio::test]
async fn rollback_discards_session_notices_and_staged_registrations_before_effects_run() {
    let mut scope = SqlScope::new().await;
    let connection = binding(2);
    let id = session(3);
    let key = SessionKey {
        peer: connection.peer,
        session_id: id,
    };
    let published_registrations = scope.registrations.clone();
    let effects = attach(&mut scope, &connection, id).await;
    assert!(matches!(
        effects.as_slice(),
        [SessionEffect::Welcome { .. }]
    ));
    publish_notice::<_, NoConsumer>(
        &mut scope,
        &key,
        Notice::LeaseChanged {
            database: DatabaseId::new(Uuid::from_u128(4)).unwrap(),
        },
    )
    .await
    .unwrap();
    let SqlScope {
        pool, transaction, ..
    } = scope;
    transaction.rollback().await.unwrap();
    // The host publishes neither staged registration edits nor effects on rollback.
    let mut reopened = SqlScope {
        transaction: pool.begin().await.unwrap(),
        pool,
        registrations: published_registrations,
    };
    assert!(
        crate::session_storage::load_session(&mut reopened, &key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        notices_after::<_, NoConsumer>(&mut reopened, &key, NoticePosition::default())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(reopened.registration(&connection.connection), None);
}

#[tokio::test]
async fn notice_positions_keep_the_full_u64_range_and_refuse_overflow() {
    let mut scope = SqlScope::new().await;
    let connection = binding(2);
    let id = session(3);
    attach(&mut scope, &connection, id).await;
    let key = SessionKey {
        peer: connection.peer,
        session_id: id,
    };
    let mut record = scope.record(&key).await;
    record.notice_head = NoticePosition::new(u64::MAX - 1);
    crate::session_storage::save_session(&mut scope, &record)
        .await
        .unwrap();
    let notice = || Notice::LeaseChanged {
        database: DatabaseId::new(Uuid::from_u128(4)).unwrap(),
    };
    publish_notice::<_, NoConsumer>(&mut scope, &key, notice())
        .await
        .unwrap();
    let history =
        notices_after::<_, NoConsumer>(&mut scope, &key, NoticePosition::new(u64::MAX - 1))
            .await
            .unwrap();
    assert_eq!(history.first().unwrap().position.value(), u64::MAX);
    assert!(matches!(
        publish_notice::<_, NoConsumer>(&mut scope, &key, notice()).await,
        Err(SessionError::NoticePositionExhausted)
    ));
    assert_eq!(scope.record(&key).await.notice_head.value(), u64::MAX);
}
