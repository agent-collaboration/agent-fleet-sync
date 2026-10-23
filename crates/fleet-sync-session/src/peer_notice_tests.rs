#![allow(clippy::unwrap_used)]
use crate::{rule_test_support::*, *};
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};
use sqlx_core::row::Row;
use sqlx_turso::Turso;
use uuid::Uuid;

struct NoticeProtocol;
#[derive(Serialize, Deserialize)]
struct NoticePayload {
    message: String,
}
impl ConsumerProtocol for NoticeProtocol {
    type Delivery = NoDelivery;
    type Request = ();
    type Answer = ();
    type ServerRequest = ();
    type ClientAnswer = ();
    type Notice = NoticePayload;
    type Annotation = ();
}

fn notice() -> Notice {
    Notice::LeaseChanged {
        database: DatabaseId::new(Uuid::from_u128(0x019eef00000070008000000000000004)).unwrap(),
    }
}

async fn commit_and_reopen(scope: SqlScope) -> SqlScope {
    let SqlScope {
        pool,
        transaction,
        registrations,
        ..
    } = scope;
    transaction.commit().await.unwrap();
    SqlScope {
        transaction: pool.begin().await.unwrap(),
        pool,
        registrations,
        publications: Vec::new(),
        effects: Vec::new(),
    }
}

async fn stored_rows(scope: &mut SqlScope) -> Vec<String> {
    sqlx_core::query::query::<Turso>(
        "SELECT 'session:' || json_array(peer,session_id,record) AS value FROM _fleet_sync_sessions
         UNION ALL SELECT 'notice:' || json_array(peer,session_id,position,body) FROM _fleet_sync_notices
         ORDER BY value"
    ).fetch_all(scope.executor()).await.unwrap().into_iter()
        .map(|row| row.try_get("value").unwrap()).collect()
}

#[tokio::test]
async fn committed_peer_notice_has_the_attached_frame_without_clone_bounds() {
    let mut scope = SqlScope::new().await;
    let connection = binding(2);
    let key = SessionKey {
        peer: connection.peer,
        session_id: session(3),
    };
    attach(&mut scope, &connection, key.session_id).await;
    let effects = publish_notice_to_peer::<_, NoticeProtocol>(
        &mut scope,
        &key.peer,
        Notice::Consumer(NoticePayload {
            message: "a rejected tail is available".into(),
        }),
    )
    .await
    .unwrap();
    // Host effects are held until the transaction commits.
    let mut scope = commit_and_reopen(scope).await;
    assert_eq!(scope.record(&key).await.notice_head.value(), 1);
    let history = notices_after::<_, NoticeProtocol>(&mut scope, &key, NoticePosition::default())
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert!(matches!(&history.first().unwrap().body,
        Notice::Consumer(payload) if payload.message == "a rejected tail is available"));
    assert_eq!(effects.len(), 1);
    let SessionEffect::Send {
        connection: target,
        frame,
    } = effects.into_iter().next().unwrap()
    else {
        panic!("attached peer did not receive a send effect")
    };
    assert_eq!(target, connection.connection);
    let decoded: ServerFrame<NoticeProtocol> =
        serde_json::from_slice(&serde_json::to_vec(&frame).unwrap()).unwrap();
    assert!(matches!(decoded,
        ServerFrame::Notice { position, body: Notice::Consumer(payload) }
        if position.value() == 1 && payload.message == "a rejected tail is available"));
    connection_closed::<_, NoConsumer>(&mut scope, &connection, CloseCause::Dropped, now(2))
        .await
        .unwrap();
    assert!(
        publish_notice_to_peer::<_, NoticeProtocol>(
            &mut scope,
            &key.peer,
            Notice::Consumer(NoticePayload {
                message: "retained while detached".into()
            }),
        )
        .await
        .unwrap()
        .is_empty()
    );
    let mut scope = commit_and_reopen(scope).await;
    assert_eq!(scope.record(&key).await.notice_head.value(), 2);
    let history = notices_after::<_, NoticeProtocol>(&mut scope, &key, NoticePosition::default())
        .await
        .unwrap();
    assert_eq!(history.len(), 2);
    assert!(matches!(&history.last().unwrap().body,
        Notice::Consumer(payload) if payload.message == "retained while detached"));
}

#[tokio::test]
async fn peer_notice_skips_ended_sessions_and_other_peers() {
    let mut scope = SqlScope::new().await;
    let first = binding(2);
    let old = SessionKey {
        peer: first.peer,
        session_id: session(3),
    };
    attach(&mut scope, &first, old.session_id).await;
    publish_notice::<_, NoConsumer>(&mut scope, &old, notice())
        .await
        .unwrap();
    let current = binding(4);
    let live = SessionKey {
        peer: current.peer,
        session_id: session(5),
    };
    attach(&mut scope, &current, live.session_id).await;
    let other = ConnectionBinding {
        peer: PeerId::new(Uuid::from_u128(0x019eef00000070008000000000000009)).unwrap(),
        ..binding(6)
    };
    let other_key = SessionKey {
        peer: other.peer,
        session_id: session(7),
    };
    attach(&mut scope, &other, other_key.session_id).await;
    let old_record = scope.record(&old).await;
    assert!(matches!(old_record.state, SessionState::Ended { .. }));
    let other_record = scope.record(&other_key).await;
    let effects = publish_notice_to_peer::<_, NoConsumer>(&mut scope, &first.peer, notice())
        .await
        .unwrap();
    let mut scope = commit_and_reopen(scope).await;
    assert_eq!(scope.record(&old).await, old_record);
    assert_eq!(scope.record(&other_key).await, other_record);
    assert_eq!(scope.record(&live).await.notice_head.value(), 1);
    assert_eq!(
        notices_after::<_, NoConsumer>(&mut scope, &old, NoticePosition::default())
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        notices_after::<_, NoConsumer>(&mut scope, &other_key, NoticePosition::default())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        matches!(effects.as_slice(), [SessionEffect::Send { connection, frame: ServerFrame::Notice { position, .. } }]
        if *connection == current.connection && position.value() == 1)
    );
}

#[tokio::test]
async fn peer_without_an_open_session_has_no_effects_or_writes() {
    let mut scope = SqlScope::new().await;
    let connection = binding(2);
    attach(&mut scope, &connection, session(3)).await;
    let absent = PeerId::new(Uuid::from_u128(0x019eef00000070008000000000000009)).unwrap();
    let before = stored_rows(&mut scope).await;
    let registrations = scope.registrations.clone();
    assert!(
        publish_notice_to_peer::<_, NoConsumer>(&mut scope, &absent, notice())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(stored_rows(&mut scope).await, before);
    assert_eq!(scope.registrations, registrations);
    let mut scope = commit_and_reopen(scope).await;
    assert_eq!(stored_rows(&mut scope).await, before);
    end_sessions::<_, NoConsumer>(&mut scope, &connection.peer, EndReason::PeerRevoked, now(2))
        .await
        .unwrap();
    let ended = stored_rows(&mut scope).await;
    assert!(
        publish_notice_to_peer::<_, NoConsumer>(&mut scope, &connection.peer, notice())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(stored_rows(&mut scope).await, ended);
    let mut scope = commit_and_reopen(scope).await;
    assert_eq!(stored_rows(&mut scope).await, ended);
}

#[tokio::test]
async fn rolled_back_peer_notice_leaves_no_history_or_position_change() {
    let mut scope = SqlScope::new().await;
    let connection = binding(2);
    let key = SessionKey {
        peer: connection.peer,
        session_id: session(3),
    };
    attach(&mut scope, &connection, key.session_id).await;
    let mut scope = commit_and_reopen(scope).await;
    let before = stored_rows(&mut scope).await;
    let effects = publish_notice_to_peer::<_, NoConsumer>(&mut scope, &connection.peer, notice())
        .await
        .unwrap();
    assert_eq!(effects.len(), 1);
    assert_eq!(scope.record(&key).await.notice_head.value(), 1);
    let SqlScope {
        pool,
        transaction,
        registrations,
        ..
    } = scope;
    transaction.rollback().await.unwrap();
    drop(effects); // The host must discard effects from the rolled-back transaction.
    let mut scope = SqlScope {
        transaction: pool.begin().await.unwrap(),
        pool,
        registrations,
        publications: Vec::new(),
        effects: Vec::new(),
    };
    assert_eq!(stored_rows(&mut scope).await, before);
    assert_eq!(scope.record(&key).await.notice_head.value(), 0);
    assert!(
        notices_after::<_, NoConsumer>(&mut scope, &key, NoticePosition::default())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn peer_notice_uses_the_session_position_and_refuses_exhaustion() {
    let mut scope = SqlScope::new().await;
    let connection = binding(2);
    let key = SessionKey {
        peer: connection.peer,
        session_id: session(3),
    };
    attach(&mut scope, &connection, key.session_id).await;
    let mut record = scope.record(&key).await;
    record.notice_head = NoticePosition::new(u64::MAX - 1);
    crate::session_storage::save_session(&mut scope, &record)
        .await
        .unwrap();
    let effects = publish_notice_to_peer::<_, NoConsumer>(&mut scope, &connection.peer, notice())
        .await
        .unwrap();
    assert!(
        matches!(effects.as_slice(), [SessionEffect::Send { frame: ServerFrame::Notice { position, .. }, .. }]
        if position.value() == u64::MAX)
    );
    assert_eq!(scope.record(&key).await.notice_head.value(), u64::MAX);
    let before = stored_rows(&mut scope).await;
    assert!(matches!(
        publish_notice_to_peer::<_, NoConsumer>(&mut scope, &connection.peer, notice()).await,
        Err(SessionError::NoticePositionExhausted)
    ));
    assert_eq!(stored_rows(&mut scope).await, before);
}
