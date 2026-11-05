#![allow(clippy::unwrap_used)]
use super::*;
use crate::rule_test_support::*;
use sqlx_core::{migrate::Migrator, pool::PoolOptions, query::query, row::Row};
use sqlx_turso::{Turso, TursoConnectOptions};
use std::collections::BTreeMap;

#[derive(serde::Serialize, serde::Deserialize)]
struct OpaqueNoticeBody {
    message: String,
    database: String,
}
struct NoticeProtocol;
impl ConsumerProtocol for NoticeProtocol {
    type Delivery = NoDelivery;
    type Request = ();
    type Answer = ();
    type ServerRequest = ();
    type ClientAnswer = ();
    type Notice = OpaqueNoticeBody;
    type Annotation = ();
}

#[tokio::test]
async fn legacy_sessions_notices_and_attach_sequences_migrate_without_rounding_full_u64() {
    let pool = PoolOptions::<Turso>::new()
        .max_connections(1)
        .connect_with(TursoConnectOptions::new())
        .await
        .unwrap();
    let mut transaction = pool.begin().await.unwrap();
    Migrator::with_migrations(
        SESSION_MIGRATOR
            .sql_migrations()
            .filter(|m| m.version <= 202610180001)
            .cloned()
            .collect::<Vec<_>>(),
    )
    .run_direct(None, &mut *transaction, false)
    .await
    .unwrap();
    let key = SessionKey {
        peer: binding(2).peer,
        session_id: session(3),
    };
    let record = SessionRecord {
        peer: key.peer,
        session_id: key.session_id,
        state: SessionState::Detached { since: now(123) },
        notice_head: NoticePosition::new(u64::MAX),
    };
    query::<Turso>("INSERT INTO _fleet_sync_sessions(peer,session_id,record) VALUES(?1,?2,?3)")
        .bind(key.peer.to_string())
        .bind(key.session_id.to_string())
        .bind(serde_json::to_string(&record).unwrap())
        .execute(&mut *transaction)
        .await
        .unwrap();
    let counter =
        query::<Turso>("SELECT record -> '$.noticeHead' AS counter FROM _fleet_sync_sessions")
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
    assert_eq!(
        counter.try_get::<String, _>("counter").unwrap(),
        "18446744073709551615"
    );
    let body = Notice::<NoticeProtocol>::Consumer(OpaqueNoticeBody {
        message: "opaque consumer notice".into(),
        database: "consumer owns this field".into(),
    });
    query::<Turso>(
        "INSERT INTO _fleet_sync_notices(peer,session_id,position,body) VALUES(?1,?2,?3,?4)",
    )
    .bind(key.peer.to_string())
    .bind(key.session_id.to_string())
    .bind("18446744073709551615")
    .bind(serde_json::to_string(&body).unwrap())
    .execute(&mut *transaction)
    .await
    .unwrap();
    query::<Turso>("INSERT INTO _fleet_sync_attach_sequences(peer,sequence) VALUES(?1,?2)")
        .bind(key.peer.to_string())
        .bind("18446744073709551615")
        .execute(&mut *transaction)
        .await
        .unwrap();
    SESSION_MIGRATOR
        .run_direct(None, &mut *transaction, false)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let mut scope = SqlScope {
        transaction: pool.begin().await.unwrap(),
        pool,
        registrations: BTreeMap::new(),
        publications: Vec::new(),
        effects: Vec::new(),
    };
    assert_eq!(
        load_session(&mut scope, &key).await.unwrap().unwrap(),
        record
    );
    let notices = load_notices_after::<_, NoticeProtocol>(
        &mut scope,
        &key,
        NoticePosition::new(u64::MAX - 1),
    )
    .await
    .unwrap();
    assert_eq!(notices.len(), 1);
    assert!(
        matches!(&notices.first().unwrap().body,Notice::Consumer(body) if body.message=="opaque consumer notice" && body.database=="consumer owns this field")
    );
    assert_eq!(notices.first().unwrap().position.value(), u64::MAX);
    assert!(matches!(
        next_attach_sequence(
            &mut scope,
            key.peer,
            fleet_sync_protocol::SyncTime::from_unix_millis(0)
        )
        .await,
        Err(SessionError::AttachSequenceExhausted)
    ));
    let row=query::<Turso>("SELECT id,notice_head,state_kind,detached_since,created_at,updated_at FROM _fleet_sync_sessions").fetch_one(scope.executor()).await.unwrap();
    let id = row.try_get::<String, _>("id").unwrap();
    assert_eq!(uuid::Uuid::parse_str(&id).unwrap().get_version_num(), 7);
    assert_eq!(
        row.try_get::<String, _>("notice_head").unwrap(),
        "18446744073709551615"
    );
    assert_eq!(row.try_get::<String, _>("state_kind").unwrap(), "detached");
    assert_eq!(row.try_get::<i64, _>("detached_since").unwrap(), 123);
    let created = row.try_get::<i64, _>("created_at").unwrap();
    assert_eq!(
        created, 123,
        "legacy detached time is the best recorded session time"
    );
    assert!(row.try_get::<i64, _>("updated_at").unwrap() >= created);
    save_session(
        &mut scope,
        &record,
        fleet_sync_protocol::SyncTime::from_unix_millis(0),
    )
    .await
    .unwrap();
    let row = query::<Turso>("SELECT id,created_at FROM _fleet_sync_sessions")
        .fetch_one(scope.executor())
        .await
        .unwrap();
    assert_eq!(row.try_get::<String, _>("id").unwrap(), id);
    assert_eq!(row.try_get::<i64, _>("created_at").unwrap(), created);
    let columns=query::<Turso>("SELECT name FROM pragma_table_info('_fleet_sync_sessions') UNION ALL SELECT name FROM pragma_table_info('_fleet_sync_notices')").fetch_all(scope.executor()).await.unwrap();
    for column in columns {
        let name = column.try_get::<String, _>("name").unwrap();
        assert!(name != "record" && name != "body");
    }
}

#[tokio::test]
async fn typed_session_surrogate_id_refuses_notice_mutation_without_repair() {
    let mut scope = SqlScope::new().await;
    let connection = binding(2);
    let key = SessionKey {
        peer: connection.peer,
        session_id: session(3),
    };
    attach(&mut scope, &connection, key.session_id).await;
    query::<Turso>("UPDATE _fleet_sync_sessions SET id='019eef00-0000-4000-8000-000000000099'")
        .execute(scope.executor())
        .await
        .unwrap();
    let before=query::<Turso>("SELECT json_array(id,peer,session_id,state_kind,connection_id,detached_since,end_reason,notice_head,created_at,updated_at) AS value FROM _fleet_sync_sessions").fetch_one(scope.executor()).await.unwrap().try_get::<String,_>("value").unwrap();
    let answer = publish_notice_to_peer::<_, NoConsumer>(
        &mut scope,
        &key.peer,
        Notice::LeaseChanged {
            database: DatabaseId::try_from("019eef00-0000-7000-8000-000000000041".to_owned())
                .unwrap(),
        },
        fleet_sync_protocol::SyncTime::from_unix_millis(0),
    )
    .await;
    assert!(
        answer.is_err(),
        "a v4 surrogate id must refuse before notice or cursor writes"
    );
    assert_eq!(query::<Turso>("SELECT json_array(id,peer,session_id,state_kind,connection_id,detached_since,end_reason,notice_head,created_at,updated_at) AS value FROM _fleet_sync_sessions").fetch_one(scope.executor()).await.unwrap().try_get::<String,_>("value").unwrap(),before);
    assert_eq!(
        query::<Turso>("SELECT count(*) AS count FROM _fleet_sync_notices")
            .fetch_one(scope.executor())
            .await
            .unwrap()
            .try_get::<i64, _>("count")
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn typed_notice_surrogate_id_refuses_history_read_without_repair() {
    let mut scope = SqlScope::new().await;
    let connection = binding(2);
    let key = SessionKey {
        peer: connection.peer,
        session_id: session(3),
    };
    attach(&mut scope, &connection, key.session_id).await;
    publish_notice_to_peer::<_, NoConsumer>(
        &mut scope,
        &key.peer,
        Notice::LeaseChanged {
            database: DatabaseId::try_from("019eef00-0000-7000-8000-000000000041".to_owned())
                .unwrap(),
        },
        fleet_sync_protocol::SyncTime::from_unix_millis(0),
    )
    .await
    .unwrap();
    query::<Turso>("UPDATE _fleet_sync_notices SET id='garbage'")
        .execute(scope.executor())
        .await
        .unwrap();
    let before=query::<Turso>("SELECT json_array(id,peer,session_id,position,notice_kind,database_id,consumer_body,created_at,updated_at) AS value FROM _fleet_sync_notices").fetch_one(scope.executor()).await.unwrap().try_get::<String,_>("value").unwrap();
    assert!(
        notices_after::<_, NoConsumer>(&mut scope, &key, NoticePosition::default())
            .await
            .is_err(),
        "garbage surrogate id was accepted"
    );
    assert_eq!(query::<Turso>("SELECT json_array(id,peer,session_id,position,notice_kind,database_id,consumer_body,created_at,updated_at) AS value FROM _fleet_sync_notices").fetch_one(scope.executor()).await.unwrap().try_get::<String,_>("value").unwrap(),before);
}
