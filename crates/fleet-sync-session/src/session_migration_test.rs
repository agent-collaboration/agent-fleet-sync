#![allow(clippy::unwrap_used)]
use crate::*;
use serde_json::json;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    migrate::Migrator,
    query::query,
    row::Row,
};
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection};
const PEER: &str = "019eef00-0000-7000-8000-000000000001";
const SESSION: &str = "019eef00-0000-7000-8000-000000000003";
async fn schema(connection: &mut TursoConnection) -> Vec<(String, String)> {
    query::<Turso>("SELECT name,sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY name")
        .fetch_all(connection)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.try_get("name").unwrap(), r.try_get("sql").unwrap()))
        .collect()
}
async fn migration_case_using(case: &str, composed: bool) {
    let mut connection = TursoConnectOptions::new().connect().await.unwrap();
    let mut setup = connection.begin_with("BEGIN IMMEDIATE").await.unwrap();
    Migrator::with_migrations(
        SESSION_MIGRATOR
            .sql_migrations()
            .filter(|m| m.version <= 202610180001)
            .cloned()
            .collect::<Vec<_>>(),
    )
    .run_direct(None, &mut *setup, false)
    .await
    .unwrap();
    let mut body = json!({"peer":PEER,"sessionId":SESSION,"state":{"kind":"detached","since":123},"noticeHead":17});
    match case {
        "peer" => body["peer"] = json!("019eef00-0000-7000-8000-000000000002"),
        "session" => body["sessionId"] = json!("019eef00-0000-7000-8000-000000000004"),
        "counter" => body["noticeHead"] = json!("not a u64"),
        "state" => body["state"] = json!({"kind":"unknownVariant","since":123}),
        "connection" => {
            body["state"] =
                json!({"kind":"active","connection":"019eef00-0000-4000-8000-000000000004"})
        }
        _ => {}
    }
    let valid_other=json!({"peer":"019eef00-0000-7000-8000-000000000002","sessionId":"019eef00-0000-7000-8000-000000000004","state":{"kind":"detached","since":124},"noticeHead":18}).to_string();
    query::<Turso>("INSERT INTO _fleet_sync_sessions(peer,session_id,record) VALUES('019eef00-0000-7000-8000-000000000002','019eef00-0000-7000-8000-000000000004',?1)").bind(valid_other).execute(&mut *setup).await.unwrap();
    let original = body.to_string();
    query::<Turso>("INSERT INTO _fleet_sync_sessions(peer,session_id,record) VALUES(?1,?2,?3)")
        .bind(PEER)
        .bind(SESSION)
        .bind(&original)
        .execute(&mut *setup)
        .await
        .unwrap();
    setup.commit().await.unwrap();
    let before = schema(&mut connection).await;
    let before_rows = legacy_rows(&mut connection).await;
    let before_receipts = receipts(&mut connection).await;
    // Exercise the public entry point with no caller transaction to protect it.
    let result = if composed {
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await.unwrap();
        query::<Turso>("CREATE TABLE caller_migration_marker(value INTEGER)")
            .execute(&mut *transaction)
            .await
            .unwrap();
        let result = async {
            validate_legacy_rows(&mut super::ConnectionScope(&mut *transaction)).await?;
            Migrator::with_migrations(
                SESSION_MIGRATOR
                    .sql_migrations()
                    .cloned()
                    .collect::<Vec<_>>(),
            )
            .run_direct(None, &mut *transaction, false)
            .await?;
            Ok::<(), SessionMigrationError>(())
        }
        .await;
        match result {
            Ok(()) => {
                transaction.commit().await.unwrap();
                Ok(())
            }
            Err(error) => {
                transaction.rollback().await.unwrap();
                Err(error)
            }
        }
    } else {
        SESSION_MIGRATOR
            .run_direct(None, &mut connection, false)
            .await
    };
    if case == "valid" {
        result.unwrap();
        let row = query::<Turso>(
            "SELECT peer,session_id,state_kind,notice_head FROM _fleet_sync_sessions WHERE peer='019eef00-0000-7000-8000-000000000001'",
        )
        .fetch_one(&mut connection)
        .await
        .unwrap();
        assert_eq!(row.try_get::<String, _>("peer").unwrap(), PEER);
        assert_eq!(row.try_get::<String, _>("session_id").unwrap(), SESSION);
        assert_eq!(row.try_get::<String, _>("state_kind").unwrap(), "detached");
        assert_eq!(row.try_get::<String, _>("notice_head").unwrap(), "17");
    } else {
        assert!(
            result.is_err(),
            "public migrator accepted legacy {case} corruption"
        );
        let error = result.err().unwrap();
        assert!(
            matches!(error,SessionMigrationError::InvalidLegacyRow {table,key,..} if table=="_fleet_sync_sessions" && key.contains(PEER) && key.contains(SESSION))
        );
        assert_eq!(schema(&mut connection).await, before);
        assert_eq!(legacy_rows(&mut connection).await, before_rows);
        assert_eq!(receipts(&mut connection).await, before_receipts);
        assert_eq!(
            query::<Turso>("SELECT record FROM _fleet_sync_sessions WHERE peer='019eef00-0000-7000-8000-000000000001'")
                .fetch_one(&mut connection)
                .await
                .unwrap()
                .try_get::<String, _>("record")
                .unwrap(),
            original
        );
    }
}
#[tokio::test]
async fn public_migrator_accepts_valid_legacy_session() {
    migration_case("valid").await;
}
#[tokio::test]
async fn public_migrator_refuses_legacy_peer_mismatch_atomically() {
    migration_case("peer").await;
}
#[tokio::test]
async fn public_migrator_refuses_legacy_session_mismatch_atomically() {
    migration_case("session").await;
}
#[tokio::test]
async fn public_migrator_refuses_legacy_counter_corruption_atomically() {
    migration_case("counter").await;
}
#[tokio::test]
async fn public_migrator_refuses_legacy_unknown_state_atomically() {
    migration_case("state").await;
}
#[tokio::test]
async fn public_migrator_refuses_legacy_v4_connection_atomically() {
    migration_case("connection").await;
}

async fn legacy_rows(connection: &mut TursoConnection) -> Vec<(String, String, String)> {
    query::<Turso>(
        "SELECT peer,session_id,record FROM _fleet_sync_sessions ORDER BY peer,session_id",
    )
    .fetch_all(connection)
    .await
    .unwrap()
    .into_iter()
    .map(|r| {
        (
            r.try_get("peer").unwrap(),
            r.try_get("session_id").unwrap(),
            r.try_get("record").unwrap(),
        )
    })
    .collect()
}
async fn receipts(connection: &mut TursoConnection) -> Vec<(i64, i64, String, i64)> {
    query::<Turso>("SELECT version,CAST(success AS INTEGER) AS success,hex(checksum) AS checksum,execution_time FROM _sqlx_migrations ORDER BY version").fetch_all(connection).await.unwrap().into_iter().map(|r|(r.try_get("version").unwrap(),r.try_get("success").unwrap(),r.try_get("checksum").unwrap(),r.try_get("execution_time").unwrap())).collect()
}

async fn migration_case(case: &str) {
    migration_case_using(case, false).await;
}
#[tokio::test]
async fn composed_bootstrap_preflight_refuses_legacy_corruption_in_the_caller_transaction() {
    migration_case_using("peer", true).await;
}
