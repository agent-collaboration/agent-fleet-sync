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
const DATABASE: &str = "019eef00-0000-7000-8000-000000000041";
async fn schema(connection: &mut TursoConnection) -> Vec<(String, String)> {
    query::<Turso>("SELECT name,sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY name")
        .fetch_all(connection)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.try_get("name").unwrap(), r.try_get("sql").unwrap()))
        .collect()
}
async fn receipts(connection: &mut TursoConnection) -> Vec<(i64, i64, String, i64)> {
    query::<Turso>("SELECT version,CAST(success AS INTEGER) AS success,hex(checksum) AS checksum,execution_time FROM _sqlx_migrations ORDER BY version").fetch_all(connection).await.unwrap().into_iter().map(|r|(r.try_get("version").unwrap(),r.try_get("success").unwrap(),r.try_get("checksum").unwrap(),r.try_get("execution_time").unwrap())).collect()
}
async fn migration_case_using(case: &str, composed: bool) {
    let mut connection = TursoConnectOptions::new().connect().await.unwrap();
    let mut setup = connection.begin_with("BEGIN IMMEDIATE").await.unwrap();
    Migrator::with_migrations(
        LEASE_MIGRATOR
            .sql_migrations()
            .filter(|m| m.version <= 4)
            .cloned()
            .collect::<Vec<_>>(),
    )
    .run_direct(None, &mut *setup, false)
    .await
    .unwrap();
    let mut body = json!({"database":DATABASE,"lifecycle":{"state":"registered","origin":{"kind":"empty"},"importedFrom":null},"revision":{"epoch":1,"change":1},"changed_at":1000});
    let mut lease = json!({"database":DATABASE,"holder":"019eef00-0000-7000-8000-000000000001","epoch":1,"expires_at":3000,"last_renewed_at":1000});
    match case {
        "identity" => body["database"] = json!("019eef00-0000-7000-8000-000000000042"),
        "lease_database" => lease["database"] = json!("019eef00-0000-7000-8000-000000000042"),
        "epoch" => body["revision"]["epoch"] = json!(2),
        "counter" => body["revision"]["change"] = json!("not a u64"),
        "state" => body["lifecycle"] = json!({"state":"unknownVariant"}),
        "holder" => lease["holder"] = json!("019eef00-0000-4000-8000-000000000001"),
        _ => {}
    }
    let other_body=json!({"database":"019eef00-0000-7000-8000-000000000042","lifecycle":{"state":"registered","origin":{"kind":"empty"},"importedFrom":null},"revision":{"epoch":1,"change":1},"changed_at":1000}).to_string();
    let other_lease=json!({"database":"019eef00-0000-7000-8000-000000000042","holder":"019eef00-0000-7000-8000-000000000002","epoch":1,"expires_at":3000,"last_renewed_at":1000}).to_string();
    query::<Turso>("INSERT INTO _fleet_sync_databases(database,record) VALUES('019eef00-0000-7000-8000-000000000042',?1)").bind(other_body).execute(&mut *setup).await.unwrap();
    query::<Turso>("INSERT INTO _fleet_sync_leases(database,record) VALUES('019eef00-0000-7000-8000-000000000042',?1)").bind(other_lease).execute(&mut *setup).await.unwrap();
    let original = body.to_string();
    let original_lease = lease.to_string();
    query::<Turso>("INSERT INTO _fleet_sync_databases(database,record) VALUES(?1,?2)")
        .bind(DATABASE)
        .bind(&original)
        .execute(&mut *setup)
        .await
        .unwrap();
    query::<Turso>("INSERT INTO _fleet_sync_leases(database,record) VALUES(?1,?2)")
        .bind(DATABASE)
        .bind(&original_lease)
        .execute(&mut *setup)
        .await
        .unwrap();
    setup.commit().await.unwrap();
    let before = schema(&mut connection).await;
    let before_rows = legacy_rows(&mut connection).await;
    let before_receipts = receipts(&mut connection).await;
    let result = if composed {
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await.unwrap();
        query::<Turso>("CREATE TABLE caller_migration_marker(value INTEGER)")
            .execute(&mut *transaction)
            .await
            .unwrap();
        let result = async {
            validate_legacy_rows(&mut super::ConnectionScope(&mut *transaction)).await?;
            Migrator::with_migrations(LEASE_MIGRATOR.sql_migrations().cloned().collect::<Vec<_>>())
                .run_direct(None, &mut *transaction, false)
                .await?;
            Ok::<(), LeaseMigrationError>(())
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
        LEASE_MIGRATOR
            .run_direct(None, &mut connection, false)
            .await
    };
    eprintln!("OBS lease {case}: accepted={}", result.is_ok());
    if case == "valid" {
        result.unwrap();
    } else {
        assert!(
            result.is_err(),
            "public lease migrator accepted legacy {case} corruption"
        );
        let expected_table = if case == "holder" || case == "lease_database" {
            "_fleet_sync_leases"
        } else {
            "_fleet_sync_databases"
        };
        let error = result.err().unwrap();
        assert!(
            matches!(error,LeaseMigrationError::InvalidLegacyRow {table,key,..} if table==expected_table && key.contains(DATABASE))
        );
        assert_eq!(schema(&mut connection).await, before);
        assert_eq!(legacy_rows(&mut connection).await, before_rows);
        assert_eq!(
            receipts(&mut connection).await,
            before_receipts,
            "failed receipt survived {case}"
        );
        assert_eq!(
            query::<Turso>("SELECT record FROM _fleet_sync_databases WHERE database='019eef00-0000-7000-8000-000000000041'")
                .fetch_one(&mut connection)
                .await
                .unwrap()
                .try_get::<String, _>("record")
                .unwrap(),
            original
        );
        assert_eq!(
            query::<Turso>("SELECT record FROM _fleet_sync_leases WHERE database='019eef00-0000-7000-8000-000000000041'")
                .fetch_one(&mut connection)
                .await
                .unwrap()
                .try_get::<String, _>("record")
                .unwrap(),
            original_lease
        );
    }
}
#[tokio::test]
async fn public_lease_migrator_accepts_valid_legacy_rows() {
    migration_case("valid").await;
}
#[tokio::test]
async fn public_lease_migrator_refuses_legacy_identity_atomically() {
    migration_case("identity").await;
}
#[tokio::test]
async fn public_lease_migrator_refuses_legacy_epoch_mismatch_atomically() {
    migration_case("epoch").await;
}
#[tokio::test]
async fn public_lease_migrator_refuses_legacy_counter_corruption_atomically() {
    migration_case("counter").await;
}
#[tokio::test]
async fn public_lease_migrator_refuses_legacy_unknown_state_atomically() {
    migration_case("state").await;
}
#[tokio::test]
async fn public_lease_migrator_refuses_legacy_v4_holder_atomically() {
    migration_case("holder").await;
}

#[tokio::test]
async fn public_lease_migrator_refuses_legacy_lease_database_mismatch_atomically() {
    migration_case("lease_database").await;
}

async fn legacy_rows(connection: &mut TursoConnection) -> Vec<(String, String, String)> {
    query::<Turso>("SELECT 'database' AS owner,database,record FROM _fleet_sync_databases UNION ALL SELECT 'lease',database,record FROM _fleet_sync_leases ORDER BY owner,database").fetch_all(connection).await.unwrap().into_iter().map(|r|(r.try_get("owner").unwrap(),r.try_get("database").unwrap(),r.try_get("record").unwrap())).collect()
}

async fn migration_case(case: &str) {
    migration_case_using(case, false).await;
}
#[tokio::test]
async fn composed_bootstrap_preflight_refuses_legacy_corruption_in_the_caller_transaction() {
    migration_case_using("lease_database", true).await;
}
