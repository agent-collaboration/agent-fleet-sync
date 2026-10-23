#![allow(clippy::unwrap_used)]

use fleet_sync_delivery::DELIVERY_MIGRATOR;
use sqlx_core::migrate::Migrator;
use sqlx_turso::{
    Turso, TursoConnectOptions, TursoConnection,
    sqlx::{ConnectOptions, Connection, Row, query},
};
use std::{path::PathBuf, time::Instant};

const ACCOUNTING_SQL: &str = include_str!("../sql/read_inbox_accounting.sql");
const INDEX_NAME: &str = "_fleet_sync_inbox_accounting";
const LARGE_ROWS: u64 = 250;
const PAYLOAD_BYTES: u64 = 65_536;
const SETTLEMENT_BYTES: u64 = 4_096;
const REPETITIONS: u32 = 10;

#[derive(Debug, Eq, PartialEq)]
struct AccountingTotals {
    items: Option<String>,
    bytes: Option<String>,
}

async fn accounting(connection: &mut TursoConnection) -> AccountingTotals {
    let row = query::<Turso>(ACCOUNTING_SQL)
        .fetch_one(connection)
        .await
        .unwrap();
    AccountingTotals {
        items: row.try_get("items").unwrap(),
        bytes: row.try_get("bytes").unwrap(),
    }
}

async fn query_plan(connection: &mut TursoConnection) -> Vec<String> {
    query::<Turso>(concat!(
        "EXPLAIN QUERY PLAN ",
        include_str!("../sql/read_inbox_accounting.sql")
    ))
    .fetch_all(connection)
    .await
    .unwrap()
    .iter()
    .map(|row| row.try_get("detail").unwrap())
    .collect()
}

async fn migrate(connection: &mut TursoConnection, through_previous: bool) {
    let migrations = DELIVERY_MIGRATOR
        .iter()
        .filter(|migration| !through_previous || migration.version <= 2)
        .cloned()
        .collect::<Vec<_>>();
    let mut migrator = Migrator::with_migrations(migrations);
    migrator.dangerous_set_table_name("_fleet_sync_delivery_migrations");
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await.unwrap();
    migrator
        .run_direct(None, &mut *transaction, false)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
}

async fn timed_accounting(connection: &mut TursoConnection, expected: &AccountingTotals) -> f64 {
    let started = Instant::now();
    for _ in 0..REPETITIONS {
        assert_eq!(&accounting(connection).await, expected);
    }
    started.elapsed().as_secs_f64() * 1_000.0 / f64::from(REPETITIONS)
}

fn assert_covering_plan(details: &[String]) {
    assert!(
        details
            .iter()
            .any(|detail| detail.contains(&format!("USING COVERING INDEX {INDEX_NAME}"))),
        "accounting must use the covering metadata index: {details:?}"
    );
    assert!(
        !details.iter().any(|detail| {
            detail.contains("SCAN _fleet_sync_inbox_items")
                && !detail.contains("USING COVERING INDEX")
        }),
        "accounting must not scan the payload table: {details:?}"
    );
}

#[tokio::test]
async fn inbox_accounting_upgrade_preserves_totals_and_uses_covering_metadata() {
    // Retain this same-file before/after fixture beside the checkout's proof
    // receipts. Physical payloads isolate query cost; they are not SDK custody.
    let scratch_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tmp");
    std::fs::create_dir_all(&scratch_root).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("d917-inbox-accounting-")
        .tempdir_in(scratch_root)
        .unwrap()
        .keep();
    let database_path = directory.join("accounting.db");
    let mut connection = TursoConnectOptions::new()
        .filename(&database_path)
        .create_if_missing(true)
        .connect()
        .await
        .unwrap();
    migrate(&mut connection, true).await;
    let payload = "p".repeat(usize::try_from(PAYLOAD_BYTES).unwrap());
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await.unwrap();
    for identity in 0..LARGE_ROWS {
        query::<Turso>("INSERT INTO _fleet_sync_inbox_items(item_id,live,payload_bytes,settlement_bytes,record,payload) VALUES(?1,1,?2,?3,'{}',?4)")
            .bind(format!("physical-{identity}"))
            .bind(i64::try_from(PAYLOAD_BYTES).unwrap())
            .bind(i64::try_from(SETTLEMENT_BYTES).unwrap())
            .bind(&payload)
            .execute(&mut *transaction).await.unwrap();
    }
    // Terminal rows do not count toward usage, but their metadata must still
    // be validated by the unchanged aggregate's corruption guard.
    query::<Turso>("INSERT INTO _fleet_sync_inbox_items VALUES('terminal',0,0,0,'{}',NULL)")
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let expected = AccountingTotals {
        items: Some(LARGE_ROWS.to_string()),
        bytes: Some((LARGE_ROWS * (PAYLOAD_BYTES + SETTLEMENT_BYTES)).to_string()),
    };
    let before_plan = query_plan(&mut connection).await;
    let before_ms = timed_accounting(&mut connection, &expected).await;
    println!("accounting fixture: {}", database_path.display());
    println!(
        "before: rows={LARGE_ROWS}, payload_bytes={PAYLOAD_BYTES}, repetitions={REPETITIONS}, totals={expected:?}, mean_ms={before_ms:.3}, plan={before_plan:?}"
    );
    query::<Turso>("UPDATE _fleet_sync_inbox_items SET payload_bytes=-1 WHERE item_id='terminal'")
        .execute(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        accounting(&mut connection).await,
        AccountingTotals {
            items: Some(LARGE_ROWS.to_string()),
            bytes: None
        }
    );
    query::<Turso>("UPDATE _fleet_sync_inbox_items SET payload_bytes=0 WHERE item_id='terminal'")
        .execute(&mut connection)
        .await
        .unwrap();

    migrate(&mut connection, false).await;
    let after_plan = query_plan(&mut connection).await;
    let after_ms = timed_accounting(&mut connection, &expected).await;
    println!(
        "after: rows={LARGE_ROWS}, payload_bytes={PAYLOAD_BYTES}, repetitions={REPETITIONS}, totals={expected:?}, mean_ms={after_ms:.3}, plan={after_plan:?}"
    );
    assert_covering_plan(&after_plan);
    query::<Turso>("UPDATE _fleet_sync_inbox_items SET payload_bytes=-1 WHERE item_id='terminal'")
        .execute(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        accounting(&mut connection).await,
        AccountingTotals {
            items: Some(LARGE_ROWS.to_string()),
            bytes: None
        }
    );
    connection.close().await.unwrap();
}
