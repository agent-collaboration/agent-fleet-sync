//! The expected catalog comes from the declared keys and indexes, not the live schema.
use super::support::*;
use durable_steps::{AtomicStore, StoreError, storage::TransactionScope};
use fleet_sync_delivery::DELIVERY_MIGRATOR;
use sqlx_core::{migrate::Migrator, query::query, row::Row, sql_str::AssertSqlSafe};
use sqlx_turso::Turso;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct IndexDefinition {
    name: String,
    table: String,
    columns: Vec<Option<String>>,
    sql: Option<String>,
}
fn normalize(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}

fn declared_indexes() -> Vec<IndexDefinition> {
    let mut indexes = Vec::new();
    // SQLx owns the delivery corpus receipt table; the journal owns its own receipts.
    let keys: &[(&str, &[&[&str]])] = &[
        ("_fleet_sync_delivery_migrations", &[&["version"]]),
        ("_fleet_sync_migrations", &[&["id"], &["version"]]),
        (
            "_fleet_sync_operations",
            &[&["operation_id"], &["kind", "request_key"]],
        ),
        (
            "_fleet_sync_steps",
            &[
                &["id"],
                &["operation_id", "position"],
                &["operation_id", "step_name"],
            ],
        ),
        ("_fleet_sync_schema", &[&["id"], &["version"]]),
        (
            "_fleet_sync_mail_items",
            &[&["item_id"], &["stream", "seq"]],
        ),
        (
            "_fleet_sync_mail_cancel_links",
            &[&["target"], &["cancel_item"]],
        ),
        ("_fleet_sync_delivery_receipts", &[&["call_id"]]),
        ("_fleet_sync_outbox_items", &[&["item_id"]]),
        ("_fleet_sync_inbox_items", &[&["item_id"]]),
        ("_fleet_sync_reports", &[&["item_id"]]),
        (
            "_fleet_sync_outbox_cancel_links",
            &[&["target"], &["cancel_item"]],
        ),
        (
            "_fleet_sync_inbox_cancel_links",
            &[&["target"], &["cancel_item"]],
        ),
        ("_fleet_sync_inbox_state", &[&["singleton"], &["id"]]),
        ("_fleet_sync_outbox_status", &[&["id"], &["singleton"]]),
        ("_fleet_sync_status_state", &[&["id"], &["machine_id"]]),
        (
            "_fleet_sync_status_events",
            &[
                &["id"],
                &["machine_id", "cursor"],
                &["machine_id", "item_id", "version"],
            ],
        ),
        (
            "_fleet_sync_status_owed",
            &[
                &["id"],
                &["item_id", "target_kind", "machine_id", "status_route"],
            ],
        ),
    ];
    for (table, keys) in keys {
        for (position, columns) in keys.iter().enumerate() {
            indexes.push(IndexDefinition {
                name: format!("sqlite_autoindex_{table}_{}", position + 1),
                table: (*table).into(),
                columns: columns
                    .iter()
                    .map(|column| Some((*column).into()))
                    .collect(),
                sql: None,
            });
        }
    }
    for (name, table, columns, sql) in [
        (
            "_fleet_sync_operations_due",
            "_fleet_sync_operations",
            vec![Some("state"), Some("next_attempt_at")],
            "CREATE INDEX _fleet_sync_operations_due ON _fleet_sync_operations(state, next_attempt_at)",
        ),
        (
            "_fleet_sync_mail_deadlines",
            "_fleet_sync_mail_items",
            vec![
                Some("json_extract(record,'$.value.payload.value.validUntil')"),
                Some("item_id"),
            ],
            "CREATE INDEX _fleet_sync_mail_deadlines ON _fleet_sync_mail_items (json_extract(record, '$.value.payload.value.validUntil'), item_id) WHERE live = 1 AND json_extract(record, '$.value.fetched') = 0",
        ),
        (
            "_fleet_sync_inbox_accounting",
            "_fleet_sync_inbox_items",
            vec![
                Some("live"),
                Some("payload_bytes"),
                Some("settlement_charge_bytes"),
            ],
            "CREATE INDEX _fleet_sync_inbox_accounting ON _fleet_sync_inbox_items(live,payload_bytes,settlement_charge_bytes)",
        ),
        (
            "_fleet_sync_status_owed_machine",
            "_fleet_sync_status_owed",
            vec![Some("machine_id"), Some("item_id")],
            "CREATE INDEX _fleet_sync_status_owed_machine ON _fleet_sync_status_owed(machine_id, item_id)",
        ),
    ] {
        indexes.push(IndexDefinition {
            name: name.into(),
            table: table.into(),
            columns: columns
                .into_iter()
                .map(|column| column.map(str::to_owned))
                .collect(),
            sql: Some(normalize(sql)),
        });
    }
    indexes.sort();
    indexes
}

async fn catalog(store: &TestStore) -> Vec<IndexDefinition> {
    store.transaction(|scope|Box::pin(async move {
        let rows=query::<Turso>("SELECT name,tbl_name,sql FROM sqlite_master WHERE type='index' AND tbl_name GLOB '_fleet_sync_*' ORDER BY name").fetch_all(scope.executor()).await?;
        let mut indexes=Vec::new();
        for row in rows {
            let name:String=row.try_get("name")?;
            let sql=format!("PRAGMA index_info(\"{}\")",name.replace('"',"\"\""));
            let columns=query::<Turso>(AssertSqlSafe(sql)).fetch_all(scope.executor()).await?.into_iter().map(|row|row.try_get::<Option<String>,_>("name").map(|name|name.map(|name|normalize(&name)))).collect::<Result<Vec<_>,_>>()?;
            indexes.push(IndexDefinition{name,table:row.try_get("tbl_name")?,columns,sql:row.try_get::<Option<String>,_>("sql")?.map(|sql|normalize(&sql))});
        }
        indexes.sort();Ok(indexes)
    })).await.unwrap()
}

async fn assert_catalog(upgrade_v04: bool) {
    let fixture = Fixture::new().await;
    let store = TestStore::open_and_migrate(
        fixture.directory.path().join("index-catalog.db"),
        fixture.host.clone(),
    )
    .await
    .unwrap();
    store
        .transaction(move |scope| {
            Box::pin(async move {
                if upgrade_v04 {
                    let mut old = Migrator::with_migrations(
                        DELIVERY_MIGRATOR
                            .iter()
                            .filter(|migration| migration.version <= 3)
                            .cloned()
                            .collect(),
                    );
                    old.dangerous_set_table_name("_fleet_sync_delivery_migrations");
                    old.run_direct(None, scope.executor(), false)
                        .await
                        .map_err(StoreError::backend)?;
                }
                let mut all =
                    Migrator::with_migrations(DELIVERY_MIGRATOR.iter().cloned().collect());
                all.dangerous_set_table_name("_fleet_sync_delivery_migrations");
                all.run_direct(None, scope.executor(), false)
                    .await
                    .map_err(StoreError::backend)?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let actual = catalog(&store).await;
    let expected = declared_indexes();
    let missing = expected
        .iter()
        .filter(|index| !actual.contains(index))
        .collect::<Vec<_>>();
    let unexpected = actual
        .iter()
        .filter(|index| !expected.contains(index))
        .collect::<Vec<_>>();
    assert!(
        actual == expected,
        "upgrade_v04={upgrade_v04}; missing={missing:?}; unexpected={unexpected:?}"
    );
}

#[tokio::test]
async fn fresh_store_keeps_the_complete_declared_index_catalog() {
    assert_catalog(false).await;
}

#[tokio::test]
async fn v04_upgraded_store_keeps_the_complete_declared_index_catalog() {
    assert_catalog(true).await;
}
