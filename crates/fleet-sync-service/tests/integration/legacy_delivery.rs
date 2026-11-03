//! Independent v0.4.0 rows exercise every UUID-derived creation-time backfill.
use sqlx_core::{executor::Executor, query::query, row::Row};
use sqlx_turso::{Turso, TursoConnection};

type Result = std::result::Result<(), Box<dyn std::error::Error>>;
const ITEMS: [(&str, &str, i64); 2] = [
    (
        "12345678-9abc-7001-8001-0123456789ab",
        "12345678-9abc-7001-8001-0123456789ac",
        0x1234_5678_9abc,
    ),
    (
        "fedcba98-7650-7001-8001-0123456789ab",
        "fedcba98-7650-7001-8001-0123456789ac",
        0xfedc_ba98_7650,
    ),
];

pub(super) async fn seed(connection: &mut TursoConnection) -> Result {
    connection
        .execute(include_str!("fixtures/v0_4_delivery.sql"))
        .await?;
    Ok(())
}

pub(super) async fn assert_preserved(connection: &mut TursoConnection) -> Result {
    for (item, target, expected) in ITEMS {
        for (table, key) in [
            ("_fleet_sync_outbox_items", "item_id"),
            ("_fleet_sync_inbox_items", "item_id"),
            ("_fleet_sync_reports", "item_id"),
            ("_fleet_sync_outbox_cancel_links", "cancel_item"),
            ("_fleet_sync_inbox_cancel_links", "cancel_item"),
        ] {
            let sql = format!("SELECT created_at,updated_at FROM {table} WHERE {key}=?1");
            let row = query::<Turso>(sqlx_core::sql_str::AssertSqlSafe(sql))
                .bind(item)
                .fetch_one(&mut *connection)
                .await?;
            assert_eq!(row.try_get::<i64, _>("created_at")?, expected, "{table}");
            assert_eq!(row.try_get::<i64, _>("updated_at")?, expected, "{table}");
        }
        let row = query::<Turso>(
            "SELECT report_seq,outcome_acceptance FROM _fleet_sync_reports WHERE item_id=?1",
        )
        .bind(item)
        .fetch_one(&mut *connection)
        .await?;
        assert_eq!(row.try_get::<i64, _>("report_seq")?, 1);
        assert_eq!(
            row.try_get::<String, _>("outcome_acceptance")?,
            "{\"counter\":7}"
        );
        for sql in [
            "SELECT target FROM _fleet_sync_outbox_cancel_links WHERE cancel_item=?1",
            "SELECT target FROM _fleet_sync_inbox_cancel_links WHERE cancel_item=?1",
        ] {
            let row = query::<Turso>(sql)
                .bind(item)
                .fetch_one(&mut *connection)
                .await?;
            assert_eq!(row.try_get::<String, _>("target")?, target);
        }
    }
    Ok(())
}
