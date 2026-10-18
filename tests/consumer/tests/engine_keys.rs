use sqlx_core::{
    connection::{ConnectOptions, Connection},
    migrate::Migrator,
    row::Row,
    sql_str::AssertSqlSafe,
};
use sqlx_turso::{Turso, TursoConnectOptions};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

// These are the audited engine tables, not consumer tables or migration journals.
// Runtime table_info supplies their actual key types and composite-key columns.
#[tokio::test]
async fn audited_engine_keys_declare_not_null_and_reject_null_without_rowid_assignment()
-> TestResult {
    let schemas: &[(&str, Migrator, &[&str])] = &[
        (
            "journal",
            sqlx::migrate!("../../crates/durable-steps/migrations"),
            &[
                "_fleet_sync_operations",
                "_fleet_sync_steps",
                "_fleet_sync_schema",
            ],
        ),
        (
            "lease",
            sqlx::migrate!("../../crates/fleet-sync-lease/migrations"),
            &[
                "_fleet_sync_databases",
                "_fleet_sync_leases",
                "_fleet_sync_epoch_floor",
                "_fleet_sync_renewal_attempts",
                "_fleet_sync_epoch_history",
                "_fleet_sync_transfers",
                "_fleet_sync_takeover_previews",
                "_fleet_sync_lifecycle_attempts",
                "_fleet_sync_transfer_admissions",
                "_fleet_sync_transfer_cancels",
            ],
        ),
        (
            "receiver",
            sqlx::migrate!("../../crates/fleet-sync-receiver/migrations"),
            &[
                "_fleet_sync_receipts",
                "_fleet_sync_staged_parts",
                "_fleet_sync_fence_receipts",
                "_fleet_sync_schema_steps",
                "_fleet_sync_seed_pages",
                "_fleet_sync_seed_attempts",
                "_fleet_sync_copy_tables",
                "_fleet_sync_hook_migrations",
                "_fleet_sync_install_pages",
            ],
        ),
        (
            "session",
            sqlx::migrate!("../../crates/fleet-sync-session/migrations"),
            &["_fleet_sync_sessions", "_fleet_sync_notices"],
        ),
        (
            "writer",
            sqlx::migrate!("../../crates/fleet-sync-writer/migrations"),
            &[
                "_fleet_sync_copies",
                "_fleet_sync_capture_bookkeeping",
                "_fleet_sync_release",
                "_fleet_sync_rejected_tails",
                "_fleet_sync_schema_steps",
            ],
        ),
        (
            "delivery",
            sqlx::migrate!("../../crates/fleet-sync-delivery/migrations"),
            &[
                "_fleet_sync_mail_items",
                "_fleet_sync_outbox_items",
                "_fleet_sync_inbox_items",
                "_fleet_sync_reports",
                "_fleet_sync_mail_cancel_links",
                "_fleet_sync_outbox_cancel_links",
                "_fleet_sync_inbox_cancel_links",
                "_fleet_sync_status_owed",
                "_fleet_sync_delivery_receipts",
                "_fleet_sync_mail_controls",
                "_fleet_sync_status_events",
            ],
        ),
        (
            "install",
            sqlx::migrate!("../../crates/fleet-sync-tokio-runner/control-migrations"),
            &["_fleet_sync_installs", "_fleet_sync_install_current"],
        ),
    ];
    let directory = tempfile::tempdir()?;
    let mut checked_keys = 0;
    let mut checked_refusals = 0;
    for (owner, migrations, tables) in schemas {
        // This test isolates NULL constraints; foreign-key validity is exercised
        // by each engine's own operations, independently of the synthetic rows.
        let mut connection = TursoConnectOptions::new()
            .filename(directory.path().join(format!("{owner}.db")))
            .create_if_missing(true)
            .foreign_keys(false)
            .connect()
            .await?;
        migrations.run_direct(None, &mut connection, false).await?;
        for table in *tables {
            let columns =
                sqlx::query::<Turso>(AssertSqlSafe(format!("PRAGMA table_info({table})")))
                    .fetch_all(&mut connection)
                    .await?;
            let keys = columns
                .iter()
                .filter_map(|row| match row.try_get::<i64, _>("pk") {
                    Ok(0) => None,
                    result => Some(result.map(|_| row)),
                })
                .collect::<Result<Vec<_>, _>>()?;
            assert!(!keys.is_empty(), "audited table {table} has no key");
            let names = columns
                .iter()
                .map(|row| row.try_get::<String, _>("name"))
                .collect::<Result<Vec<_>, _>>()?;
            let literals = columns
                .iter()
                .map(|row| {
                    let kind: String = row.try_get("type")?;
                    Ok::<_, sqlx_core::Error>(if kind.eq_ignore_ascii_case("BLOB") {
                        "X'00'"
                    } else if kind.eq_ignore_ascii_case("INTEGER") {
                        "0"
                    } else {
                        "'{}'"
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            for key in keys {
                let name: String = key.try_get("name")?;
                let kind: String = key.try_get("type")?;
                assert_eq!(
                    key.try_get::<i64, _>("notnull")?,
                    1,
                    "{owner}.{table}.{name} must declare NOT NULL"
                );
                checked_keys += 1;
                let rowid_alias = kind.eq_ignore_ascii_case("INTEGER")
                    && columns
                        .iter()
                        .filter(|row| row.try_get::<i64, _>("pk").is_ok_and(|key| key > 0))
                        .count()
                        == 1;
                if rowid_alias {
                    continue;
                }
                let offered = names
                    .iter()
                    .zip(&literals)
                    .map(
                        |(column, value)| {
                            if column == &name { "NULL" } else { *value }
                        },
                    )
                    .collect::<Vec<_>>()
                    .join(",");
                let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
                let result = sqlx::query::<Turso>(AssertSqlSafe(format!(
                    "INSERT INTO {table}({}) VALUES({offered})",
                    names.join(",")
                )))
                .execute(&mut *transaction)
                .await;
                let error = result
                    .err()
                    .ok_or_else(|| format!("NULL accepted at {table}.{name}"))?;
                assert!(
                    error.to_string().contains(&format!("{table}.{name}")),
                    "wrong refusal for {table}.{name}: {error}"
                );
                transaction.rollback().await?;
                let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
                sqlx::query::<Turso>(AssertSqlSafe(format!(
                    "INSERT INTO {table}({}) VALUES({})",
                    names.join(","),
                    literals.join(",")
                )))
                .execute(&mut *transaction)
                .await?;
                transaction.rollback().await?;
                checked_refusals += 1;
            }
        }
    }
    eprintln!(
        "audited {checked_keys} key declarations; {checked_refusals} NULL refusals with valid-insert controls"
    );
    Ok(())
}
