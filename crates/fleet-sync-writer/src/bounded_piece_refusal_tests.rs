#![allow(clippy::panic_in_result_fn)]
use super::fixture::last_capture;
use crate::{sender_test_fixture::*, *};
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::{unit_budget::UnitTooLarge, *};
use sqlx_core::{
    migrate::{MigrationType, Migrator},
    query::query,
    sql_str::{AssertSqlSafe, SqlSafeStr},
};
use sqlx_turso::Turso;
use std::{num::NonZeroUsize, sync::LazyLock, time::Duration};

#[tokio::test]
async fn oversize_row_refusal_context_keeps_the_actual_table_and_key() -> TestResult {
    let source = SenderFixture::open().await?;
    let mut write = source.database.begin_write().await?;
    query::<Turso>(
        "UPDATE text_records SET label=? WHERE id='019eef00-0000-7000-8000-000000000001'",
    )
    .bind("r".repeat(2 * 1024 * 1024))
    .execute(&mut write)
    .await?;
    write.commit().await?;
    let (after, captured) = last_capture(source.database.as_ref()).await?;
    let result = crate::piece_builder::cut_transaction(
        source.database.database(),
        captured.position.epoch,
        after,
        &captured,
        NonZeroUsize::MIN.saturating_add(999),
        &(),
    );
    if let Ok(parts) = &result {
        eprintln!(
            "oversize row produced {} units and {} encoded bytes",
            parts.len(),
            serde_json::to_vec(parts)?.len()
        );
    }
    assert!(
        matches!(result, Err(SenderError::UnitTooLarge(UnitTooLarge::Row { table, key, encoded_size }))
        if table == "text_records" && key == ["019eef00-0000-7000-8000-000000000001"] && encoded_size > 1536 * 1024),
        "oversize row was emitted or lost its actual key context"
    );
    source.close().await
}

static REGULAR: Migrator = sqlx::migrate!("./tests/fixtures/regular");
static TABLES: LazyLock<Result<Vec<TableName>, TextValueError>> = LazyLock::new(|| {
    ["text_records", "composite_records", "real_records"]
        .into_iter()
        .map(TableName::new)
        .collect()
});
static LARGE_HISTORY: LazyLock<Vec<Migration>> = LazyLock::new(|| {
    let mut migrations = REGULAR.iter().cloned().collect::<Vec<_>>();
    migrations.push(Migration::new(3, "oversize encoded historical statement".into(), MigrationType::Simple,
        AssertSqlSafe(format!("CREATE TABLE discarded_payload(id TEXT NOT NULL PRIMARY KEY,body TEXT DEFAULT '{}'); DROP TABLE discarded_payload", "\"".repeat(900 * 1024))).into_sql_str(), false));
    migrations
});
struct HistorySchema {
    tables: &'static [TableName],
}
impl ReplicatedSchema for HistorySchema {
    fn migrations(&self) -> &'static [Migration] {
        &LARGE_HISTORY
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        self.tables
    }
}

#[tokio::test]
async fn oversize_schema_statement_refusal_context_keeps_commit_version_and_index() -> TestResult {
    let root = tempfile::tempdir()?;
    let source = ReplicatedDatabase::create(
        &root.path().join("history.db"),
        HistorySchema {
            tables: TABLES.as_ref().map_err(Clone::clone)?,
        },
        PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000002,
        ))?,
        LeasePolicy {
            lease_duration: Duration::from_secs(600),
            renew_interval: Duration::from_secs(60),
        },
        WriterOptions::default(),
    )
    .await?;
    let mut snapshot = source.snapshot().await?;
    assert!(
        serde_json::to_vec(&snapshot.page(None).await?.schema_steps)?.len() < 4096,
        "large retained history must not become a current-catalog quota"
    );
    snapshot.close().await?;
    let (after, captured) = last_capture(&source).await?;
    let ReplicatedTransaction::SchemaStep {
        migration_version,
        statements,
        ..
    } = &captured.transaction
    else {
        return Err("missing real admitted schema-step capture".into());
    };
    assert_eq!(*migration_version, 3);
    assert!(
        statements
            .first()
            .ok_or("missing historical statement")?
            .len()
            < 1024 * 1024,
        "the encoded budget must count JSON escaping"
    );
    let result = crate::piece_builder::cut_transaction(
        source.database(),
        captured.position.epoch,
        after,
        &captured,
        NonZeroUsize::MIN.saturating_add(999),
        &(),
    );
    if let Ok(parts) = &result {
        eprintln!(
            "oversize schema statement produced {} units and {} encoded bytes",
            parts.len(),
            serde_json::to_vec(parts)?.len()
        );
    }
    assert!(
        matches!(result, Err(SenderError::UnitTooLarge(UnitTooLarge::SchemaStatement {
        commit_id, migration_version: 3, statement_index: 0, encoded_size,
    })) if commit_id == captured.position.sequence && encoded_size > 1536 * 1024),
        "oversize schema statement was emitted or lost its real capture context"
    );
    source.close().await;
    Ok(())
}
