#![allow(clippy::panic_in_result_fn)]

use sqlx_core::{executor::Executor, pool::PoolOptions, query::query, row::Row};
use sqlx_turso::{Turso, TursoConnectOptions};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Pinned-engine reproduction: deleting every CDC row discards its WAL rowid
/// floor, despite the table's AUTOINCREMENT declaration. A marker-only floor
/// preserves continuation without retaining acknowledged row images.
#[tokio::test]
async fn retaining_the_commit_marker_prevents_wal_capture_id_reuse_after_reopen() -> TestResult {
    for keep_marker in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("capture.db");
        let pool = PoolOptions::<Turso>::new()
            .max_connections(1)
            .connect_with(
                TursoConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await?;
        pool.execute("PRAGMA capture_data_changes_conn('full'); CREATE TABLE records(id TEXT NOT NULL PRIMARY KEY); INSERT INTO records VALUES('before')").await?;
        let previous =
            query::<Turso>("SELECT max(change_id) AS marker FROM turso_cdc WHERE change_type=2")
                .fetch_one(&pool)
                .await?
                .try_get::<i64, _>("marker")?;
        pool.execute("PRAGMA capture_data_changes_conn('off')")
            .await?;
        let prune = if keep_marker {
            "DELETE FROM turso_cdc WHERE change_id < ?"
        } else {
            "DELETE FROM turso_cdc WHERE change_id <= ?"
        };
        query::<Turso>(prune).bind(previous).execute(&pool).await?;
        pool.close().await;
        let pool = PoolOptions::<Turso>::new()
            .max_connections(1)
            .connect_with(TursoConnectOptions::new().filename(&path))
            .await?;
        pool.execute(
            "PRAGMA capture_data_changes_conn('full'); INSERT INTO records VALUES('after')",
        )
        .await?;
        let current =
            query::<Turso>("SELECT max(change_id) AS marker FROM turso_cdc WHERE change_type=2")
                .fetch_one(&pool)
                .await?
                .try_get::<i64, _>("marker")?;
        if keep_marker {
            assert!(
                current > previous,
                "retained marker must keep native capture positions monotonic"
            );
        } else {
            assert!(
                current <= previous,
                "upstream allocator changed; revisit the marker-retention workaround"
            );
        }
        pool.close().await;
    }
    Ok(())
}
