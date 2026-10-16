//! Diagnostic engine cost comparison; these disposable tables are not project admission proof.
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    query::query,
    query_scalar::query_scalar,
};
use sqlx_turso_core::{Turso, TursoConnectOptions};
use std::time::Instant;

type TestError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::test]
#[allow(clippy::panic_in_result_fn)] // Counts prove real writes/capture; timings report cost without imposing a machine-dependent test budget.
async fn native_insert_and_commit_report_capture_cost_for_small_and_large_payloads()
-> Result<(), TestError> {
    for bytes in [1024_usize, 65_536] {
        for capturing in [false, true] {
            let directory = tempfile::tempdir()?;
            let mut connection = TursoConnectOptions::new()
                .filename(directory.path().join("profile.db"))
                .create_if_missing(true)
                .connect()
                .await?;
            connection
                .execute(
                    "CREATE TABLE measured_rows(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
                )
                .await?;
            if capturing {
                connection
                    .execute("PRAGMA capture_data_changes_conn('full')")
                    .await?;
            }
            let payload = "x".repeat(bytes);
            let mut insert_times = Vec::new();
            let mut commit_times = Vec::new();
            for id in 0..100_i64 {
                let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
                let started = Instant::now();
                query::<Turso>("INSERT INTO measured_rows(id,payload) VALUES(?,?)")
                    .bind(id)
                    .bind(&payload)
                    .execute(&mut *transaction)
                    .await?;
                insert_times.push(started.elapsed().as_micros());
                let started = Instant::now();
                transaction.commit().await?;
                commit_times.push(started.elapsed().as_micros());
            }
            let count: i64 = query_scalar("SELECT count(*) FROM measured_rows")
                .fetch_one(&mut connection)
                .await?;
            assert_eq!(count, 100);
            if capturing {
                let captured: i64 =
                    query_scalar("SELECT count(*) FROM turso_cdc WHERE table_name='measured_rows'")
                        .fetch_one(&mut connection)
                        .await?;
                assert_eq!(captured, 100, "every insert is really captured");
            }
            insert_times.sort_unstable();
            commit_times.sort_unstable();
            eprintln!(
                "native payload={bytes} capture={capturing}: insert p50={}us p95={}us; commit p50={}us p95={}us",
                insert_times.get(49).ok_or("insert p50")?,
                insert_times.get(94).ok_or("insert p95")?,
                commit_times.get(49).ok_or("commit p50")?,
                commit_times.get(94).ok_or("commit p95")?
            );
            connection.close().await?;
        }
    }
    Ok(())
}
