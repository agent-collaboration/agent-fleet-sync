//! Native pager timing and process-RSS driver; prepare fixtures in a separate process.
//! Run with `prepare-writer|prepare-copy|writer|copy PATH ROWS BODY_BYTES PAGE_LIMIT`.
//! A zero page limit reads the whole snapshot. Build candidates sequentially in target/debug.
mod fixture;

use fixture::{MeasurementResult, TursoScope};
use fleet_sync_protocol::{SnapshotPage, SnapshotRead, TableDigestBuilder};
use sqlx_core::connection::Connection;
use std::{
    path::Path,
    time::{Duration, Instant},
};

#[derive(Default)]
struct Measurements {
    elapsed: Duration,
    rows: usize,
    pages: usize,
    bytes: usize,
    digest: TableDigestBuilder,
}
impl Measurements {
    fn record(&mut self, page: &SnapshotPage, elapsed: Duration) -> MeasurementResult {
        self.elapsed += elapsed;
        self.rows += page.rows.len();
        self.pages += 1;
        self.bytes += serde_json::to_vec(page)?.len();
        for row in &page.rows {
            self.digest.push_row(row)?;
        }
        Ok(())
    }
    fn print(self) -> MeasurementResult {
        println!(
            "{}",
            serde_json::json!({
                "page_seconds":self.elapsed.as_secs_f64(), "rows":self.rows,
                "pages":self.pages, "encoded_bytes":self.bytes,
                "row_digest":self.digest.finish(),
            })
        );
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> MeasurementResult {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let [action, path, rows, body_bytes, page_limit] = arguments.as_slice() else {
        return Err("expected ACTION PATH ROWS BODY_BYTES PAGE_LIMIT".into());
    };
    let path = Path::new(path);
    let rows = rows.parse::<usize>()?;
    let body_bytes = body_bytes.parse::<usize>()?;
    let page_limit = page_limit.parse::<usize>()?;
    match action.as_str() {
        "prepare-writer" => fixture::prepare_writer(path, rows, body_bytes).await?,
        "prepare-copy" => fixture::prepare_copy(path, rows, body_bytes).await?,
        "writer" => {
            let database = fixture::writer(path).await?;
            // Admission and its 256-row key scan precede paging and are not timed.
            // Process RSS still includes them; compare the separate copy path too.
            let mut snapshot = database.snapshot().await?;
            let mut measurements = Measurements::default();
            let mut cursor = None;
            loop {
                let started = Instant::now();
                let page = snapshot.page(cursor).await?;
                let elapsed = started.elapsed();
                measurements.record(&page, elapsed)?;
                cursor = page.next;
                if cursor.is_none() || (page_limit > 0 && measurements.pages >= page_limit) {
                    break;
                }
            }
            measurements.print()?;
            snapshot.close().await?;
            database.close().await;
        }
        "copy" => {
            let mut connection = fixture::connection(path).await?;
            let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
            let mut measurements = Measurements::default();
            let mut cursor = None;
            loop {
                let started = Instant::now();
                let SnapshotRead::Page(page) =
                    fleet_sync_receiver::snapshot_page(&mut TursoScope(&mut transaction), cursor)
                        .await?
                else {
                    return Err("prepared copy moved".into());
                };
                let elapsed = started.elapsed();
                measurements.record(&page, elapsed)?;
                cursor = page.next;
                if cursor.is_none() || (page_limit > 0 && measurements.pages >= page_limit) {
                    break;
                }
            }
            measurements.print()?;
            transaction.rollback().await?;
            connection.close().await?;
        }
        _ => return Err("unknown measurement action".into()),
    }
    Ok(())
}
