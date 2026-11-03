//! Observe a real uncommitted mapping before exposing the causal crash point.
use super::*;
use durable_steps::{storage::TransactionScope, turso_store::TursoScope};
use sqlx_core::{query::query, row::Row};
use sqlx_turso::{Turso, TursoConnection};
use std::io::Write;

pub const CHECKPOINT: &str = "SUBMITTER_ROWS_MAPPED_NOT_COMMITTED";
pub struct BackfillCheckpointScope<'view, 'transaction> {
    pub inner: &'view mut TursoScope<'transaction, TestHost>,
}
impl TransactionScope for BackfillCheckpointScope<'_, '_> {
    type Database = Turso;
    type Executor<'query>
        = &'query mut TursoConnection
    where
        Self: 'query;
    fn executor(&mut self) -> Self::Executor<'_> {
        let connection = self.inner.executor();
        let mapped = std::thread::scope(|threads| {
            threads.spawn(move || {
                tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async move {
                    let row=query::<Turso>("SELECT (SELECT count(*) FROM _fleet_sync_outbox_items WHERE submitter_kind IS NOT NULL)+(SELECT count(*) FROM _fleet_sync_inbox_items WHERE submitter_kind IS NOT NULL) AS count")
                        .fetch_one(connection).await.unwrap();
                    row.try_get::<i64,_>("count").unwrap()
                })
            }).join().unwrap()
        });
        if mapped > 0 {
            println!("\n{CHECKPOINT}");
            std::io::stdout().flush().unwrap();
            loop {
                std::thread::park();
            }
        }
        self.inner.executor()
    }
}
