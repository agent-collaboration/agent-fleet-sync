#![allow(clippy::panic_in_result_fn)]
use crate::*;
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::{PeerId, TableName};
use futures_util::StreamExt;
use sqlx_core::{executor::Executor, query::query, row::Row};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct EmptySchema;
impl ReplicatedSchema for EmptySchema {
    fn migrations(&self) -> &'static [Migration] {
        &[]
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        &[]
    }
}
async fn writer(path: &std::path::Path) -> TestResult<ReplicatedDatabase<EmptySchema>> {
    Ok(ReplicatedDatabase::create(
        path,
        EmptySchema,
        PeerId::new(durable_steps::Uuid::from_u128(2))?,
        LeasePolicy {
            lease_duration: Duration::from_secs(60),
            renew_interval: Duration::from_secs(10),
        },
        WriterOptions::default(),
    )
    .await?)
}
#[tokio::test]
async fn held_read_stream_refuses_closed_path_callback_until_its_guard_is_dropped() -> TestResult {
    let root = tempfile::tempdir()?;
    let writer = writer(&root.path().join("writer.db")).await?;
    writer.begin_seeding().await?;
    let mut reading = writer
        .reader()
        .fetch("SELECT 1 AS value UNION ALL SELECT 2 AS value");
    assert_eq!(
        reading
            .next()
            .await
            .ok_or("no first row")??
            .try_get::<i64, _>("value")?,
        1
    );
    let called = std::sync::Arc::new(AtomicBool::new(false));
    let observed = called.clone();
    let refused = writer
        .with_drained_file(move |_| {
            Box::pin(async move {
                observed.store(true, Ordering::SeqCst);
                Ok(())
            })
        })
        .await;
    assert!(matches!(
        refused,
        Err(native_file::PublishError::ActiveReaders)
    ));
    assert!(!called.load(Ordering::SeqCst));
    drop(reading);
    let pinned = writer.snapshot().await?;
    assert!(matches!(
        writer
            .with_drained_file(|_| Box::pin(async { Ok(()) }))
            .await,
        Err(native_file::PublishError::ActiveReaders)
    ));
    pinned.close().await?;
    writer
        .with_drained_file(|path| {
            Box::pin(async move {
                assert!(
                    tokio::fs::metadata(path)
                        .await
                        .map_err(native_file::PublishError::io)?
                        .len()
                        > 0
                );
                Ok(())
            })
        })
        .await?;
    assert_eq!(
        query::<sqlx_turso::Turso>("SELECT 3 AS value")
            .fetch_one(writer.reader())
            .await?
            .try_get::<i64, _>("value")?,
        3
    );
    writer.close().await;
    Ok(())
}
#[tokio::test]
async fn failed_closed_path_callback_reopens_old_file_and_preserves_seeding_gate() -> TestResult {
    let root = tempfile::tempdir()?;
    let writer = writer(&root.path().join("writer.db")).await?;
    assert!(matches!(
        writer
            .with_drained_file(|_| Box::pin(async { Ok(()) }))
            .await,
        Err(native_file::PublishError::NotSeeding)
    ));
    let position = writer.begin_seeding().await?;
    let failed: Result<(), _> = writer
        .with_drained_file(|_| {
            Box::pin(async {
                Err(native_file::PublishError::io(
                    "intentional callback failure",
                ))
            })
        })
        .await;
    assert!(matches!(failed, Err(native_file::PublishError::Io { .. })));
    assert_eq!(writer.snapshot().await?.position(), position);
    assert_eq!(
        writer.writer_state().await?,
        fleet_sync_lease::WriterState::Seeding {
            epoch: position.epoch
        }
    );
    assert!(matches!(
        writer.begin_write().await,
        Err(WriteRefusal::Seeding)
    ));
    writer
        .with_drained_file(|_| Box::pin(async { Ok(()) }))
        .await?;
    writer.close().await;
    Ok(())
}
