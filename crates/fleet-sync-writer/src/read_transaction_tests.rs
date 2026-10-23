#![allow(clippy::panic_in_result_fn)]
use crate::*;
use fleet_sync_lease::LeasePolicy;
use fleet_sync_protocol::{PeerId, TableName};
use sqlx_core::{executor::Executor, migrate::Migrator, row::Row};
use std::time::Duration;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static LOCAL: Migrator = sqlx::migrate!("./tests/fixtures/sender_local");
pub(super) struct ReadSchema;
impl ReplicatedSchema for ReadSchema {
    fn migrations(&self) -> &'static [Migration] {
        &[]
    }
    fn local_migrations(&self) -> &'static [Migration] {
        LOCAL.iter().as_slice()
    }
    fn replicated_tables(&self) -> &'static [TableName] {
        &[]
    }
}
pub(super) async fn writer(path: &std::path::Path) -> TestResult<ReplicatedDatabase<ReadSchema>> {
    Ok(ReplicatedDatabase::create(
        path,
        ReadSchema,
        PeerId::new(durable_steps::Uuid::now_v7())?,
        LeasePolicy {
            lease_duration: Duration::from_secs(60),
            renew_interval: Duration::from_secs(10),
        },
        WriterOptions {
            allow_provisional_writes: true,
        },
    )
    .await?)
}

#[tokio::test]
async fn related_reads_keep_one_snapshot_while_the_writer_commits() -> TestResult {
    // Arrange a real managed writer, with all reads on its guarded reader pool.
    let directory = tempfile::tempdir()?;
    let database = writer(&directory.path().join("writer.db")).await?;
    let mut write = database.begin_write().await?;
    // A write guard already holds the writer control lock. Reader admission
    // must remain independent of it, rather than waiting for write completion.
    let mut read =
        tokio::time::timeout(Duration::from_secs(1), database.reader().begin_read()).await??;
    let first = (&mut read)
        .fetch_one("SELECT COUNT(*) AS count FROM local_marks")
        .await?
        .try_get::<i64, _>("count")?;

    // Act: an independent real writer commits between the related reads.
    (&mut write)
        .execute("INSERT INTO local_marks VALUES('after first read')")
        .await?;
    write.commit().await?;
    let second = (&mut read)
        .fetch_one("SELECT COUNT(*) AS count FROM local_marks")
        .await?
        .try_get::<i64, _>("count")?;
    let current = database
        .reader()
        .fetch_one("SELECT COUNT(*) AS count FROM local_marks")
        .await?
        .try_get::<i64, _>("count")?;
    read.close().await?;
    database.close().await;

    // Assert the snapshot invariant, with a fresh-reader positive control.
    assert_eq!(first, 0);
    assert_eq!(current, 1);
    assert_eq!(
        second, first,
        "related reads crossed a committed writer change"
    );
    Ok(())
}
