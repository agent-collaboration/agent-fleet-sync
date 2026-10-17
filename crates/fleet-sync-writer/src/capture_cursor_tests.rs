#![allow(clippy::panic_in_result_fn)]
use crate::{
    BootstrapGrant, OpenError, ReplicatedDatabase, SenderError,
    sender_test_fixture::{SenderFixture, TestResult},
    writer_file_record::{read_file_record, write_file_record},
};
use durable_steps::Uuid;
use fleet_sync_lease::{LeaseHolder, LeasePolicy};
use fleet_sync_protocol::*;
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;
use std::{sync::atomic::Ordering, time::Duration};

async fn copy_records(fixture: &SenderFixture) -> TestResult<Vec<String>> {
    let mut connection = fixture.database.maintenance.acquire().await?;
    query::<Turso>("SELECT record FROM _fleet_sync_copies ORDER BY copy_id")
        .fetch_all(&mut *connection)
        .await?
        .into_iter()
        .map(|row| row.try_get("record").map_err(Into::into))
        .collect()
}

#[tokio::test]
async fn incoherent_and_out_of_range_capture_cursors_refuse_before_idle_prune_or_send() -> TestResult
{
    for (cursor, floor) in [(1, 0), (u64::MAX, 0), (u64::MAX, u64::MAX)] {
        let fixture = SenderFixture::open().await?;
        let before = copy_records(&fixture).await?;
        let calls = fixture.transport.calls.load(Ordering::SeqCst);
        {
            let mut connection = fixture.database.maintenance.acquire().await?;
            let mut record = read_file_record(&mut connection)
                .await?
                .ok_or("writer missing")?;
            assert_eq!(record.retained_after.sequence.value(), 0);
            record.retained_change_id = cursor;
            record.retained_after.sequence = CommitSequence::new(floor);
            write_file_record(&mut connection, &record).await?;
            assert!(
                matches!(
                    read_file_record(&mut connection).await,
                    Err(OpenError::Corrupt { field }) if field == "writer.capture_cursor"
                ),
                "cursor {cursor} at floor {floor} decoded"
            );
        }
        assert!(
            matches!(
                fixture.sender.replicate_once(fixture.first).await,
                Err(SenderError::Storage(OpenError::Corrupt { field })) if field == "writer.capture_cursor"
            ),
            "invalid cursor returned idle success"
        );
        assert_eq!(fixture.transport.calls.load(Ordering::SeqCst), calls);
        assert_eq!(copy_records(&fixture).await?, before);
        let path = fixture.database.path().to_owned();
        let database = fixture.database.database();
        let schema = fixture.database.schema.clone();
        fixture.database.close().await;
        assert!(matches!(
            ReplicatedDatabase::open(
                &path,
                database,
                schema,
                LeaseHolder::provisional(database, PeerId::new(Uuid::from_u128(2))?, policy())
            )
            .await,
            Err(OpenError::Corrupt { field }) if field == "writer.capture_cursor"
        ));
        fixture.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn ordinary_pruning_keeps_a_coherent_capture_cursor_through_reopen() -> TestResult {
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    fixture.sender.replicate_once(fixture.first).await?;
    {
        let mut connection = fixture.database.maintenance.acquire().await?;
        let record = read_file_record(&mut connection)
            .await?
            .ok_or("writer missing")?;
        assert_eq!(record.retained_after.epoch, record.epoch);
        assert!(record.retained_change_id > 0);
        assert_eq!(
            record.retained_change_id,
            record.retained_after.sequence.value()
        );
    }
    let fixture = fixture.restart().await?;
    let mut connection = fixture.database.maintenance.acquire().await?;
    assert!(read_file_record(&mut connection).await?.is_some());
    drop(connection);
    fixture.close().await
}

fn policy() -> LeasePolicy {
    LeasePolicy {
        lease_duration: Duration::from_secs(600),
        renew_interval: Duration::from_secs(10),
    }
}
#[tokio::test]
async fn bootstrap_predecessor_position_keeps_a_zero_capture_cursor_on_reopen() -> TestResult {
    let source = SenderFixture::open().await?;
    let root = tempfile::tempdir()?;
    let path = root.path().join("seeding-writer.db");
    let database = DatabaseId::new(Uuid::now_v7())?;
    let peer = PeerId::new(Uuid::from_u128(8))?;
    let start = ReplicationPosition::new(Epoch::new(1)?, CommitSequence::new(4242));
    let now = source.database.clock.now();
    let writer = ReplicatedDatabase::seeding(
        &path,
        source.database.schema.clone(),
        BootstrapGrant {
            lease: LeaseStatus {
                database,
                holder: peer,
                epoch: Epoch::new(2)?,
                state: LeaseState::Held,
                expires_at: now.after(Duration::from_secs(600)),
                remaining_ms: 600_000,
            },
            request_started: now,
            start,
        },
        policy(),
    )
    .await?;
    {
        let mut connection = writer.maintenance.acquire().await?;
        let record = read_file_record(&mut connection)
            .await?
            .ok_or("bootstrap missing")?;
        assert_eq!(record.retained_after, start);
        assert_eq!(record.retained_change_id, 0);
        assert!(record.retained_after.epoch < record.epoch);
    }
    writer.close().await;
    let writer = ReplicatedDatabase::open(
        &path,
        database,
        source.database.schema.clone(),
        LeaseHolder::seeding(database, peer, Epoch::new(2)?, policy()),
    )
    .await?;
    let mut connection = writer.maintenance.acquire().await?;
    let record = read_file_record(&mut connection)
        .await?
        .ok_or("bootstrap missing after reopen")?;
    assert_eq!(record.retained_after, start);
    assert_eq!(record.retained_change_id, 0);
    drop(connection);
    writer.close().await;
    source.close().await
}
