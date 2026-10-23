#![allow(clippy::panic_in_result_fn)]
use crate::{
    ReplicationEvents, SenderError, WriteRefusal,
    sender_test_fixture::{SenderFixture, TestResult},
    writer_file_record::read_file_record,
    writer_scope::WriterScope,
};
use durable_steps::Uuid;
use fleet_sync_lease::WriterState;
use fleet_sync_protocol::*;
use sqlx_core::{connection::Connection, query::query, row::Row};
use sqlx_turso::Turso;
use std::time::Duration;

async fn persist_live_lease(fixture: &SenderFixture) -> TestResult {
    let now = fixture.database.clock.now();
    let expires_at = now.after(Duration::from_secs(600));
    fixture
        .database
        .observe_renewal(
            RenewAnswer::Renewed {
                recorded: RenewResult {
                    epoch: Epoch::new(1)?,
                    expires_at,
                },
                current: LeaseStatus {
                    database: fixture.database.database(),
                    epoch: Epoch::new(1)?,
                    holder: PeerId::new(Uuid::from_u128(0x019eef00000070008000000000000002))?,
                    state: LeaseState::Held,
                    expires_at,
                    remaining_ms: 600_000,
                },
                transfer_in_progress: None,
                replayed: false,
            },
            now,
        )
        .await?;
    Ok(())
}
async fn fence_copy(fixture: &SenderFixture, copy: ReceiverId) -> TestResult<ReplicationPosition> {
    let mut owner = fixture
        .transport
        .files
        .get(&copy)
        .ok_or("copy missing")?
        .connection
        .lock()
        .await;
    let connection = owner.as_mut().ok_or("copy closed")?;
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let CopyStatusAnswer::Status(status) =
        fleet_sync_receiver::copy_status(&mut WriterScope(&mut transaction)).await?
    else {
        return Err("copy status missing".into());
    };
    let answer = fleet_sync_receiver::fence(
        &mut WriterScope(&mut transaction),
        &FenceRequest {
            database: fixture.database.database(),
            transfer_id: TransferId::new(Uuid::now_v7())?,
            new_epoch: Epoch::new(2)?,
            expect: FenceExpectation {
                epoch: Epoch::new(1)?,
                acked: status.acked,
            },
            target_schema: Some(fleet_sync_protocol::SchemaSupport { max_version: 2 }),
            holder: PeerId::new(Uuid::from_u128(0x019eef00000070008000000000000008))?,
        },
    )
    .await?;
    assert!(matches!(answer, FenceResult::Fenced { .. }));
    transaction.commit().await?;
    Ok(status.acked)
}
async fn write_amount(fixture: &SenderFixture, amount: i64) -> TestResult {
    let mut write = fixture.database.begin_write().await?;
    query::<Turso>("UPDATE text_records SET amount=?")
        .bind(amount)
        .execute(&mut write)
        .await?;
    write.commit().await?;
    Ok(())
}
async fn authoritative_amount(fixture: &SenderFixture) -> TestResult<i64> {
    let mut owner = fixture
        .transport
        .files
        .get(&fixture.first)
        .ok_or("authority missing")?
        .connection
        .lock()
        .await;
    let connection = owner.as_mut().ok_or("authority closed")?;
    Ok(query::<Turso>("SELECT amount FROM text_records")
        .fetch_one(connection)
        .await?
        .try_get("amount")?)
}
async fn assert_unended_writer(fixture: &SenderFixture) -> TestResult {
    assert!(
        matches!(fixture.database.writer_state().await?, WriterState::Writing { epoch } if epoch==Epoch::new(1)?)
    );
    assert!(fixture.sender.rejected_tails().await?.is_empty());
    let mut connection = fixture.database.maintenance.acquire().await?;
    let record = read_file_record(&mut connection)
        .await?
        .ok_or("writer record missing")?;
    assert!(record.epoch_ended_at.is_none());
    Ok(())
}

#[tokio::test]
async fn follower_stale_epoch_cannot_end_a_live_writer_before_or_after_reopen() -> TestResult {
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    persist_live_lease(&fixture).await?;
    assert!(
        fixture
            .sender
            .copies()
            .await?
            .iter()
            .any(|copy| copy.copy == fixture.second && copy.role == CopyRole::Follower)
    );
    fence_copy(&fixture, fixture.second).await?;
    write_amount(&fixture, 99).await?;
    let answer = fixture.sender.replicate_once(fixture.second).await;
    assert_unended_writer(&fixture).await?;
    assert!(
        matches!(answer, Err(SenderError::Refused { copy, reason: PieceRefusal::StaleEpoch { .. } }) if copy==fixture.second)
    );
    write_amount(&fixture, 100).await?;
    fixture.sender.replicate_once(fixture.first).await?;
    fixture.sender.replicate_once(fixture.first).await?;
    assert_eq!(authoritative_amount(&fixture).await?, 100);
    let fixture = fixture.restart().await?;
    assert_unended_writer(&fixture).await?;
    write_amount(&fixture, 101).await?;
    fixture.sender.replicate_once(fixture.first).await?;
    assert_eq!(authoritative_amount(&fixture).await?, 101);
    assert_unended_writer(&fixture).await?;
    fixture.close().await
}

#[tokio::test]
async fn authoritative_stale_epoch_closes_admission_and_keeps_the_tail_after_reopen() -> TestResult
{
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    persist_live_lease(&fixture).await?;
    let ended_at = fence_copy(&fixture, fixture.first).await?;
    write_amount(&fixture, 99).await?;
    let events = fixture.sender.replicate_once(fixture.first).await?;
    assert!(events.iter().any(
        |event| matches!(event, ReplicationEvents::RejectedTail(tail) if tail.ended_at==ended_at)
    ));
    assert!(
        matches!(fixture.database.begin_write().await, Err(WriteRefusal::Superseded { current }) if current==Epoch::new(2)?)
    );
    let tails = fixture.sender.rejected_tails().await?;
    assert_eq!(tails.len(), 1);
    let fixture = fixture.restart().await?;
    assert!(
        matches!(fixture.database.writer_state().await?, WriterState::Superseded { current } if current==Epoch::new(2)?)
    );
    assert!(matches!(
        fixture.database.begin_write().await,
        Err(WriteRefusal::Superseded { .. })
    ));
    assert_eq!(fixture.sender.rejected_tails().await?, tails);
    let mut connection = fixture.database.maintenance.acquire().await?;
    assert_eq!(
        read_file_record(&mut connection)
            .await?
            .ok_or("writer record missing")?
            .epoch_ended_at,
        Some(ended_at)
    );
    drop(connection);
    fixture.close().await
}
