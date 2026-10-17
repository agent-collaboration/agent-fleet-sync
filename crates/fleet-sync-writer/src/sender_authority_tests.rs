#![allow(clippy::panic_in_result_fn)]
use crate::{
    ReplicationEvents, WriteRefusal,
    sender_test_fixture::{SenderFixture, TestResult},
    writer_scope::WriterScope,
};
use durable_steps::{CallId, OperationId, StepName, Uuid};
use fleet_sync_lease::WriterState;
use fleet_sync_protocol::{
    CopyStatusAnswer, Epoch, FenceExpectation, FenceRequest, FenceResult, LeaseState, LeaseStatus,
    PeerId, ReleaseAnswer, ReleaseRequest, RenewAnswer, RenewResult, TransferId,
};
use sqlx_core::{connection::Connection, executor::Executor, query::query, row::Row};
use sqlx_turso::Turso;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn bounded_retention_releases_a_follower_but_keeps_every_unacknowledged_authoritative_change()
-> TestResult {
    use crate::{ReplicationSender, Retention, SenderOptions};
    use fleet_sync_protocol::{CopyRole, ReceiverId};
    use std::{sync::Arc, time::Duration};
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    let start = fixture
        .sender
        .copies()
        .await?
        .into_iter()
        .find(|copy| copy.copy == fixture.first)
        .ok_or("no authoritative checkpoint")?
        .acked;
    let absent = ReceiverId::new(Uuid::from_u128(44))?;
    fixture
        .sender
        .add_copy(absent, CopyRole::Follower, start)
        .await?;
    let mut write = fixture.database.begin_write().await?;
    (&mut write)
        .execute(
            "INSERT INTO text_records VALUES('019eef00-0000-7000-8000-000000000008',99,'retained until ack')",
        )
        .await?;
    write.commit().await?;
    fixture.sender.replicate_once(fixture.second).await?;
    let bounded = ReplicationSender::new(
        fixture.database.clone(),
        Arc::new(()),
        fixture.marks.clone(),
        fixture.transport.clone(),
        SenderOptions {
            retention: Retention::Bounded {
                max_age: Duration::MAX,
                max_changes: 0,
            },
            ..SenderOptions::default()
        },
    );
    let events = bounded.replicate_once(absent).await?;
    assert!(
        events.iter().any(
            |event| matches!(event,ReplicationEvents::CopyNeedsSnapshot {copy} if *copy==absent)
        )
    );
    let copies = bounded.copies().await?;
    assert!(
        !copies
            .iter()
            .find(|copy| copy.copy == fixture.first)
            .ok_or("missing authority")?
            .needs_snapshot
    );
    assert!(
        copies
            .iter()
            .find(|copy| copy.copy == absent)
            .ok_or("missing follower")?
            .needs_snapshot
    );
    assert!(
        query::<Turso>("SELECT change_id FROM turso_cdc WHERE table_name='text_records'")
            .fetch_optional(fixture.database.reader())
            .await?
            .is_some(),
        "an absent authority must keep capture even when every follower is current or released"
    );
    bounded.replicate_once(fixture.first).await?;
    // The next maintenance pass sees the acknowledgement that just committed.
    bounded.replicate_once(fixture.first).await?;
    {
        let mut connection = fixture
            .transport
            .files
            .get(&fixture.first)
            .ok_or("missing authority file")?
            .connection
            .lock()
            .await;
        let connection = connection.as_mut().ok_or("closed copy")?;
        assert_eq!(
            query::<Turso>(
                "SELECT amount FROM text_records WHERE id='019eef00-0000-7000-8000-000000000008'"
            )
            .fetch_one(connection)
            .await?
            .try_get::<i64, _>("amount")?,
            99
        );
    }
    assert!(
        query::<Turso>("SELECT change_id FROM turso_cdc WHERE table_name='text_records'")
            .fetch_optional(fixture.database.reader())
            .await?
            .is_none(),
        "the authority's real acknowledgement releases those row images"
    );
    fixture.close().await
}

#[tokio::test]
async fn release_stops_writes_but_sender_drains_every_accepted_commit_to_its_final_position()
-> TestResult {
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    let mut write = fixture.database.begin_write().await?;
    (&mut write)
        .execute("INSERT INTO text_records VALUES('019eef00-0000-7000-8000-000000000009',42,'must arrive')")
        .await?;
    write.commit().await?;
    let answer = fixture
        .database
        .request_release(
            &ReleaseRequest {
                database: fixture.database.database(),
                epoch: Epoch::new(1)?,
                transfer: TransferId::new(Uuid::from_u128(40))?,
            },
            CallId {
                operation: OperationId(Uuid::from_u128(41)),
                step: StepName::new("release")?,
            },
        )
        .await?;
    let ReleaseAnswer::Released { final_position } = answer else {
        return Err("release refused".into());
    };
    assert!(matches!(
        fixture.database.begin_write().await,
        Err(WriteRefusal::Released)
    ));
    for copy in [fixture.first, fixture.second] {
        fixture.sender.replicate_once(copy).await?;
        let mut connection = fixture
            .transport
            .files
            .get(&copy)
            .ok_or("missing copy")?
            .connection
            .lock()
            .await;
        let connection = connection.as_mut().ok_or("closed copy")?;
        assert_eq!(
            query::<Turso>(
                "SELECT amount FROM text_records WHERE id='019eef00-0000-7000-8000-000000000009'"
            )
            .fetch_one(&mut *connection)
            .await?
            .try_get::<i64, _>("amount")?,
            42
        );
        let CopyStatusAnswer::Status(status) =
            fleet_sync_receiver::copy_status(&mut WriterScope(connection)).await?
        else {
            return Err("no copy status".into());
        };
        assert_eq!(status.acked, final_position);
    }
    assert!(matches!(
        fixture.database.writer_state().await?,
        WriterState::Released { .. }
    ));
    fixture.close().await
}

#[tokio::test]
async fn replayed_renewal_can_close_gate_before_authoritative_probe_records_the_rejected_tail()
-> TestResult {
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    let ended_at = {
        let mut connection = fixture
            .transport
            .files
            .get(&fixture.first)
            .ok_or("missing copy")?
            .connection
            .lock()
            .await;
        let connection = connection.as_mut().ok_or("closed copy")?;
        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        let CopyStatusAnswer::Status(status) =
            fleet_sync_receiver::copy_status(&mut WriterScope(&mut transaction)).await?
        else {
            return Err("no copy status".into());
        };
        let result = fleet_sync_receiver::fence(
            &mut WriterScope(&mut transaction),
            &FenceRequest {
                database: fixture.database.database(),
                transfer_id: TransferId::new(Uuid::from_u128(42))?,
                new_epoch: Epoch::new(2)?,
                expect: FenceExpectation {
                    epoch: Epoch::new(1)?,
                    acked: status.acked,
                },
                target_schema_max: 2,
                holder: PeerId::new(Uuid::from_u128(43))?,
            },
        )
        .await?;
        assert!(matches!(result, FenceResult::Fenced { .. }));
        transaction.commit().await?;
        status.acked
    };
    let mut write = fixture.database.begin_write().await?;
    (&mut write)
        .execute("UPDATE text_records SET label='accepted locally after remote fence'")
        .await?;
    write.commit().await?;
    let now = fixture.database.clock.now();
    fixture
        .database
        .observe_renewal(
            RenewAnswer::Renewed {
                recorded: RenewResult {
                    epoch: Epoch::new(1)?,
                    expires_at: now,
                },
                current: LeaseStatus {
                    database: fixture.database.database(),
                    epoch: Epoch::new(2)?,
                    holder: PeerId::new(Uuid::from_u128(43))?,
                    state: LeaseState::Held,
                    expires_at: now,
                    remaining_ms: 10_000,
                },
                transfer_in_progress: None,
                replayed: true,
            },
            now,
        )
        .await?;
    assert!(matches!(
        fixture.database.begin_write().await,
        Err(WriteRefusal::Superseded { .. })
    ));
    let before = fixture.transport.calls.load(Ordering::SeqCst);
    assert!(
        fixture
            .sender
            .replicate_once(fixture.second)
            .await?
            .is_empty()
    );
    assert_eq!(
        fixture.transport.calls.load(Ordering::SeqCst),
        before,
        "superseded capture must not go to a follower"
    );
    let events = fixture.sender.replicate_once(fixture.first).await?;
    assert!(events.iter().any(|event|matches!(event,ReplicationEvents::RejectedTail(tail) if tail.ended_at==ended_at && tail.last_commit>ended_at)));
    let calls = fixture.transport.calls.load(Ordering::SeqCst);
    assert_eq!(
        calls,
        before + 1,
        "one authoritative probe identifies the end"
    );
    assert!(
        fixture
            .sender
            .replicate_once(fixture.first)
            .await?
            .is_empty()
    );
    assert_eq!(fixture.transport.calls.load(Ordering::SeqCst), calls);
    fixture
        .sender
        .acknowledge_rejected_tail(Epoch::new(1)?)
        .await?;
    fixture
        .database
        .observe_renewal(
            RenewAnswer::Refused(fleet_sync_protocol::LeaseRefusal::StaleEpoch {
                current_epoch: Epoch::new(2)?,
                epoch_ended_at: ended_at,
            }),
            fixture.database.clock.now(),
        )
        .await?;
    assert!(
        fixture.sender.rejected_tails().await?.is_empty(),
        "a repeated renewal must not resurrect a consumed tail"
    );
    fixture.sender.replicate_once(fixture.first).await?;
    assert_eq!(
        fixture.transport.calls.load(Ordering::SeqCst),
        calls,
        "a resolved epoch must not be probed again after its tail was consumed"
    );
    assert!(
        query::<Turso>("SELECT change_id FROM turso_cdc WHERE table_name='text_records'")
            .fetch_optional(fixture.database.reader())
            .await?
            .is_some()
    );
    fixture.close().await
}
