#![allow(clippy::panic_in_result_fn)]
use crate::{
    ReplicationEvents, SenderError,
    sender_test_fixture::{SenderFixture, TestResult},
};
use fleet_sync_protocol::ReceiverId;
use sqlx_core::{connection::Connection, executor::Executor, query::query, row::Row};
use sqlx_turso::Turso;
use std::{collections::BTreeMap, sync::atomic::Ordering, time::Duration};
use tokio::sync::{mpsc, watch};

async fn wait_for_pass(
    passes: &mut watch::Receiver<BTreeMap<ReceiverId, usize>>,
    copy: ReceiverId,
    target: usize,
) -> TestResult {
    tokio::time::timeout(
        Duration::from_secs(31),
        passes.wait_for(|counts| counts.get(&copy).copied().unwrap_or(0) >= target),
    )
    .await??;
    Ok(())
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

#[tokio::test]
async fn idle_copy_checks_only_at_the_safety_net_over_sixty_virtual_seconds() -> TestResult {
    // A 100 ms polling loop fails at the very next pass, even with paused time.
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    tokio::time::pause();
    let (shutdown, stopped) = watch::channel(false);
    let (events, _received) = mpsc::channel(16);
    let mut passes = fixture.sender.copy_passes.subscribe();
    let sender = fixture.sender.clone();
    let copy = fixture.first;
    let worker = tokio::spawn(async move { sender.run_copy(copy, stopped, events).await });
    wait_for_pass(&mut passes, fixture.first, 1).await?;
    let started = tokio::time::Instant::now();
    for expected in 2..=3 {
        wait_for_pass(&mut passes, fixture.first, expected).await?;
        assert_eq!(
            started.elapsed(),
            Duration::from_secs(30 * (expected as u64 - 1))
        );
    }
    assert!(passes.borrow().get(&copy).copied().unwrap_or(0) <= 60 / 30 + 2);
    shutdown.send_replace(true);
    worker.await?;
    fixture.close().await
}

#[tokio::test]
async fn committed_capture_wakes_an_idle_copy_before_the_safety_net() -> TestResult {
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    tokio::time::pause();
    let (shutdown, stopped) = watch::channel(false);
    let (events, mut received) = mpsc::channel(16);
    let mut passes = fixture.sender.copy_passes.subscribe();
    let sender = fixture.sender.clone();
    let copy = fixture.first;
    let worker = tokio::spawn(async move { sender.run_copy(copy, stopped, events).await });
    wait_for_pass(&mut passes, fixture.first, 1).await?;
    let started = tokio::time::Instant::now();

    write_amount(&fixture, 451).await?;
    let event = tokio::time::timeout(Duration::from_millis(50), received.recv())
        .await?
        .ok_or("runner closed")??;
    assert!(
        matches!(event, ReplicationEvents::Acknowledged { copy: changed, .. } if changed == copy)
    );
    assert!(started.elapsed() < Duration::from_millis(50));
    shutdown.send_replace(true);
    worker.await?;
    let mut connection = fixture
        .transport
        .files
        .get(&copy)
        .ok_or("missing copy")?
        .connection
        .lock()
        .await;
    let amount = query::<Turso>("SELECT amount FROM text_records")
        .fetch_one(connection.as_mut().ok_or("closed copy")?)
        .await?
        .try_get::<i64, _>("amount")?;
    assert_eq!(amount, 451);
    drop(connection);
    fixture.close().await
}

#[tokio::test]
async fn error_waits_for_retry_after_even_when_change_signals_arrive() -> TestResult {
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    write_amount(&fixture, 73).await?;
    fixture.marks.refuse.store(true, Ordering::SeqCst);
    tokio::time::pause();
    let (shutdown, stopped) = watch::channel(false);
    let (events, mut received) = mpsc::channel(16);
    let sender = fixture.sender.clone();
    let copy = fixture.first;
    let worker = tokio::spawn(async move { sender.run_copy(copy, stopped, events).await });
    let failure = received
        .recv()
        .await
        .ok_or("runner closed")?
        .err()
        .ok_or("bookkeeping did not fail")?;
    assert!(matches!(failure.error, SenderError::Bookkeeping(_)));
    let started = tokio::time::Instant::now();
    fixture.marks.refuse.store(false, Ordering::SeqCst);
    fixture.database.capture_changes.send_replace(());
    fixture.database.authority_changes.send_replace(());
    fixture.database.copies_changes.send_replace(());
    assert!(
        tokio::time::timeout(Duration::from_millis(99), received.recv())
            .await
            .is_err()
    );
    let event = tokio::time::timeout(Duration::from_millis(2), received.recv())
        .await?
        .ok_or("runner closed")??;
    assert!(matches!(event, ReplicationEvents::Acknowledged { .. }));
    assert_eq!(started.elapsed(), Duration::from_millis(100));
    shutdown.send_replace(true);
    worker.await?;
    fixture.close().await
}

#[tokio::test]
async fn progress_drains_multiple_commits_without_a_retry_delay() -> TestResult {
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    write_amount(&fixture, 81).await?;
    write_amount(&fixture, 82).await?;
    tokio::time::pause();
    let (shutdown, stopped) = watch::channel(false);
    let (events, mut received) = mpsc::channel(16);
    let sender = fixture.sender.clone();
    let copy = fixture.first;
    let worker = tokio::spawn(async move { sender.run_copy(copy, stopped, events).await });
    let started = tokio::time::Instant::now();
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_millis(50), received.recv())
            .await?
            .ok_or("runner closed")??;
        assert!(matches!(event, ReplicationEvents::Acknowledged { .. }));
    }
    assert!(started.elapsed() < Duration::from_millis(50));
    shutdown.send_replace(true);
    worker.await?;
    fixture.close().await
}

#[tokio::test]
async fn idle_pass_preserves_warmed_statements_and_does_not_need_the_file_write_lock() -> TestResult
{
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    // Warm all idle paths before adding a sentinel unused by replication.
    fixture.sender.replicate_once(fixture.first).await?;
    let before = {
        let mut connection = fixture.database.maintenance.acquire().await?;
        query::<Turso>("SELECT 734 AS idle_cache_sentinel")
            .fetch_all(&mut *connection)
            .await?;
        connection.cached_statements_size()
    };
    // The capture-full writer owns a RESERVED lock, while the sender only reads.
    let mut writer = fixture.database.writer.acquire().await?;
    let held_write = writer.begin_with("BEGIN IMMEDIATE").await?;
    assert!(
        fixture
            .sender
            .replicate_once(fixture.first)
            .await?
            .is_empty()
    );
    // Acquisition pings the connection and also performs any deferred rollback.
    let after = fixture
        .database
        .maintenance
        .acquire()
        .await?
        .cached_statements_size();
    assert_eq!(
        after, before,
        "idle pass must retain the sentinel and its query cache"
    );
    held_write.rollback().await?;
    drop(writer);
    fixture.close().await
}

#[tokio::test]
async fn capture_watch_signals_only_successful_commits_and_each_replicated_migration() -> TestResult
{
    let fixture = SenderFixture::open().await?;
    let mut changed = fixture.database.subscribe_capture_changes();
    let mut write = fixture.database.begin_write().await?;
    (&mut write)
        .execute("UPDATE text_records SET amount=13")
        .await?;
    assert!(!changed.has_changed()?);
    write.rollback().await?;
    assert!(!changed.has_changed()?);
    write_amount(&fixture, 14).await?;
    assert!(changed.has_changed()?);
    changed.borrow_and_update();
    fixture
        .database
        .schema
        .migration_count
        .store(3, Ordering::SeqCst);
    fixture.database.migrate().await?;
    assert!(changed.has_changed()?);
    fixture.close().await
}

#[tokio::test]
async fn adding_a_copy_wakes_discovery_without_waiting_for_the_fallback() -> TestResult {
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    let checkpoint = fixture
        .sender
        .copies()
        .await?
        .into_iter()
        .find(|status| status.copy == fixture.second)
        .ok_or("missing follower")?
        .acked;
    {
        let mut connection = fixture.database.maintenance.acquire().await?;
        query::<Turso>("DELETE FROM _fleet_sync_copies WHERE copy_id=?")
            .bind(fixture.second.as_uuid().to_string())
            .execute(&mut *connection)
            .await?;
    }
    tokio::time::pause();
    let (shutdown, stopped) = watch::channel(false);
    let (events, _received) = mpsc::channel(16);
    let mut passes = fixture.sender.copy_passes.subscribe();
    let worker = tokio::spawn(fixture.sender.clone().run(stopped, events));
    wait_for_pass(&mut passes, fixture.first, 1).await?;
    let started = tokio::time::Instant::now();
    fixture
        .sender
        .add_copy(
            fixture.second,
            fleet_sync_protocol::CopyRole::Follower,
            checkpoint,
        )
        .await?;
    tokio::time::timeout(
        Duration::from_millis(50),
        passes.wait_for(|counts| counts.contains_key(&fixture.second)),
    )
    .await??;
    assert!(started.elapsed() < Duration::from_millis(50));
    shutdown.send_replace(true);
    worker.await??;
    fixture.close().await
}

#[tokio::test]
async fn completing_a_snapshot_wakes_the_same_idle_copy() -> TestResult {
    let fixture = SenderFixture::open().await?;
    fixture.migrate_copies().await?;
    let mut status = fixture
        .sender
        .copies()
        .await?
        .into_iter()
        .find(|status| status.copy == fixture.second)
        .ok_or("missing follower")?;
    status.needs_snapshot = true;
    {
        let mut connection = fixture.database.maintenance.acquire().await?;
        crate::sender_storage::save_copy(&mut connection, &status).await?;
    }
    write_amount(&fixture, 65).await?;
    tokio::time::pause();
    let (shutdown, stopped) = watch::channel(false);
    let (events, mut received) = mpsc::channel(16);
    let sender = fixture.sender.clone();
    let copy = fixture.second;
    let worker = tokio::spawn(async move { sender.run_copy(copy, stopped, events).await });
    let event = received.recv().await.ok_or("runner closed")??;
    assert!(
        matches!(event, ReplicationEvents::CopyNeedsSnapshot { copy: changed } if changed == copy)
    );
    let started = tokio::time::Instant::now();
    fixture.sender.complete_snapshot(copy, status.acked).await?;
    let event = tokio::time::timeout(Duration::from_millis(50), received.recv())
        .await?
        .ok_or("runner closed")??;
    assert!(
        matches!(event, ReplicationEvents::Acknowledged { copy: changed, .. } if changed == copy)
    );
    assert!(started.elapsed() < Duration::from_millis(50));
    shutdown.send_replace(true);
    worker.await?;
    fixture.close().await
}

#[tokio::test]
async fn registration_copy_notification_is_published_after_its_journal_commit() -> TestResult {
    let fixture = SenderFixture::open().await?;
    let store = fixture
        .database
        .journal_store(std::sync::Arc::new(crate::writer_journal_tests::LocalHost))
        .await?;
    let mut changed = fixture.database.copies_changes.subscribe();
    let receiver = fleet_sync_protocol::ReceiverId::new(durable_steps::Uuid::now_v7())?;
    let start = fixture
        .sender
        .copies()
        .await?
        .first()
        .ok_or("missing copy")?
        .start;
    store
        .rule_transaction(move |scope| {
            Box::pin(async move {
                scope
                    .register_sender_copy(receiver, start)
                    .await
                    .map_err(durable_steps::StoreError::backend)
            })
        })
        .await?;
    assert!(changed.has_changed()?);
    changed.borrow_and_update();
    let receiver = fleet_sync_protocol::ReceiverId::new(durable_steps::Uuid::now_v7())?;
    let result: Result<(), durable_steps::StoreError> = store
        .rule_transaction(move |scope| {
            Box::pin(async move {
                scope
                    .register_sender_copy(receiver, start)
                    .await
                    .map_err(durable_steps::StoreError::backend)?;
                Err(durable_steps::StoreError::backend("roll back registration"))
            })
        })
        .await;
    assert!(result.is_err());
    assert!(!changed.has_changed()?);
    fixture.close().await
}
