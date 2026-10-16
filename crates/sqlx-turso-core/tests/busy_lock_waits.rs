#![cfg(unix)]
#![allow(clippy::panic_in_result_fn)]

use std::time::{Duration, Instant};

use nix::{
    sys::time::TimeValLike,
    time::{ClockId, clock_gettime},
};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    query_scalar::query_scalar,
};
use sqlx_turso_core::TursoConnectOptions;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn thread_cpu_microseconds() -> Result<i64, nix::errno::Errno> {
    Ok(clock_gettime(ClockId::CLOCK_THREAD_CPUTIME_ID)?.num_microseconds())
}

async fn measure_waiting_cpu<TFuture: std::future::Future>(
    waiting: TFuture,
) -> Result<(TFuture::Output, i64), nix::errno::Errno> {
    let mut waiting = std::pin::pin!(waiting);
    let mut cpu_used = 0;
    // The future may migrate between executor threads. Count the current thread's
    // CPU only while polling this future, excluding other tasks and pending time.
    std::future::poll_fn(|context| {
        let before = match thread_cpu_microseconds() {
            Ok(value) => value,
            Err(error) => return std::task::Poll::Ready(Err(error)),
        };
        let result = waiting.as_mut().poll(context);
        let after = match thread_cpu_microseconds() {
            Ok(value) => value,
            Err(error) => return std::task::Poll::Ready(Err(error)),
        };
        cpu_used += after - before;
        result.map(|value| Ok((value, cpu_used)))
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waiting_cpu_excludes_work_on_another_thread() -> TestResult {
    let (start_sender, start_receiver) = std::sync::mpsc::channel();
    let (finished_sender, finished_receiver) = tokio::sync::oneshot::channel();
    let other_thread = std::thread::spawn(move || -> TestResult {
        start_receiver.recv()?;
        let before = thread_cpu_microseconds()?;
        let mut checksum = 1_u64;
        while thread_cpu_microseconds()? - before < 300_000 {
            for _ in 0..1024 {
                checksum = std::hint::black_box(checksum.wrapping_mul(3).wrapping_add(1));
            }
        }
        finished_sender
            .send(checksum)
            .map_err(|_| "waiting task dropped")?;
        Ok(())
    });
    let (finished, cpu_used) = tokio::time::timeout(
        Duration::from_secs(10),
        measure_waiting_cpu(async {
            start_sender.send(())?;
            Ok::<u64, Box<dyn std::error::Error + Send + Sync>>(finished_receiver.await?)
        }),
    )
    .await??;
    finished?;
    other_thread.join().map_err(|_| "CPU worker panicked")??;
    assert!(
        cpu_used < 200_000,
        "unrelated work charged {cpu_used}us to the waiting task"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contended_begin_waits_for_release_with_low_task_cpu() -> TestResult {
    // Arrange: measure only the contended real-engine path, after setup has completed.
    let directory = tempfile::tempdir()?;
    let options = TursoConnectOptions::new()
        .filename(directory.path().join("contended.db"))
        .create_if_missing(true);
    let mut lock_holder = options.connect().await?;
    let mut waiter = options.connect().await?;
    lock_holder
        .execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .await?;
    let mut held_transaction = lock_holder.begin_with("BEGIN IMMEDIATE").await?;
    held_transaction
        .execute("INSERT INTO items (id) VALUES (1)")
        .await?;
    let wait_started = Instant::now();

    // Act: the two-second timer defines the required lock hold, rather than polling for it.
    let release_lock = async {
        tokio::time::sleep(Duration::from_secs(2)).await;
        held_transaction.commit().await
    };
    let wait_for_lock = async {
        let mut transaction = waiter.begin_with("BEGIN IMMEDIATE").await?;
        transaction
            .execute("INSERT INTO items (id) VALUES (2)")
            .await?;
        transaction.commit().await
    };
    let (release_result, waiter_result) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(release_lock, measure_waiting_cpu(wait_for_lock))
    })
    .await?;
    let elapsed = wait_started.elapsed();
    let (waiter_result, cpu_used) = waiter_result?;
    release_result?;
    waiter_result?;
    let committed_count: i64 = query_scalar("SELECT count(*) FROM items")
        .fetch_one(&mut waiter)
        .await?;

    // Assert: the waiter acquired the real held lock and committed exactly once.
    eprintln!("contended lock wait: wall={elapsed:?}, waiting_task_cpu={cpu_used}us");
    assert!(elapsed >= Duration::from_secs(2));
    assert!(cpu_used < 200_000, "lock wait used {cpu_used}us CPU");
    assert_eq!(committed_count, 2);
    Ok(())
}

#[tokio::test]
async fn held_lock_returns_busy_at_the_configured_deadline_and_can_be_reused() -> TestResult {
    // Arrange: leave the lock held past the default five-second budget.
    let directory = tempfile::tempdir()?;
    let options = TursoConnectOptions::new()
        .filename(directory.path().join("deadline.db"))
        .create_if_missing(true);
    let mut lock_holder = options.connect().await?;
    let mut waiter = options.connect().await?;
    lock_holder
        .execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .await?;
    let held_transaction = lock_holder.begin_with("BEGIN IMMEDIATE").await?;

    // Act: deadline failure must leave no half-open waiter transaction.
    let wait_started = Instant::now();
    let failed_begin = tokio::time::timeout(
        Duration::from_secs(10),
        waiter.begin_with("BEGIN IMMEDIATE"),
    )
    .await?;
    let elapsed = wait_started.elapsed();
    let busy_error = failed_begin.expect_err("the holder has not released its lock");
    held_transaction.rollback().await?;
    let mut transaction = waiter.begin_with("BEGIN IMMEDIATE").await?;
    transaction
        .execute("INSERT INTO items (id) VALUES (1)")
        .await?;
    transaction.commit().await?;
    let committed_count: i64 = query_scalar("SELECT count(*) FROM items")
        .fetch_one(&mut waiter)
        .await?;

    // Assert: retain the database failure code, the configured budget and reuse semantics.
    assert_eq!(
        busy_error
            .as_database_error()
            .and_then(|error| error.code()),
        Some("SQLITE_BUSY".into())
    );
    assert!(elapsed >= Duration::from_secs(5));
    assert!(
        elapsed < Duration::from_secs(7),
        "busy deadline: {elapsed:?}"
    );
    assert_eq!(committed_count, 1);
    Ok(())
}

#[tokio::test]
async fn busy_batch_keeps_its_earlier_begin_and_cannot_be_replayed_as_a_whole() -> TestResult {
    // Arrange: BEGIN succeeds before the INSERT meets another connection's write lock.
    let directory = tempfile::tempdir()?;
    let options = TursoConnectOptions::new()
        .filename(directory.path().join("partial-batch.db"))
        .create_if_missing(true);
    let mut lock_holder = options.connect().await?;
    let mut waiter = options
        .clone()
        .busy_timeout(Duration::ZERO)
        .connect()
        .await?;
    lock_holder
        .execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .await?;
    let held_transaction = lock_holder.begin_with("BEGIN IMMEDIATE").await?;
    let batch_sql = "BEGIN; INSERT INTO items (id) VALUES (1); COMMIT";

    // Act: the SDK reports the busy statement without undoing the earlier BEGIN.
    let first_error = waiter
        .execute(batch_sql)
        .await
        .expect_err("write lock held");
    held_transaction.rollback().await?;
    let replay_error = waiter
        .execute(batch_sql)
        .await
        .expect_err("replaying BEGIN is not resuming the failed statement");
    waiter.execute("ROLLBACK").await?;

    // Assert: a blanket whole-batch retry changes the failure and cannot honor the wait.
    assert_eq!(
        first_error
            .as_database_error()
            .and_then(|error| error.code()),
        Some("SQLITE_BUSY".into())
    );
    assert!(replay_error.to_string().contains("within a transaction"));
    let committed_count: i64 = query_scalar("SELECT count(*) FROM items")
        .fetch_one(&mut waiter)
        .await?;
    assert_eq!(committed_count, 0);
    Ok(())
}

#[tokio::test]
async fn dropped_pooled_transaction_releases_its_lock_before_an_idle_connection_is_reused()
-> TestResult {
    use sqlx_core::pool::PoolOptions;
    use sqlx_turso_core::Turso;

    let directory = tempfile::tempdir()?;
    let options = TursoConnectOptions::new()
        .filename(directory.path().join("dropped-pool.db"))
        .create_if_missing(true);
    let mut observer = options
        .clone()
        .busy_timeout(Duration::ZERO)
        .connect()
        .await?;
    observer
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")
        .await?;
    let pool = PoolOptions::<Turso>::new()
        .max_connections(1)
        .min_connections(1)
        .test_before_acquire(false)
        .max_lifetime(None)
        .idle_timeout(None)
        .connect_with(options)
        .await?;
    let mut transaction = pool.begin().await?;
    transaction
        .execute("INSERT INTO items(id) VALUES(1)")
        .await?;
    drop(transaction);

    // Acquiring the idle slot closes the pool's return owner. Disable acquire ping
    // and issue no SQL on that connection: next-use cleanup cannot mask the defect.
    let idle = pool.acquire().await?;
    let independent_begin = observer.begin_with("BEGIN IMMEDIATE").await;
    let mut independent =
        independent_begin.expect("idle pool connection still holds the dropped write lock");
    independent
        .execute("INSERT INTO items(id) VALUES(2)")
        .await?;
    independent.commit().await?;
    let ids: Vec<i64> = sqlx_core::query_scalar::query_scalar("SELECT id FROM items ORDER BY id")
        .fetch_all(&mut observer)
        .await?;
    assert_eq!(
        ids,
        vec![2],
        "the dropped write rolled back before a separate writer entered"
    );
    drop(idle);
    pool.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_waiting_batch_resumes_the_busy_statement_without_repeating_its_begin() -> TestResult {
    let directory = tempfile::tempdir()?;
    let options = TursoConnectOptions::new()
        .filename(directory.path().join("resumed-batch.db"))
        .create_if_missing(true);
    let mut holder = options.connect().await?;
    let mut waiter = options.connect().await?;
    holder
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")
        .await?;
    let held = holder.begin_with("BEGIN IMMEDIATE").await?;
    let (release, batch) = tokio::join!(
        async {
            tokio::time::sleep(Duration::from_secs(2)).await;
            held.rollback().await
        },
        measure_waiting_cpu(waiter.execute("BEGIN; INSERT INTO items(id) VALUES(1); COMMIT"))
    );
    release?;
    let (batch, cpu_used) = batch?;
    batch?;
    let count: i64 = query_scalar("SELECT count(*) FROM items")
        .fetch_one(&mut waiter)
        .await?;
    eprintln!("contended batch waiting_task_cpu={cpu_used}us");
    assert_eq!(count, 1);
    assert!(cpu_used < 200_000, "batch wait used {cpu_used}us CPU");
    Ok(())
}
