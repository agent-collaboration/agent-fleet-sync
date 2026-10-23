#![cfg(feature = "tokio")]
// Assertions remain test failures while setup/timeout errors propagate normally.
#![allow(clippy::panic_in_result_fn)]

use runtime_primitives::{
    Clock, IdSource, SyncTime, SystemClock, TokioWakeTimer, UuidV7Ids, WakeScheduler,
};
use std::{
    future::{Future, poll_fn},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    task::Poll,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

struct TestClock {
    epoch: tokio::time::Instant,
}
impl Clock for TestClock {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(
            i64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(i64::MAX),
        )
    }
}
fn timer() -> TokioWakeTimer<TestClock> {
    TokioWakeTimer::new(TestClock {
        epoch: tokio::time::Instant::now(),
    })
}

#[derive(Clone)]
struct ManualClock {
    milliseconds: Arc<AtomicI64>,
}

impl ManualClock {
    fn new(milliseconds: i64) -> Self {
        Self {
            milliseconds: Arc::new(AtomicI64::new(milliseconds)),
        }
    }

    fn set(&self, milliseconds: i64) {
        self.milliseconds.store(milliseconds, Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(self.milliseconds.load(Ordering::Relaxed))
    }
}

#[tokio::test(start_paused = true)]
async fn a_forward_clock_jump_rearms_a_due_wake_without_a_duplicate()
-> Result<(), Box<dyn std::error::Error>> {
    // Arrange: the timer is waiting on a physical trigger 100 ms away.
    let clock = ManualClock::new(0);
    let timer = TokioWakeTimer::new(clock.clone());
    timer.wake_at(SyncTime::from_unix_millis(100)).await?;
    let mut waiting = Box::pin(timer.wait());
    assert!(
        poll_fn(|context| Poll::Ready(waiting.as_mut().poll(context)))
            .await
            .is_pending()
    );

    // Act: only the domain clock jumps; Tokio time has not advanced.
    clock.set(30_000);
    timer.wake_at(SyncTime::from_unix_millis(30_000)).await?;

    // Assert: retain the earlier logical wake, but consume it promptly.
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(2), waiting).await?,
        SyncTime::from_unix_millis(100)
    );
    let mut next_wait = Box::pin(timer.wait());
    assert!(
        poll_fn(|context| Poll::Ready(next_wait.as_mut().poll(context)))
            .await
            .is_pending()
    );
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(
        poll_fn(|context| Poll::Ready(next_wait.as_mut().poll(context)))
            .await
            .is_pending(),
        "the replaced physical trigger must not fire a second wake"
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_forward_clock_jump_rechecks_the_pending_deadline_for_any_request()
-> Result<(), Box<dyn std::error::Error>> {
    // A repeated deadline and a new future request both need to recheck the
    // earlier pending wake against the current clock.
    for requested_milliseconds in [100, 30_100] {
        let clock = ManualClock::new(0);
        let timer = TokioWakeTimer::new(clock.clone());
        timer.wake_at(SyncTime::from_unix_millis(100)).await?;

        clock.set(30_000);
        timer
            .wake_at(SyncTime::from_unix_millis(requested_milliseconds))
            .await?;

        assert_eq!(
            tokio::time::timeout(Duration::from_millis(2), timer.wait()).await,
            Ok(SyncTime::from_unix_millis(100)),
            "request {requested_milliseconds} must rearm the due pending wake"
        );
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_backward_clock_jump_preserves_the_earlier_physical_trigger()
-> Result<(), Box<dyn std::error::Error>> {
    let clock = ManualClock::new(0);
    let timer = TokioWakeTimer::new(clock.clone());
    timer.wake_at(SyncTime::from_unix_millis(100)).await?;

    // The newly earlier logical deadline maps to a later physical trigger.
    clock.set(-30_000);
    timer.wake_at(SyncTime::from_unix_millis(50)).await?;

    assert_eq!(
        tokio::time::timeout(Duration::from_millis(101), timer.wait()).await?,
        SyncTime::from_unix_millis(50)
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn later_or_repeated_request_never_postpones_the_earlier_wake()
-> Result<(), Box<dyn std::error::Error>> {
    let timer = timer();
    timer.wake_at(SyncTime::from_unix_millis(10)).await?;
    timer.wake_at(SyncTime::from_unix_millis(100)).await?;
    timer.wake_at(SyncTime::from_unix_millis(10)).await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(20), timer.wait()).await?,
        SyncTime::from_unix_millis(10)
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn an_earlier_request_interrupts_an_already_waiting_timer()
-> Result<(), Box<dyn std::error::Error>> {
    let timer = timer();
    timer.wake_at(SyncTime::from_unix_millis(100)).await?;
    let mut waiting = Box::pin(timer.wait());
    assert!(
        poll_fn(|context| Poll::Ready(waiting.as_mut().poll(context)))
            .await
            .is_pending()
    );
    timer.wake_at(SyncTime::from_unix_millis(10)).await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(20), waiting).await?,
        SyncTime::from_unix_millis(10)
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_wait_preserves_its_wake_and_consumption_allows_the_next()
-> Result<(), Box<dyn std::error::Error>> {
    let timer = timer();
    timer.wake_at(SyncTime::from_unix_millis(10)).await?;
    {
        let mut waiting = Box::pin(timer.wait());
        assert!(
            poll_fn(|context| Poll::Ready(waiting.as_mut().poll(context)))
                .await
                .is_pending()
        );
    }
    assert_eq!(timer.wait().await, SyncTime::from_unix_millis(10));
    timer.wake_at(SyncTime::from_unix_millis(30)).await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(25), timer.wait()).await?,
        SyncTime::from_unix_millis(30)
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn an_idle_wait_is_signalled_and_overdue_work_fires_immediately()
-> Result<(), Box<dyn std::error::Error>> {
    let timer = timer();
    let mut waiting = Box::pin(timer.wait());
    assert!(
        poll_fn(|context| Poll::Ready(waiting.as_mut().poll(context)))
            .await
            .is_pending()
    );
    timer.wake_at(SyncTime::from_unix_millis(-1)).await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(1), waiting).await?,
        SyncTime::from_unix_millis(-1)
    );
    Ok(())
}

#[test]
fn native_clock_and_id_source_use_wall_time_and_distinct_uuid_v7s()
-> Result<(), Box<dyn std::error::Error>> {
    let before = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let observed = SystemClock.now().unix_millis();
    let after = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    assert!(
        (before..=after).contains(&observed),
        "clock {observed} outside independent wall-time bracket {before}..={after}"
    );
    let first = UuidV7Ids.new_id();
    let second = UuidV7Ids.new_id();
    assert_ne!(first, second);
    assert_eq!(first.get_version_num(), 7);
    assert_eq!(second.get_version_num(), 7);
    Ok(())
}
