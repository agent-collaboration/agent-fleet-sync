#![cfg(feature = "tokio")]
// Assertions remain test failures while setup/timeout errors propagate normally.
#![allow(clippy::panic_in_result_fn)]

use runtime_primitives::{
    Clock, IdSource, SyncTime, SystemClock, TokioWakeTimer, UuidV7Ids, WakeScheduler,
};
use std::{
    future::{Future, poll_fn},
    task::Poll,
    time::Duration,
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
fn native_clock_and_id_source_use_wall_time_and_distinct_uuid_v7s() {
    let before = SystemClock.now();
    let first = UuidV7Ids.new_id();
    let second = UuidV7Ids.new_id();
    let after = SystemClock.now();
    assert!(after >= before);
    assert_ne!(first, second);
    assert_eq!(first.get_version_num(), 7);
    assert_eq!(second.get_version_num(), 7);
}
