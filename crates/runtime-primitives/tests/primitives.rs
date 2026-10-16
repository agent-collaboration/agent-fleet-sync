// Match the workspace's Result-returning integration-test convention.
#![allow(clippy::panic_in_result_fn)]

use runtime_primitives::{Backoff, SyncTime};
use std::time::Duration;

#[test]
fn backoff_keeps_growing_past_a_u32_multiplier() {
    let backoff = Backoff {
        initial: Duration::from_nanos(1),
        max: Duration::from_secs(10),
        factor: 2,
    };
    assert_eq!(backoff.delay(40), Duration::from_secs(10));
}

#[test]
fn backoff_matches_exponential_sequence_and_caps_overflow() {
    let backoff = Backoff {
        initial: Duration::from_millis(10),
        max: Duration::from_millis(75),
        factor: 2,
    };
    assert_eq!(
        (0..5)
            .map(|attempt| backoff.delay(attempt))
            .collect::<Vec<_>>(),
        vec![10, 20, 40, 75, 75]
            .into_iter()
            .map(Duration::from_millis)
            .collect::<Vec<_>>()
    );
    assert_eq!(backoff.delay(u32::MAX), Duration::from_millis(75));
}

#[test]
fn backoff_respects_zero_initial_and_a_maximum_below_initial() {
    assert_eq!(
        Backoff {
            initial: Duration::ZERO,
            max: Duration::from_secs(1),
            factor: 2
        }
        .delay(99),
        Duration::ZERO
    );
    assert_eq!(
        Backoff {
            initial: Duration::from_secs(2),
            max: Duration::from_secs(1),
            factor: 2
        }
        .delay(0),
        Duration::from_secs(1)
    );
}

#[test]
fn time_roundtrips_exact_milliseconds_and_saturates_deadlines() -> Result<(), serde_json::Error> {
    let at = SyncTime::from_unix_millis(9_007_199_254_740_993);
    assert_eq!(serde_json::to_string(&at)?, "9007199254740993");
    assert_eq!(serde_json::from_str::<SyncTime>("9007199254740993")?, at);
    assert_eq!(
        SyncTime::from_unix_millis(-1)
            .after(Duration::from_millis(2))
            .unix_millis(),
        1
    );
    assert_eq!(
        SyncTime::from_unix_millis(i64::MAX - 1)
            .after(Duration::from_millis(5))
            .unix_millis(),
        i64::MAX
    );
    Ok(())
}
