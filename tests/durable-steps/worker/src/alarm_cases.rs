//! Poll actual adapter clones together in the same object event.
use crate::{conformance::BusinessScope, support::TestHost, worker_error};
use durable_steps::{
    AtomicStore, JournalScope, StoreError, durable_object_store::DurableObjectStore,
};
use futures_util::future::join;
use runtime_primitives::{AlarmOwner, SyncTime, WakeScheduler};
use std::{sync::Arc, time::Duration};
use worker::State;

#[derive(serde::Serialize)]
pub struct AlarmObservation {
    order: &'static str,
    earlier: i64,
    later: i64,
    actual: Option<i64>,
}

pub async fn overlapping_wakes(
    state: &State,
    alarm: &AlarmOwner,
    store: Arc<DurableObjectStore<TestHost>>,
    now: SyncTime,
) -> worker::Result<Vec<AlarmObservation>> {
    let earlier = now.after(Duration::from_secs(50));
    let later = now.after(Duration::from_secs(100));
    let mut observations = Vec::new();
    for late_first in [true, false] {
        state.storage().delete_alarm().await?;
        let first = alarm.clone();
        let second = alarm.clone();
        let (first_at, second_at, order) = if late_first {
            (later, earlier, "late_then_early")
        } else {
            (earlier, later, "early_then_late")
        };
        let (one, two) = join(first.wake_at(first_at), second.wake_at(second_at)).await;
        one.map_err(worker_error)?;
        two.map_err(worker_error)?;
        observations.push(observe(state, earlier, later, order).await?);

        // The same exclusion must work within a caller-owned transaction.
        state.storage().delete_alarm().await?;
        store
            .transaction(move |scope| {
                Box::pin(async move {
                    scope.write_counter("alarm-concurrent-pair", 1).await?;
                    let (one, two) = join(first.wake_at(first_at), second.wake_at(second_at)).await;
                    one.map_err(StoreError::backend)?;
                    two.map_err(StoreError::backend)?;
                    Ok(())
                })
            })
            .await
            .map_err(worker_error)?;
        observations.push(
            observe(
                state,
                earlier,
                later,
                if late_first {
                    "transaction_late_then_early"
                } else {
                    "transaction_early_then_late"
                },
            )
            .await?,
        );

        state.storage().delete_alarm().await?;
        let one = store.transaction(move |scope| {
            Box::pin(async move {
                scope.write_counter("alarm-transaction-one", 1).await?;
                scope.ensure_wake(first_at).await?;
                Ok(())
            })
        });
        let two = store.transaction(move |scope| {
            Box::pin(async move {
                scope.write_counter("alarm-transaction-two", 1).await?;
                scope.ensure_wake(second_at).await?;
                Ok(())
            })
        });
        let (one, two) = join(one, two).await;
        one.map_err(worker_error)?;
        two.map_err(worker_error)?;
        observations.push(
            observe(
                state,
                earlier,
                later,
                if late_first {
                    "transactions_late_then_early"
                } else {
                    "transactions_early_then_late"
                },
            )
            .await?,
        );
    }

    // A post-commit rearm can overlap a new deadline-writing transaction.
    // Poll both orders so the async exclusion cannot invert the storage gate.
    for transaction_first in [true, false] {
        state.storage().delete_alarm().await?;
        let inside = store.transaction(move |scope| {
            Box::pin(async move {
                scope.write_counter("alarm-concurrent", 1).await?;
                scope.ensure_wake(earlier).await?;
                Ok(())
            })
        });
        let outside = alarm.wake_at(later);
        if transaction_first {
            let (one, two) = join(inside, outside).await;
            one.map_err(worker_error)?;
            two.map_err(worker_error)?;
        } else {
            let (one, two) = join(outside, inside).await;
            one.map_err(worker_error)?;
            two.map_err(worker_error)?;
        }
        observations.push(
            observe(
                state,
                earlier,
                later,
                if transaction_first {
                    "transaction_then_external"
                } else {
                    "external_then_transaction"
                },
            )
            .await?,
        );
    }
    Ok(observations)
}

async fn observe(
    state: &State,
    earlier: SyncTime,
    later: SyncTime,
    order: &'static str,
) -> worker::Result<AlarmObservation> {
    let actual = state.storage().get_alarm().await?;
    if actual != Some(earlier.unix_millis()) {
        return Err(worker_error(format!(
            "overlapping {order} wake requests postponed the earliest alarm: expected {}, actual {actual:?}",
            earlier.unix_millis(),
        )));
    }
    Ok(AlarmObservation {
        order,
        earlier: earlier.unix_millis(),
        later: later.unix_millis(),
        actual,
    })
}
