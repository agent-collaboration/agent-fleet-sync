use crate::{
    conformance::{self, BusinessScope},
    support::TestHost,
    worker_error,
};
use durable_steps::{
    AtomicStore, JournalScope, ReborrowScope, StoreError, durable_object_store::DurableObjectStore,
    storage::TransactionScope,
};
use futures_util::future::join;
use runtime_primitives::{AlarmOwner, SyncTime, WakeScheduler};
use sqlx_core::executor::Executor;
use std::{sync::Arc, time::Duration};
use worker::{
    State,
    send::{SendFuture, SendWrapper},
};

type Store = DurableObjectStore<TestHost>;

pub async fn run(
    state: &State,
    alarm: &AlarmOwner,
    store: Arc<Store>,
    now: SyncTime,
) -> worker::Result<Vec<&'static str>> {
    conformance::reborrow_cases::commit_and_rollback(store.clone(), now)
        .await
        .map_err(worker_error)?;
    let mut passed = vec!["SQL and journal share parent commit and rollback"];

    let original = alarm.arbitration_identity_for_test();
    let shared = alarm.clone();
    let independent = AlarmOwner::new(state.storage());
    conformance::check(
        original == shared.arbitration_identity_for_test()
            && original != independent.arbitration_identity_for_test(),
        "identity observer accepts shared arbitration and rejects a second owner on the same storage",
    ).map_err(worker_error)?;
    store
        .transaction(move |parent| {
            Box::pin(async move {
                let before_loan = parent.alarm_arbitration_identity_for_test();
                let view = Store::reborrow(parent);
                conformance::check(
                    before_loan == original
                        && view.alarm_arbitration_identity_for_test() == before_loan,
                    "view retains the parent's shared alarm arbitration state",
                )
            })
        })
        .await
        .map_err(worker_error)?;
    passed.push("parent and view share the observed arbitration owner");

    // Check both an absent and a pre-existing alarm, with callback and SQL
    // failures after an earlier wake. All deadlines stay ahead of workerd.
    let prior = now.after(Duration::from_secs(100));
    let earlier = now.after(Duration::from_secs(50));
    for existing in [false, true] {
        for sql_error in [false, true] {
            state.storage().delete_alarm().await?;
            if existing {
                alarm.wake_at(prior).await.map_err(worker_error)?;
            }
            let before = state.storage().get_alarm().await?;
            conformance::check(
                before
                    == if existing {
                        Some(prior.unix_millis())
                    } else {
                        None
                    },
                "rollback control starts with the requested exact alarm state",
            )
            .map_err(worker_error)?;
            let storage = SendWrapper::new(state.storage());
            let aborted = store
                .transaction(move |parent| {
                    Box::pin(async move {
                        let mut view = Store::reborrow(parent);
                        view.write_counter("reborrow-alarm-rollback", 1).await?;
                        view.ensure_wake(earlier).await?;
                        let inside = SendFuture::new(storage.get_alarm()).await.map_err(StoreError::backend)?;
                        conformance::check(inside == Some(earlier.unix_millis()), "view wake changes the actual durable alarm before the deliberate failure")?;
                        if sql_error {
                            view.executor()
                                .execute("INSERT INTO reborrow_missing_table VALUES (1)")
                                .await?;
                            return Err(StoreError::backend(
                                "missing-table SQL unexpectedly succeeded",
                            ));
                        }
                        Err::<(), _>(StoreError::Aborted {
                            reason: "reborrow wake rollback".into(),
                        })
                    })
                })
                .await;
            conformance::check(
                if sql_error {
                    matches!(aborted, Err(StoreError::Backend { message }) if message.contains("reborrow_missing_table") && !message.contains("unexpectedly succeeded"))
                } else {
                    matches!(aborted, Err(StoreError::Aborted { reason }) if reason == "reborrow wake rollback")
                },
                "the failure after a view wake reaches the parent transaction",
            ).map_err(worker_error)?;
            let after = state.storage().get_alarm().await?;
            conformance::check(
                after == before,
                "durable alarm equals its exact pre-transaction value after rollback",
            )
            .map_err(worker_error)?;
            store
                .transaction(|parent| {
                    Box::pin(async move {
                        conformance::check(
                            parent
                                .read_counter("reborrow-alarm-rollback")
                                .await?
                                .is_none(),
                            "SQL also rolls back after the view wake",
                        )
                    })
                })
                .await
                .map_err(worker_error)?;
            passed.push(match (existing, sql_error) {
                (false, false) => "callback rollback restores absent alarm",
                (false, true) => "SQL rollback restores absent alarm",
                (true, false) => "callback rollback restores existing alarm",
                (true, true) => "SQL rollback restores existing alarm",
            });
        }
    }

    for external_first in [false, true] {
        state.storage().delete_alarm().await?;
        let external = alarm.clone();
        store
            .transaction(move |parent| {
                Box::pin(async move {
                    let before_loan = parent.alarm_arbitration_identity_for_test();
                    let mut view = Store::reborrow(parent);
                    conformance::check(
                        view.alarm_arbitration_identity_for_test() == before_loan
                            && before_loan == external.arbitration_identity_for_test(),
                        "overlapping wakes use one observed arbitration owner",
                    )?;
                    if external_first {
                        let (outside, inside) =
                            join(external.wake_at(prior), view.ensure_wake(earlier)).await;
                        outside.map_err(StoreError::backend)?;
                        inside?;
                    } else {
                        let (inside, outside) =
                            join(view.ensure_wake(earlier), external.wake_at(prior)).await;
                        inside?;
                        outside.map_err(StoreError::backend)?;
                    }
                    view.write_counter("reborrow-alarm-commit", 1).await?;
                    Ok(())
                })
            })
            .await
            .map_err(worker_error)?;
        conformance::check(
            state.storage().get_alarm().await? == Some(earlier.unix_millis()),
            "overlapping view and external wakes commit the earliest deadline",
        )
        .map_err(worker_error)?;
        passed.push(if external_first {
            "external then view overlapping wake"
        } else {
            "view then external overlapping wake"
        });
    }
    Ok(passed)
}
