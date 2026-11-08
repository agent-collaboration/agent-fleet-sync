//! Shared product conformance: identical scenarios for volatile, Turso and DO.
#![allow(clippy::panic, clippy::panic_in_result_fn, clippy::expect_used)]

mod call_cases;
mod event_resume_cases;
pub mod reborrow_cases;
mod reconciliation_cases;
mod resume_cases;
mod resume_rollback_case;
mod resume_token_case;
mod retry_bound_cases;
pub mod retryable_cases;
mod task_cases;
mod task_fixtures;

use crate::support::TestHost;
use durable_steps::{AtomicStore, JournalScope, RequestKey, StepName, StoreError};
use std::{future::Future, sync::Arc};

pub trait BusinessScope: JournalScope {
    fn read_counter(
        &mut self,
        key: &str,
    ) -> impl Future<Output = Result<Option<u64>, StoreError>> + Send;
    fn write_counter(
        &mut self,
        key: &str,
        value: u64,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

pub fn key(value: &str) -> RequestKey {
    RequestKey::new(value).expect("nonempty fixture key")
}
pub fn step(value: &str) -> StepName {
    StepName::new(value).expect("nonempty fixture step")
}
pub fn check(holds: bool, message: &str) -> Result<(), StoreError> {
    if holds {
        Ok(())
    } else {
        Err(StoreError::Aborted {
            reason: message.into(),
        })
    }
}

pub async fn run<S>(store: Arc<S>, host: Arc<TestHost>) -> Result<Vec<&'static str>, StoreError>
where
    S: AtomicStore,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let mut passed = Vec::new();
    event_resume_cases::named_operation_only(store.clone(), host.clone()).await?;
    passed.push("drive_one advances only its named operation");
    event_resume_cases::resume_backoff(store.clone(), host.clone()).await?;
    passed.push("make_due resumes backoff with the same CallId without clock advance");
    event_resume_cases::preserve_live_lease(store.clone(), host.clone()).await?;
    passed.push("make_due preserves live lease exclusion across engines");
    reconciliation_cases::retryable_lookup(store.clone(), host.clone(), false).await?;
    passed.push("Retryable recovered lookup retains MayHaveReached and retries to an answer");
    reconciliation_cases::retryable_lookup(store.clone(), host.clone(), true).await?;
    passed.push("Retryable live lookup retries without another dispatch");
    reconciliation_cases::live_non_retryable(store.clone(), host.clone()).await?;
    passed.push("non-retryable live lookup remains Uncertain");
    reconciliation_cases::live_lost_answer(store.clone(), host.clone()).await?;
    passed.push("live AnswerLost reconciles once without another dispatch");
    reconciliation_cases::recovered_answer(store.clone(), host.clone()).await?;
    passed.push(
        "read-only reconciliation saves recovered answer without dispatch and replays CallDone",
    );
    reconciliation_cases::missing_answer(store.clone(), host.clone()).await?;
    passed.push("missing read-only reconciliation stays Uncertain");
    reconciliation_cases::failures_preserve_possible_reach(store.clone(), host.clone()).await?;
    passed.push("reconciliation failures preserve possible effect without dispatch");
    reconciliation_cases::default_target(store.clone(), host.clone()).await?;
    passed.push("default target preserves nonrepeating Uncertain behavior");
    reconciliation_cases::deduplicating_target(store.clone(), host.clone()).await?;
    passed.push("deduplicating recovery never reconciles");
    reconciliation_cases::fresh_and_safe_retries(store.clone(), host.clone()).await?;
    passed.push("fresh, NeverSent and NoEffect calls dispatch without a preflight lookup");
    task_cases::running_keys(Arc::clone(&store), Arc::clone(&host)).await?;
    passed.push("running request keys are kind-scoped and share journal commit/rollback");
    task_cases::admission_and_rollback(Arc::clone(&store), Arc::clone(&host)).await?;
    passed.push("mixed rollback, exact input, duplicate and conflict");
    task_cases::stale_token(Arc::clone(&store), Arc::clone(&host)).await?;
    passed.push("stale drive token");
    task_cases::parent_and_retention(Arc::clone(&store), Arc::clone(&host)).await?;
    passed.push("generic parent/child rollback, replay and compact retention");
    task_cases::failure_hook(Arc::clone(&store), Arc::clone(&host)).await?;
    passed.push("version failure hook and owed child commit atomically");
    task_cases::delete_at_end(Arc::clone(&store), Arc::clone(&host)).await?;
    passed.push("KEEP_RESULT=false deletes terminal journal");
    call_cases::never_sent_replays(Arc::clone(&store), Arc::clone(&host)).await?;
    passed.push("bounded NotSent saves NeverSent and replays it");
    call_cases::lost_answer_prevents_give_up(Arc::clone(&store), Arc::clone(&host)).await?;
    passed.push("AnswerLost prevents bounded give-up after NotSent");
    call_cases::answer_after_not_sent(Arc::clone(&store), Arc::clone(&host)).await?;
    passed.push("answer after NotSent");
    call_cases::non_deduplicating_lost_answer(Arc::clone(&store), Arc::clone(&host)).await?;
    passed.push("non-deduplicating answer loss is Uncertain");
    call_cases::in_flight_recovery(Arc::clone(&store), Arc::clone(&host)).await?;
    passed.push("in-flight dispatch prevents false NeverSent");
    call_cases::non_deduplicating_in_flight(Arc::clone(&store), Arc::clone(&host)).await?;
    passed.push("non-deduplicating in-flight recovery is Uncertain");
    resume_cases::finish_unanswered(Arc::clone(&store), Arc::clone(&host)).await?;
    passed.push("resume hook finishes unanswered calls with normal retention");
    resume_cases::continue_replays(Arc::clone(&store), Arc::clone(&host)).await?;
    passed.push("resume hook Continue preserves normal replay");
    resume_rollback_case::finish_rolls_back_with_hook_writes(Arc::clone(&store), Arc::clone(&host))
        .await?;
    passed.push("resume hook writes and Finish roll back together before retry");
    resume_token_case::stale_hook_is_not_invoked(store.clone(), host.clone()).await?;
    passed.push("stale drive cannot invoke the resume hook");
    retryable_cases::safe_retry_after_engine_restart(store.clone(), host.clone()).await?;
    passed.push("non-deduplicating NoEffect retries safely after engine restart");
    retryable_cases::not_sent_after_no_effect_stays_pending(store.clone(), host.clone()).await?;
    passed.push("NotSent after NoEffect cannot use the unsent timeout");
    retryable_cases::crash_during_no_effect_retry_is_uncertain(store.clone(), host.clone()).await?;
    passed.push("interrupted NoEffect retry is Uncertain without another send");
    retry_bound_cases::retry_bounds(store.clone(), host.clone()).await?;
    passed.push(
        "domain retry bounds apply to Retry and Storage while default backoff stays unchanged",
    );
    retryable_cases::retryable_preserves_earlier_possible_effect(store, host).await?;
    passed.push("Retryable preserves earlier possible effect and original deduplication identity");
    Ok(passed)
}
