#[path = "../conformance/reconciliation_cases.rs"]
mod cases;
use super::support::TestHost;
use durable_steps::{StoreError, volatile_store::VolatileStore};
use std::sync::Arc;

#[test]
fn recovered_answer_is_saved_once_without_dispatch() -> Result<(), StoreError> {
    let host = TestHost::new();
    futures_executor::block_on(cases::recovered_answer(
        Arc::new(VolatileStore::new(host.clone())),
        host,
    ))
}
#[test]
fn missing_reconciliation_answer_stays_uncertain() -> Result<(), StoreError> {
    let host = TestHost::new();
    futures_executor::block_on(cases::missing_answer(
        Arc::new(VolatileStore::new(host.clone())),
        host,
    ))
}
#[test]
fn reconciliation_errors_never_redispatch_a_possible_effect() -> Result<(), StoreError> {
    let host = TestHost::new();
    futures_executor::block_on(cases::failures_preserve_possible_reach(
        Arc::new(VolatileStore::new(host.clone())),
        host,
    ))
}
#[test]
fn default_target_still_fails_uncertain_without_reconciliation() -> Result<(), StoreError> {
    let host = TestHost::new();
    futures_executor::block_on(cases::default_target(
        Arc::new(VolatileStore::new(host.clone())),
        host,
    ))
}
#[test]
fn deduplicating_target_uses_normal_dispatch_even_if_opted_in() -> Result<(), StoreError> {
    let host = TestHost::new();
    futures_executor::block_on(cases::deduplicating_target(
        Arc::new(VolatileStore::new(host.clone())),
        host,
    ))
}
#[test]
fn fresh_calls_and_safe_retries_never_reconcile() -> Result<(), StoreError> {
    let host = TestHost::new();
    futures_executor::block_on(cases::fresh_and_safe_retries(
        Arc::new(VolatileStore::new(host.clone())),
        host,
    ))
}
