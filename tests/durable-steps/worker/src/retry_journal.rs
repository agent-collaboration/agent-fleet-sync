//! Inspect the actual private SQL representation, including pre-NoEffect rows.
use crate::support::TestHost;
use durable_steps::{
    AtomicStore, OperationId, StoreError,
    durable_object_store::DurableObjectStore,
    storage::{CallReach, JournalStorage, StepState, TransactionScope},
};
use sqlx_core::{query::query, row::Row};
use sqlx_durable_object::Do;
use std::sync::Arc;

pub async fn check_stored_reach(
    store: Arc<DurableObjectStore<TestHost>>,
    operation: OperationId,
) -> Result<(), StoreError> {
    store.transaction(move |scope| Box::pin(async move {
        let row = query::<Do>("SELECT intent,may_have_reached FROM _fleet_sync_steps WHERE operation_id=? AND position=0")
            .bind(operation.as_uuid().to_string()).fetch_one(scope.executor()).await?;
        let original: String = row.try_get("intent")?;
        crate::conformance::check(row.try_get::<i64,_>("may_have_reached")? == 1, "NoEffect uses the existing boolean projection 1")?;
        let value: serde_json::Value = serde_json::from_str(&original).map_err(StoreError::backend)?;
        for (reach, expected, may) in [("never_sent", CallReach::NeverSent, 0_i64), ("dispatching", CallReach::Dispatching, 1), ("may_have_reached", CallReach::MayHaveReached, 1)] {
            let mut old = value.clone();
            let object = old.as_object_mut().ok_or_else(|| StoreError::backend("intent is not an object"))?;
            object.insert("reach".into(), reach.into());
            object.insert("last_seen".into(), serde_json::json!(123));
            object.remove("retry_reason");
            query::<Do>("UPDATE _fleet_sync_steps SET intent=?,may_have_reached=? WHERE operation_id=? AND position=0")
                .bind(old.to_string()).bind(may).bind(operation.as_uuid().to_string()).execute(scope.executor()).await?;
            let Some(step) = scope.step(operation, 0).await? else { return Err(StoreError::backend("old step missing")); };
            let StepState::CallPending { attempt, .. } = step.state else { return Err(StoreError::backend("old pending row changed kind")); };
            crate::conformance::check(attempt.reach == expected && attempt.retry_reason.is_none() && attempt.last_seen == Some(runtime_primitives::SyncTime::from_unix_millis(123)), "legacy reach and last_seen must retain their meanings")?;
        }
        query::<Do>("UPDATE _fleet_sync_steps SET intent=?,may_have_reached=1 WHERE operation_id=? AND position=0")
            .bind(original).bind(operation.as_uuid().to_string()).execute(scope.executor()).await?;
        Ok(())
    })).await
}
