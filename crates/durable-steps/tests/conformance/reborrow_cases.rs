//! Transaction laws shared by the real Turso and Durable Object adapters.
use super::{BusinessScope, check, key, task_fixtures::Child};
use durable_steps::{
    JournalScope, ReborrowScope, StoreError, SyncTime,
    storage::{JournalStorage, OperationExecution},
};
use std::sync::Arc;

#[allow(dead_code)] // Volatile-only builds intentionally lack this optional capability.
pub async fn commit_and_rollback<S>(store: Arc<S>, now: SyncTime) -> Result<(), StoreError>
where
    S: ReborrowScope,
    for<'t> S::Scope<'t>: BusinessScope,
{
    let committed = store
        .transaction(move |parent| {
            Box::pin(async move {
                parent.write_counter("reborrow", 3).await?;
                let original = parent
                    .admit::<Child<S>>(&key("reborrow-parent"), &7)
                    .await
                    .map_err(StoreError::backend)?
                    .id();
                let admitted = {
                    let mut view = S::reborrow(parent);
                    check(
                        view.read_counter("reborrow").await? == Some(3),
                        "view sees the parent's uncommitted SQL",
                    )?;
                    let repeated = view
                        .admit::<Child<S>>(&key("reborrow-parent"), &7)
                        .await
                        .map_err(StoreError::backend)?
                        .id();
                    check(
                        repeated == original,
                        "view retains the parent's journal identity",
                    )?;
                    view.write_counter("reborrow", 4).await?;
                    view.admit::<Child<S>>(&key("reborrow-view"), &9)
                        .await
                        .map_err(StoreError::backend)?
                        .id()
                };
                check(
                    parent.read_counter("reborrow").await? == Some(4)
                        && parent.operation(admitted).await?.is_some(),
                    "parent sees view SQL and journal writes before commit",
                )?;
                Ok([original, admitted])
            })
        })
        .await?;

    let aborted = store
        .transaction(|parent| {
            Box::pin(async move {
                let mut view = S::reborrow(parent);
                view.write_counter("reborrow-rollback", 5).await?;
                view.admit::<Child<S>>(&key("reborrow-rollback"), &11)
                    .await
                    .map_err(StoreError::backend)?;
                Err::<(), _>(StoreError::Aborted {
                    reason: "reborrow requested rollback".into(),
                })
            })
        })
        .await;
    check(
        matches!(aborted, Err(StoreError::Aborted { reason }) if reason == "reborrow requested rollback"),
        "parent returns the view callback's deliberate rollback",
    )?;

    store
        .transaction(move |parent| {
            Box::pin(async move {
                check(
                    parent.read_counter("reborrow").await? == Some(4)
                        && parent.read_counter("reborrow-rollback").await?.is_none(),
                    "view SQL shares the parent's commit and rollback",
                )?;
                let due = parent.due(now).await?;
                check(
                    due.len() == 2 && committed.iter().all(|id| due.contains(id)),
                    "only the committed parent and view admissions survive",
                )?;
                for id in committed {
                    let record = parent.operation(id).await?
                        .ok_or(StoreError::MissingOperation { id })?;
                    check(
                        matches!(record.execution, OperationExecution::Running { next_attempt_at, .. } if next_attempt_at == now),
                        "view admission uses the same host and journal deadline",
                    )?;
                }
                Ok(())
            })
        })
        .await
}
