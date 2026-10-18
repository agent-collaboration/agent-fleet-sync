//! Test-only controls on the actual copy owner; no substituted transaction or host.
use crate::{ServiceConsumer, ServiceError, ServiceHandle};
use durable_steps::{AtomicStore, storage::TransactionScope};
use fleet_sync_protocol::{ConsumerProtocol, DatabaseId};
use fleet_sync_session::PeerAuthenticator;
use runtime_primitives::{Clock, WakeScheduler};
use sqlx_core::{query::query, row::Row};
use sqlx_turso::Turso;

impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>>
    ServiceHandle<P, C, A>
{
    pub async fn hold_copy_call_for_test(
        &self,
        database: DatabaseId,
        kind: crate::CopyCallKind,
    ) -> Result<(), ServiceError> {
        self.state.copy(database).await?.call_probe.arm(kind).await
    }
    pub async fn wait_copy_call_for_test(
        &self,
        database: DatabaseId,
    ) -> Result<durable_steps::CallId, ServiceError> {
        self.state.copy(database).await?.call_probe.wait().await
    }
    pub async fn release_copy_call_for_test(
        &self,
        database: DatabaseId,
    ) -> Result<(), ServiceError> {
        self.state.copy(database).await?.call_probe.release().await;
        Ok(())
    }
    pub async fn wait_copy_waiter_for_test(
        &self,
        database: DatabaseId,
    ) -> Result<durable_steps::storage::CallReach, ServiceError> {
        self.state
            .copy(database)
            .await?
            .call_probe
            .wait_for_waiter()
            .await
    }
    pub async fn hold_copy_drive_for_test(
        &self,
        database: DatabaseId,
    ) -> Result<tokio::sync::OwnedMutexGuard<()>, ServiceError> {
        Ok(self
            .state
            .copy(database)
            .await?
            .object
            .engine_lock()
            .lock_owned()
            .await)
    }
    pub async fn replace_copy_marker_for_test(
        &self,
        database: DatabaseId,
        replacement: String,
    ) -> Result<String, ServiceError> {
        let copy = self.state.copy(database).await?;
        let previous = copy
            .object
            .transaction(move |scope| {
                Box::pin(async move {
                    let previous: String =
                        query::<Turso>("SELECT record FROM _fleet_sync_copy WHERE singleton=1")
                            .fetch_one(scope.executor())
                            .await?
                            .try_get("record")?;
                    query::<Turso>("UPDATE _fleet_sync_copy SET record=? WHERE singleton=1")
                        .bind(replacement)
                        .execute(scope.executor())
                        .await?;
                    Ok(previous)
                })
            })
            .await?;
        copy.changed.send_replace(());
        Ok(previous)
    }
    pub async fn wait_holder_change_copy_for_test(
        &self,
        attempt: fleet_sync_protocol::TransferId,
    ) -> Result<fleet_sync_lease::HolderChangeView, ServiceError> {
        let mut changed = self.state.registry.subscribe_commits();
        loop {
            if let Some(view) = self.holder_change_view(attempt).await? {
                if let Some(waiting) = &view.waiting {
                    let database = match &view.request {
                        fleet_sync_protocol::HolderChangeRequest::Move { database, .. }
                        | fleet_sync_protocol::HolderChangeRequest::Takeover { database, .. }
                        | fleet_sync_protocol::HolderChangeRequest::Claim { database, .. } => {
                            *database
                        }
                    };
                    let copy = self
                        .state
                        .copies
                        .lock()
                        .await
                        .get(&database)
                        .and_then(|slot| slot.get())
                        .cloned();
                    if let Some(copy) = copy {
                        copy.call_probe.observe_waiter(waiting.reach).await;
                    }
                }
                if view.waiting.as_ref().is_some_and(|waiting| {
                    waiting.copy_unavailable.is_some()
                        && waiting.reach == durable_steps::storage::CallReach::NoEffect
                }) || view.outcome.is_some()
                {
                    return Ok(view);
                }
            }
            changed.changed().await.map_err(ServiceError::rule)?;
        }
    }
    pub async fn lose_copy_transaction_reply_for_test(
        &self,
        database: DatabaseId,
        fault: fleet_sync_tokio_runner::TransactionReplyFault,
    ) -> Result<(), ServiceError> {
        self.state
            .copy(database)
            .await?
            .object
            .lose_transaction_reply_for_test(fault);
        Ok(())
    }
    pub async fn fail_next_copy_wake_for_test(
        &self,
        database: DatabaseId,
    ) -> Result<(), ServiceError> {
        self.state.copy(database).await?.apply_wake.arm();
        Ok(())
    }
    pub async fn copy_wake_failures_for_test(
        &self,
        database: DatabaseId,
    ) -> Result<u64, ServiceError> {
        Ok(self.state.copy(database).await?.apply_wake.failures())
    }
    pub async fn wake_copy_for_test(&self, database: DatabaseId) -> Result<(), ServiceError> {
        let host = self.state.copy(database).await?.object.host();
        host.wake_at(host.now()).await.map_err(ServiceError::rule)
    }
}
