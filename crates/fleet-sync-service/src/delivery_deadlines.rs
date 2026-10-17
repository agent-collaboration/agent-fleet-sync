use crate::{ServiceError, ServiceObject, delivery_owner::DeliveryOwner};
use fleet_sync_delivery::{DeliveryLimits, Mailbox, PublishStatus, admit_publications};
use fleet_sync_protocol::{DeliveryTypes, PeerId};
use runtime_primitives::{Clock, WakeScheduler};
use sqlx_turso::Turso;
use std::sync::Arc;
use tokio::sync::watch;

pub(crate) async fn run_mailbox<D: DeliveryTypes>(
    owner: Arc<DeliveryOwner>,
    mailbox: PeerId,
    limits: DeliveryLimits,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServiceError> {
    let host = owner.object.host();
    let engine_lock = owner.object.engine_lock();
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        {
            let _work = engine_lock.lock().await;
            let now = host.now();
            let next = owner
                .object
                .rule_transaction(move |scope| {
                    Box::pin(async move {
                        let next = Mailbox::<_, D>::new(scope, mailbox, limits)
                            .expire_due(now)
                            .await?;
                        admit_publications::<_, D, PublishStatus<ServiceObject, Turso, D>>(scope)
                            .await?;
                        Ok::<_, fleet_sync_delivery::DeliveryError>(next)
                    })
                })
                .await
                .map_err(ServiceError::rule)?;
            let report = owner.engine.drive_due().await?;
            for (operation, failure) in report.failed {
                tracing::warn!(operation=%operation.0,?failure,"durable delivery operation failed");
            }
            if let Some(error) = report.wake_failure {
                return Err(ServiceError::rule(error));
            }
            if let Some(next) = next {
                // The timer keeps the earlier of this durable custody deadline
                // and the engine's own next retry. Startup runs this same scan.
                host.wake_at(next).await.map_err(ServiceError::rule)?;
            }
        }
        tokio::select! {
            _ = host.wait() => {},
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return Ok(()); }
            }
        }
    }
}
