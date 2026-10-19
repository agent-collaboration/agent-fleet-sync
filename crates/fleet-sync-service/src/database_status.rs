use crate::{
    ServiceConsumer, ServiceError, call_router::ServiceObject, service_state::ServiceState,
};
use durable_steps::{AtomicStore, StoreError};
use fleet_sync_protocol::*;
use fleet_sync_receiver::SnapshotInstallRules;
use fleet_sync_session::PeerAuthenticator;

impl<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
> ServiceState<P, C, A>
{
    pub(crate) async fn database_status(
        &self,
        database: DatabaseId,
        lease: LeaseStatus,
    ) -> Result<Option<DatabaseStatus>, ServiceError> {
        let copy = self.copy(database).await?;
        let status = self.copy_status(database).await;
        if matches!(status, Ok(CopyStatusAnswer::Retired)) {
            return Ok(None);
        }
        if self.installs.needs_snapshot(copy.object.key()).await? {
            let acked = match &status {
                Ok(CopyStatusAnswer::Status(status)) => Some(status.acked),
                Ok(CopyStatusAnswer::NotInitialized) => published_position(&copy.object).await?,
                _ => None,
            };
            if let Some(acked) = acked {
                return Ok(Some(DatabaseStatus::NeedsSnapshot { database, acked }));
            }
        }
        if let Some(reason) = copy.health.lock().await.clone() {
            return Ok(Some(DatabaseStatus::Unavailable {
                database,
                lease: Some(lease),
                reason,
            }));
        }
        Ok(Some(match status {
            Ok(CopyStatusAnswer::Status(status)) => DatabaseStatus::Ready {
                database,
                acked: status.acked,
                lease,
            },
            Ok(CopyStatusAnswer::NotInitialized | CopyStatusAnswer::Retired) => {
                DatabaseStatus::Unavailable {
                    database,
                    lease: Some(lease),
                    reason: CopyUnavailableReason::Uninitialized,
                }
            }
            Err(error) => DatabaseStatus::Unavailable {
                database,
                lease: Some(lease),
                reason: CopyUnavailableReason::Damaged {
                    detail: error.to_string(),
                },
            },
        }))
    }
}
async fn published_position(
    copy: &ServiceObject,
) -> Result<Option<ReplicationPosition>, StoreError> {
    copy.transaction(|scope| {
        Box::pin(async move {
            Ok(SnapshotInstallRules(scope)
                .state()
                .await
                .map_err(StoreError::backend)?
                .filter(|marker| marker.verified)
                .map(|marker| marker.install.position))
        })
    })
    .await
}
