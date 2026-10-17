use crate::{ServiceConsumer, ServiceError, ServiceHandle, ServiceObject};
use durable_steps::RequestKey;
use fleet_sync_lease::{
    DatabaseLifecycle, HolderChangeTask, ImportTask, PreviewTakeoverTask, RetirementTask,
    holder_change_request_key, lifecycle_request_key,
};
use fleet_sync_protocol::*;
use fleet_sync_session::PeerAuthenticator;
use sqlx_turso::Turso;

/// Trusted consumer entry points. The peer WebSocket exposes none of these
/// admin operations; the consumer authenticates its own admin surface.
impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>>
    ServiceHandle<P, C, A>
{
    pub async fn copy_status(
        &self,
        database: DatabaseId,
    ) -> Result<CopyStatusAnswer, ServiceError> {
        self.state.copy_status(database).await
    }
    pub async fn lifecycle(
        &self,
        database: DatabaseId,
    ) -> Result<Option<DatabaseLifecycle>, ServiceError> {
        self.state.lifecycle(database).await
    }
    pub async fn register(
        &self,
        request: Registration,
    ) -> Result<RegistrationAnswer, ServiceError> {
        self.state.register(request).await
    }
    pub async fn renew(
        &self,
        request: RenewRequest,
        holder: PeerId,
    ) -> Result<RenewAnswer, ServiceError> {
        self.state.renew(request, holder).await
    }
    pub async fn import(&self, request: Import) -> Result<ImportAnswer, ServiceError> {
        if self.state.retired(request.database).await? {
            return Ok(ImportAnswer::Retired);
        }
        let key = lifecycle_request_key(request.attempt).map_err(ServiceError::rule)?;
        self.state
            .execute_registry::<ImportTask<ServiceObject, Turso>>(key, request)
            .await
    }
    pub async fn retire(&self, request: Retirement) -> Result<RetirementAnswer, ServiceError> {
        // A Retiring database must resume its deletion operation. Only the
        // terminal marker can answer this admin request without a drive.
        if let Some(DatabaseLifecycle::Retired { at }) =
            self.state.lifecycle(request.database).await?
        {
            return Ok(RetirementAnswer::Retired { at });
        }
        let key = lifecycle_request_key(request.attempt).map_err(ServiceError::rule)?;
        self.state
            .execute_registry::<RetirementTask<ServiceObject, Turso>>(key, request)
            .await
    }
    pub async fn preview_takeover(
        &self,
        key: RequestKey,
        database: DatabaseId,
    ) -> Result<TakeoverPreview, ServiceError> {
        if self.state.retired(database).await? {
            return Err(ServiceError::rule("database retired"));
        }
        self.state
            .execute_registry::<PreviewTakeoverTask<ServiceObject, Turso>>(
                key,
                PreviewTakeover { database },
            )
            .await
    }
    pub async fn holder_change(
        &self,
        request: HolderChangeRequest,
    ) -> Result<HolderChangeAnswer, ServiceError> {
        let (database, attempt) = match &request {
            HolderChangeRequest::Move {
                database, attempt, ..
            }
            | HolderChangeRequest::Takeover {
                database, attempt, ..
            }
            | HolderChangeRequest::Claim {
                database, attempt, ..
            } => (*database, *attempt),
        };
        if let Some(answer) = self.state.retirement_holder_answer(database).await? {
            return Ok(answer);
        }
        let key = holder_change_request_key(attempt).map_err(ServiceError::rule)?;
        match self
            .state
            .execute_registry::<HolderChangeTask<ServiceObject, Turso>>(key, request)
            .await
        {
            Ok(answer) => Ok(answer),
            Err(ServiceError::Pending { .. }) => {
                Ok(HolderChangeAnswer::Pending { transfer: attempt })
            }
            Err(error) => Err(error),
        }
    }
    /// Call after the authenticator's owner has committed the credential change.
    pub async fn revoke(&self, peer: PeerId) -> Result<(), ServiceError> {
        self.sessions.revoke(peer).await.map_err(ServiceError::rule)
    }
}
