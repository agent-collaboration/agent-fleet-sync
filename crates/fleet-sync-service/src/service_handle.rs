use crate::{ServiceConsumer, ServiceError, ServiceHandle, ServiceObject};
use durable_steps::{AtomicStore, RequestKey};
use fleet_sync_lease::{
    DatabaseLifecycle, ImportTask, PreviewTakeoverTask, RetirementTask, lifecycle_request_key,
};
use fleet_sync_protocol::*;
use fleet_sync_session::PeerAuthenticator;
use runtime_primitives::{Clock, WakeScheduler};
use sqlx_turso::Turso;

/// Seed data supplied through the consumer's authenticated import surface.
/// Peer sessions always use their writer identity instead.
pub enum ImportSeedRequest {
    Begin {
        database: DatabaseId,
        attempt: durable_steps::Uuid,
    },
    Page(SeedPage),
    Complete(SeedComplete),
}

/// Trusted consumer entry points. The peer WebSocket exposes none of these
/// admin operations; the consumer authenticates its own admin surface.
impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>>
    ServiceHandle<P, C, A>
{
    pub async fn database_statuses(
        &self,
        peer: PeerId,
    ) -> Result<Vec<DatabaseStatus>, ServiceError> {
        self.state.gather(peer).await
    }
    pub async fn copy_status(
        &self,
        database: DatabaseId,
    ) -> Result<CopyStatusAnswer, ServiceError> {
        self.state.copy_status(database).await
    }
    #[cfg(feature = "test-support")]
    pub async fn wait_opening(&self, peer: PeerId) -> Result<ConnectionId, ServiceError> {
        self.sessions
            .wait_opening(peer)
            .await
            .map_err(ServiceError::rule)
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
    pub async fn seed_import(
        &self,
        request: ImportSeedRequest,
    ) -> Result<ReplicationAnswer, ServiceError> {
        let request = match request {
            ImportSeedRequest::Begin { database, attempt } => {
                ReplicationRequest::SeedBegin { database, attempt }
            }
            ImportSeedRequest::Page(page) => ReplicationRequest::SeedPage(Box::new(page)),
            ImportSeedRequest::Complete(done) => ReplicationRequest::SeedComplete(done),
        };
        self.state
            .replicate_with_authority(fleet_sync_receiver::SeedAuthority::Admin, request)
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
        requested_by: ActorRef,
    ) -> Result<HolderChangeAnswer, ServiceError> {
        self.state.holder_change(request, requested_by).await
    }
    pub async fn cancel_holder_change(
        &self,
        cancel: fleet_sync_lease::CancelHolderChange,
    ) -> Result<fleet_sync_lease::CancelAnswer, ServiceError> {
        let now = self.state.registry.host().now();
        let answer = self
            .state
            .registry
            .transaction(move |scope| {
                Box::pin(async move {
                    fleet_sync_lease::cancel_holder_change(scope, &cancel, now)
                        .await
                        .map_err(durable_steps::StoreError::backend)
                })
            })
            .await?;
        if matches!(answer, fleet_sync_lease::CancelAnswer::Accepted { .. }) {
            self.state
                .registry
                .host()
                .wake_at(now)
                .await
                .map_err(ServiceError::rule)?;
        }
        Ok(answer)
    }
    pub async fn holder_change_view(
        &self,
        attempt: TransferId,
    ) -> Result<Option<fleet_sync_lease::HolderChangeView>, ServiceError> {
        self.state
            .registry
            .rule_transaction(move |scope| {
                Box::pin(async move {
                    fleet_sync_lease::holder_change_view(scope, attempt)
                        .await
                        .map_err(durable_steps::StoreError::backend)
                })
            })
            .await
            .map_err(Into::into)
    }
    #[cfg(feature = "test-support")]
    pub async fn replay_holder_release(
        &self,
        holder: PeerId,
        request: ReleaseRequest,
        call: durable_steps::CallId,
    ) -> Result<ReleaseAnswer, ServiceError> {
        let answer = self
            .sessions
            .request_peer(
                holder,
                ServerRequest::Crate(ServerCall::Release { call, request }),
                self.state.options.call_timeout,
            )
            .await
            .map_err(ServiceError::rule)?;
        match answer {
            ClientAnswer::Crate(ServerCallAnswer::Release(answer)) => Ok(answer),
            _ => Err(ServiceError::rule("holder returned another answer kind")),
        }
    }
    #[cfg(feature = "test-support")]
    pub async fn wait_holder_change_phase(
        &self,
        attempt: TransferId,
        expected: fleet_sync_lease::HolderChangePhase,
    ) -> Result<fleet_sync_lease::HolderChangeView, ServiceError> {
        let mut changed = self.state.registry.subscribe_commits();
        loop {
            if let Some(view) = self.holder_change_view(attempt).await?
                && (view.phase == expected || view.outcome.is_some())
            {
                return Ok(view);
            }
            changed.changed().await.map_err(ServiceError::rule)?;
        }
    }
    #[cfg(feature = "test-support")]
    pub async fn wait_holder_change_settled(
        &self,
        attempt: TransferId,
    ) -> Result<fleet_sync_lease::HolderChangeView, ServiceError> {
        let mut changed = self.state.registry.subscribe_commits();
        loop {
            if let Some(view) = self.holder_change_view(attempt).await?
                && view.outcome.is_some()
                && !view.settlement_owed
            {
                return Ok(view);
            }
            changed.changed().await.map_err(ServiceError::rule)?;
        }
    }
    /// Observe an admitted change through its domain record and committed wakes.
    #[cfg(feature = "test-support")]
    pub async fn wait_holder_change(
        &self,
        request: HolderChangeRequest,
    ) -> Result<HolderChangeAnswer, ServiceError> {
        let mut changed = self.state.registry.subscribe_commits();
        loop {
            let reading = request.clone();
            let answer = self
                .state
                .registry
                .rule_transaction(move |scope| {
                    Box::pin(async move {
                        fleet_sync_lease::holder_change_answer(scope, &reading)
                            .await
                            .map_err(durable_steps::StoreError::backend)
                    })
                })
                .await?;
            if let Some(answer) = answer
                && !matches!(answer, HolderChangeAnswer::Pending { .. })
            {
                return Ok(answer);
            }
            changed.changed().await.map_err(ServiceError::rule)?;
        }
    }
    /// Call after the authenticator's owner has committed the credential change.
    pub async fn revoke(&self, peer: PeerId) -> Result<(), ServiceError> {
        self.sessions.revoke(peer).await.map_err(ServiceError::rule)
    }
}
