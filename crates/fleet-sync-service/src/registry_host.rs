use crate::{
    ServiceConsumer, ServiceError, ServiceHost, ServiceObject, service_state::ServiceState,
};
use durable_steps::{
    AtomicStore, Engine, OperationState, RequestKey, SettleError, StoreError, Task,
    storage::TransactionScope,
};
use fleet_sync_lease::*;
use fleet_sync_protocol::*;
use fleet_sync_session::PeerAuthenticator;
use runtime_primitives::{Clock, WakeScheduler};
use sqlx_core::migrate::Migrator;
use sqlx_turso::Turso;
use std::sync::Arc;

pub(crate) async fn registry_engine(
    object: Arc<ServiceObject>,
    config: RegistryConfig,
) -> Result<Engine<ServiceObject, ServiceHost>, ServiceError> {
    object
        .transaction(|scope| {
            Box::pin(async move {
                let mut migrator =
                    Migrator::with_migrations(LEASE_MIGRATOR.iter().cloned().collect::<Vec<_>>());
                migrator.dangerous_set_table_name("_fleet_sync_registry_migrations");
                migrator
                    .run_direct(None, scope.executor(), false)
                    .await
                    .map_err(StoreError::backend)
            })
        })
        .await?;
    let mut engine = Engine::new(object.clone(), object.host());
    engine.register(RegistrationTask::<ServiceObject, Turso>::new(config))?;
    engine.register(ImportTask::<ServiceObject, Turso>::new(config))?;
    engine.register(RetirementTask::<ServiceObject, Turso>::new(config))?;
    engine.register(PreviewTakeoverTask::<ServiceObject, Turso>::new(config))?;
    let clock: Arc<dyn Clock> = object.host();
    engine.register(HolderChangeTask::<ServiceObject, Turso>::new(
        config,
        clock.clone(),
    ))?;
    engine.register(HolderChangeCancelTask::<ServiceObject, Turso>::new(
        config, clock,
    ))?;
    engine.register(SettleReservationTask::<ServiceObject>::new())?;
    Ok(engine)
}

impl<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
> ServiceState<P, C, A>
{
    pub(crate) async fn lifecycle(
        &self,
        database: DatabaseId,
    ) -> Result<Option<DatabaseLifecycle>, ServiceError> {
        self.registry
            .transaction(move |scope| {
                Box::pin(async move {
                    database_lifecycle(scope, database)
                        .await
                        .map_err(StoreError::backend)
                })
            })
            .await
            .map_err(Into::into)
    }
    pub(crate) async fn retired(&self, database: DatabaseId) -> Result<bool, ServiceError> {
        Ok(matches!(
            self.lifecycle(database).await?,
            Some(DatabaseLifecycle::Retired { .. } | DatabaseLifecycle::Retiring { .. })
        ))
    }
    pub(crate) async fn retirement_holder_answer(
        &self,
        database: DatabaseId,
    ) -> Result<Option<HolderChangeAnswer>, ServiceError> {
        Ok(match self.lifecycle(database).await? {
            Some(DatabaseLifecycle::Retired { .. }) => {
                Some(HolderChangeAnswer::Refused(HolderChangeRefusal::Retired))
            }
            Some(DatabaseLifecycle::Retiring { attempt }) => Some(HolderChangeAnswer::Refused(
                HolderChangeRefusal::TransferInProgress {
                    transfer: TransferId::new(attempt.as_uuid()).map_err(ServiceError::rule)?,
                },
            )),
            _ => None,
        })
    }
    pub(crate) async fn register(
        &self,
        request: Registration,
    ) -> Result<RegistrationAnswer, ServiceError> {
        if self.retired(request.database).await? {
            return Ok(RegistrationAnswer::Refused(RegistrationRefusal::Retired));
        }
        let key = lifecycle_request_key(request.attempt).map_err(ServiceError::rule)?;
        self.execute_registry::<RegistrationTask<ServiceObject, Turso>>(key, request)
            .await
    }
    pub(crate) async fn renew(
        &self,
        request: RenewRequest,
        peer: PeerId,
    ) -> Result<RenewAnswer, ServiceError> {
        // Read lifecycle at the host entry point even though renew also checks
        // it inside its own transaction. Cached durable results never outrank it.
        if self.retired(request.database).await? {
            return Ok(RenewAnswer::Refused(LeaseRefusal::Retired));
        }
        let now = self.registry.host().now();
        let policy = self.options.registry.lease;
        self.registry
            .transaction(move |scope| {
                Box::pin(async move {
                    fleet_sync_lease::renew(scope, &request, &peer, now, &policy)
                        .await
                        .map_err(StoreError::backend)
                })
            })
            .await
            .map_err(Into::into)
    }
    pub(crate) async fn execute_registry<T: Task<Store = ServiceObject>>(
        &self,
        key: RequestKey,
        input: T::Input,
    ) -> Result<T::Output, ServiceError> {
        let status = self
            .registry_engine
            .start_and_settle::<T>(&key, input)
            .await
            .map_err(|error| match error {
                SettleError::Admission(refusal) => ServiceError::rule(refusal),
                SettleError::Execution(error) => ServiceError::Storage(error),
            })?;
        match status.state {
            OperationState::Completed { result } => {
                serde_json::from_slice(&result).map_err(ServiceError::rule)
            }
            OperationState::Failed { reason } => Err(ServiceError::Operation { failure: reason }),
            OperationState::Running { .. } => Err(ServiceError::Pending {
                operation: status.id,
            }),
        }
    }

    pub(crate) async fn holder_change(
        &self,
        request: HolderChangeRequest,
        requested_by: ActorRef,
    ) -> Result<HolderChangeAnswer, ServiceError> {
        let database = match &request {
            HolderChangeRequest::Move { database, .. }
            | HolderChangeRequest::Takeover { database, .. }
            | HolderChangeRequest::Claim { database, .. } => *database,
        };
        if let Some(answer) = self.retirement_holder_answer(database).await? {
            return Ok(answer);
        }
        let now = self.registry.host().now();
        let admitted_request = request.clone();
        self.registry
            .transaction(move |scope| {
                Box::pin(async move {
                    admit_holder_change::<ServiceObject, Turso>(
                        scope,
                        &admitted_request,
                        &requested_by,
                        now,
                    )
                    .await
                    .map_err(StoreError::backend)?;
                    Ok(())
                })
            })
            .await?;
        // Admission's committed wake belongs to the service's joined registry
        // runner. The admin call must not wait behind an unanswered dependency.
        self.registry
            .host()
            .wake_at(now)
            .await
            .map_err(ServiceError::rule)?;
        // The domain outcome can advance after a compact Pending result, when
        // the withdrawal follow-up completes. Always read that canonical owner.
        self.registry
            .rule_transaction(move |scope| {
                Box::pin(async move {
                    let attempt = match &request {
                        HolderChangeRequest::Move { attempt, .. }
                        | HolderChangeRequest::Takeover { attempt, .. }
                        | HolderChangeRequest::Claim { attempt, .. } => *attempt,
                    };
                    Ok::<_, StoreError>(
                        holder_change_answer(scope, &request)
                            .await
                            .map_err(StoreError::backend)?
                            .unwrap_or(HolderChangeAnswer::Pending { transfer: attempt }),
                    )
                })
            })
            .await
            .map_err(Into::into)
    }
}
