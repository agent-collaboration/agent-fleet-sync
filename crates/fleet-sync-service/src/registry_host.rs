use crate::{
    ServiceConsumer, ServiceError, ServiceHost, ServiceObject, service_state::ServiceState,
};
use durable_steps::{
    AtomicStore, Engine, OperationState, RequestKey, StoreError, Task, storage::TransactionScope,
};
use fleet_sync_lease::*;
use fleet_sync_protocol::*;
use fleet_sync_session::PeerAuthenticator;
use runtime_primitives::Clock;
use sqlx_core::migrate::Migrator;
use sqlx_turso::Turso;
use std::sync::Arc;

static MIGRATIONS: Migrator = sqlx::migrate!("../fleet-sync-lease/migrations");
pub(crate) async fn registry_engine(
    object: Arc<ServiceObject>,
    config: RegistryConfig,
) -> Result<Engine<ServiceObject, ServiceHost>, ServiceError> {
    object
        .transaction(|scope| {
            Box::pin(async move {
                let mut migrator =
                    Migrator::with_migrations(MIGRATIONS.iter().cloned().collect::<Vec<_>>());
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
    engine.register(HolderChangeTask::<ServiceObject, Turso>::new(config))?;
    engine.register(SettleReservationTask::<ServiceObject>::new())?;
    Ok(engine)
}

impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>>
    ServiceState<P, C, A>
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
        let operation = self
            .registry_engine
            .start::<T>(&key, input)
            .await
            .map_err(ServiceError::rule)?;
        {
            let lock = self.registry.engine_lock();
            let _work = lock.lock().await;
            self.registry_engine.drive_due().await?;
        }
        match self.registry_engine.status(&operation).await?.state {
            OperationState::Completed { result } => {
                serde_json::from_slice(&result).map_err(ServiceError::rule)
            }
            OperationState::Failed { reason } => Err(ServiceError::Operation { failure: reason }),
            OperationState::Running { .. } => Err(ServiceError::Pending { operation }),
        }
    }
}
