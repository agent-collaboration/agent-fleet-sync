use crate::{
    ServiceConsumer, ServiceError, ServiceHost, ServiceObject, service_state::ServiceState,
};
use durable_steps::{AtomicStore, Engine, StoreError, storage::TransactionScope};
use fleet_sync_protocol::{ConsumerProtocol, CopyStatusAnswer, CopyUnavailableReason, DatabaseId};
use fleet_sync_receiver::{ApplyFollowUp, RECEIVER_MIGRATOR, initialize_hook};
use fleet_sync_session::PeerAuthenticator;
use fleet_sync_tokio_runner::{ObjectKey, ObjectOptions};
use runtime_primitives::SystemClock;
use sqlx_core::migrate::Migrator;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

pub(crate) struct CopyOwner {
    #[cfg(feature = "test-fault")]
    pub call_probe: crate::copy_call_probe::CopyCallProbe,
    #[cfg(feature = "test-fault")]
    pub apply_wake: crate::copy_apply_wake::CopyApplyWake,
    pub object: Arc<ServiceObject>,
    pub engine: Arc<Engine<ServiceObject, ServiceHost>>,
    pub changed: watch::Sender<()>,
    pub health: Mutex<Option<CopyUnavailableReason>>,
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
    pub(crate) async fn copy(&self, database: DatabaseId) -> Result<Arc<CopyOwner>, ServiceError> {
        let slot = self
            .copies
            .lock()
            .await
            .entry(database)
            .or_default()
            .clone();
        if let Some(owner) = slot.get() {
            return Ok(owner.clone());
        }
        let owner = slot
            .get_or_try_init(|| async {
                let key = ObjectKey::new(format!(
                    "copy-{}-{}",
                    self.options.receiver.as_uuid(),
                    database.as_uuid()
                ))
                .map_err(ServiceError::rule)?;
                let host = Arc::new(ServiceHost::new(self.calls.clone(), SystemClock));
                let object = self
                    .objects
                    .object(
                        key,
                        ObjectOptions {
                            foreign_keys: false,
                        },
                        host,
                    )
                    .await?;
                let consumer = self.consumer.clone();
                object
                    .transaction(move |scope| {
                        Box::pin(async move {
                            let mut migrator = Migrator::with_migrations(
                                RECEIVER_MIGRATOR.iter().cloned().collect::<Vec<_>>(),
                            );
                            migrator.dangerous_set_table_name("_fleet_sync_copy_migrations");
                            migrator
                                .run_direct(None, scope.executor(), false)
                                .await
                                .map_err(StoreError::backend)?;
                            initialize_hook(scope, consumer.as_ref())
                                .await
                                .map_err(StoreError::backend)?;
                            Ok(())
                        })
                    })
                    .await?;
                let mut engine = Engine::new(object.clone(), object.host());
                engine.register(ApplyFollowUp::<ServiceObject, C::HookItem>::new())?;
                let (changed, _) = watch::channel(());
                let owner = Arc::new(CopyOwner {
                    #[cfg(feature = "test-fault")]
                    call_probe: Default::default(),
                    #[cfg(feature = "test-fault")]
                    apply_wake: crate::copy_apply_wake::CopyApplyWake::new(object.host()),
                    object,
                    engine: Arc::new(engine),
                    changed,
                    health: Mutex::new(None),
                });
                Ok::<_, ServiceError>(owner)
            })
            .await?
            .clone();
        self.copy_added.notify_one();
        Ok(owner)
    }

    pub(crate) async fn copy_status(
        &self,
        database: DatabaseId,
    ) -> Result<CopyStatusAnswer, ServiceError> {
        self.copy(database)
            .await?
            .object
            .transaction(|scope| {
                Box::pin(async move {
                    fleet_sync_receiver::copy_status(scope)
                        .await
                        .map_err(fleet_sync_receiver::receiver_rollback)
                })
            })
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn await_acked(
        &self,
        database: DatabaseId,
        through: fleet_sync_protocol::ReplicationPosition,
    ) -> Result<fleet_sync_protocol::AwaitAckedAnswer, ServiceError> {
        use fleet_sync_protocol::AwaitAckedAnswer;
        let copy = self.copy(database).await?;
        let mut changed = copy.changed.subscribe();
        loop {
            match self.copy_status(database).await? {
                CopyStatusAnswer::Status(status) if status.acked >= through => {
                    return Ok(AwaitAckedAnswer::Acked {
                        through: status.acked,
                    });
                }
                CopyStatusAnswer::Status(_) => {}
                CopyStatusAnswer::NotInitialized => return Ok(AwaitAckedAnswer::NotInitialized),
                CopyStatusAnswer::Retired => return Ok(AwaitAckedAnswer::Retired),
            }
            changed.changed().await.map_err(ServiceError::rule)?;
        }
    }
}
