use crate::{
    ServiceConsumer, ServiceError, ServiceHost, ServiceObject, service_state::ServiceState,
};
use durable_steps::{AtomicStore, Engine, StoreError, storage::TransactionScope};
use fleet_sync_delivery::{DeliveryLimits, PublishStatus};
use fleet_sync_protocol::{ConsumerProtocol, PeerId};
use fleet_sync_session::PeerAuthenticator;
use fleet_sync_tokio_runner::{ObjectKey, ObjectOptions};
use runtime_primitives::SystemClock;
use sqlx_core::migrate::Migrator;
use sqlx_turso::Turso;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum DeliveryObject {
    Mailbox(PeerId),
    Status(PeerId),
}
impl DeliveryObject {
    fn file_key(self) -> String {
        match self {
            Self::Mailbox(peer) => format!("delivery-mailbox-{}", peer.as_uuid()),
            Self::Status(peer) => format!("delivery-status-{}", peer.as_uuid()),
        }
    }
    pub fn from_file(name: &str) -> Result<Option<Self>, ServiceError> {
        let Some(name) = name.strip_suffix(".db") else {
            return Ok(None);
        };
        let (mailbox, id) = if let Some(id) = name.strip_prefix("delivery-mailbox-") {
            (true, id)
        } else if let Some(id) = name.strip_prefix("delivery-status-") {
            (false, id)
        } else {
            return Ok(None);
        };
        let peer =
            PeerId::new(id.parse().map_err(ServiceError::rule)?).map_err(ServiceError::rule)?;
        Ok(Some(if mailbox {
            Self::Mailbox(peer)
        } else {
            Self::Status(peer)
        }))
    }
}
pub(crate) struct DeliveryOwner {
    pub object: Arc<ServiceObject>,
    pub engine: Arc<Engine<ServiceObject, ServiceHost>>,
}
static MIGRATIONS: Migrator = sqlx::migrate!("../fleet-sync-delivery/migrations");
impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>>
    ServiceState<P, C, A>
{
    pub(crate) fn delivery_limits(&self) -> Result<DeliveryLimits, ServiceError> {
        self.options
            .delivery
            .ok_or_else(|| ServiceError::rule("delivery is not installed"))
    }
    pub(crate) async fn delivery_owner(
        &self,
        key: DeliveryObject,
    ) -> Result<Arc<DeliveryOwner>, ServiceError> {
        self.delivery_limits()?;
        let slot = self.delivery.lock().await.entry(key).or_default().clone();
        if let Some(owner) = slot.get() {
            return Ok(owner.clone());
        }
        let owner = slot
            .get_or_try_init(|| async {
                let host = Arc::new(ServiceHost::new(self.calls.clone(), SystemClock));
                let object = self
                    .objects
                    .object(
                        ObjectKey::new(key.file_key()).map_err(ServiceError::rule)?,
                        ObjectOptions::default(),
                        host,
                    )
                    .await?;
                object
                    .transaction(|scope| {
                        Box::pin(async move {
                            let mut migrations = Migrator::with_migrations(
                                MIGRATIONS.iter().cloned().collect::<Vec<_>>(),
                            );
                            migrations.dangerous_set_table_name("_fleet_sync_delivery_migrations");
                            migrations
                                .run_direct(None, scope.executor(), false)
                                .await
                                .map_err(StoreError::backend)
                        })
                    })
                    .await?;
                let mut engine = Engine::new(object.clone(), object.host());
                engine.register(PublishStatus::<ServiceObject, Turso, P::Delivery>::new())?;
                Ok::<_, ServiceError>(Arc::new(DeliveryOwner {
                    object,
                    engine: Arc::new(engine),
                }))
            })
            .await?
            .clone();
        self.delivery_added.notify_one();
        Ok(owner)
    }
}
