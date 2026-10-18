use crate::{
    ServiceConsumer, ServiceError, ServiceHost, ServiceObject, service_state::ServiceState,
};
use durable_steps::{AtomicStore, Engine, StoreError, storage::TransactionScope};
use fleet_sync_delivery::{DELIVERY_MIGRATOR, DeliveryLimits, PublishStatus};
use fleet_sync_protocol::{ConsumerProtocol, DeliveryFailureKind, PeerId};
use fleet_sync_session::PeerAuthenticator;
use fleet_sync_tokio_runner::{ObjectKey, ObjectOptions};
use runtime_primitives::SystemClock;
use sqlx_core::migrate::Migrator;
use sqlx_turso::Turso;
use std::sync::Arc;
use tokio::sync::{OnceCell, watch};

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
pub(crate) struct DeliverySlot {
    pub owner: OnceCell<Arc<DeliveryOwner>>,
    #[cfg(feature = "test-support")]
    pub acquired: watch::Sender<()>,
}
impl Default for DeliverySlot {
    fn default() -> Self {
        Self {
            owner: OnceCell::new(),
            #[cfg(feature = "test-support")]
            acquired: watch::channel(()).0,
        }
    }
}

pub(crate) struct DeliveryOwner {
    pub object: Arc<ServiceObject>,
    pub engine: Arc<Engine<ServiceObject, ServiceHost>>,
    pub health: watch::Sender<Option<DeliveryFailureKind>>,
    schema: OnceCell<()>,
}
impl DeliveryOwner {
    pub fn mark_corrupt(&self, mailbox: PeerId, field: &'static str) {
        let changed = self.health.send_if_modified(|failure| {
            if failure.is_some() {
                return false;
            }
            *failure = Some(DeliveryFailureKind::CustodyCorrupt);
            true
        });
        if changed {
            tracing::warn!(?mailbox, field, "delivery mailbox unavailable");
        }
    }
    pub async fn ensure_schema(&self) -> Result<(), StoreError> {
        self.schema
            .get_or_try_init(|| async {
                self.object
                    .transaction(|scope| {
                        Box::pin(async move {
                            let mut migrations = Migrator::with_migrations(
                                DELIVERY_MIGRATOR.iter().cloned().collect::<Vec<_>>(),
                            );
                            migrations.dangerous_set_table_name("_fleet_sync_delivery_migrations");
                            migrations
                                .run_direct(None, scope.executor(), false)
                                .await
                                .map_err(StoreError::backend)
                        })
                    })
                    .await
            })
            .await
            .map(|_| ())
    }
}
impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>>
    ServiceState<P, C, A>
{
    pub(crate) fn delivery_limits(&self) -> Result<DeliveryLimits, ServiceError> {
        self.options
            .delivery
            .ok_or_else(|| ServiceError::rule("delivery is not installed"))
    }
    pub(crate) async fn delivery_slot(&self, key: DeliveryObject) -> Arc<DeliverySlot> {
        let slot = self.delivery.lock().await.entry(key).or_default().clone();
        // An empty slot still needs an owned retry runner if acquisition fails.
        self.delivery_added.notify_one();
        slot
    }
    pub(crate) async fn delivery_owner(
        &self,
        key: DeliveryObject,
    ) -> Result<Arc<DeliveryOwner>, ServiceError> {
        self.delivery_limits()?;
        let slot = self.delivery_slot(key).await;
        if let Some(owner) = slot.owner.get() {
            return Ok(owner.clone());
        }
        let owner = slot
            .owner
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
                let mut engine = Engine::new(object.clone(), object.host());
                engine.register(PublishStatus::<ServiceObject, Turso, P::Delivery>::new())?;
                let owner = Arc::new(DeliveryOwner {
                    object,
                    engine: Arc::new(engine),
                    health: watch::channel(None).0,
                    schema: OnceCell::new(),
                });
                if matches!(key, DeliveryObject::Status(_)) {
                    owner.ensure_schema().await?;
                }
                Ok::<_, ServiceError>(owner)
            })
            .await?
            .clone();
        #[cfg(feature = "test-support")]
        slot.acquired.send_replace(());
        self.delivery_added.notify_one();
        Ok(owner)
    }
}
