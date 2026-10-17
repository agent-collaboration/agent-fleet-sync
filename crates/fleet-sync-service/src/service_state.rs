use crate::{
    ServiceConsumer, ServiceError, ServiceHost, ServiceObject, ServiceOptions,
    call_router::ServiceCalls, copy_owner::CopyOwner, session_handler::ServiceSessionHandler,
};
use durable_steps::{BoxFuture, Engine};
use fleet_sync_protocol::{ConsumerProtocol, DatabaseId, ReceiverId};
use fleet_sync_session::PeerAuthenticator;
use fleet_sync_tokio_runner::{InstallCoordinator, ObjectStore, SessionHost, SessionRequests};
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock, Weak},
};
use tokio::sync::{Mutex, Notify, OnceCell, watch};

pub(crate) struct SourceRunner {
    pub run: BoxFuture<'static, Result<(), fleet_sync_session::ClientError>>,
    pub stop: watch::Sender<bool>,
}

pub(crate) type ServiceSessions<P, C, A> =
    SessionHost<P, A, ServiceSessionHandler<P, C, A>, ServiceHost>;
pub(crate) struct ServiceState<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<Material = String>,
> {
    pub options: ServiceOptions,
    pub consumer: Arc<C>,
    pub calls: Arc<ServiceCalls>,
    pub objects: Arc<ObjectStore<ServiceHost>>,
    pub registry: Arc<ServiceObject>,
    pub registry_engine: Arc<Engine<ServiceObject, ServiceHost>>,
    pub control: Arc<ServiceObject>,
    pub control_engine: Arc<Engine<ServiceObject, ServiceHost>>,
    pub installs: Arc<InstallCoordinator<ServiceHost>>,
    pub sources: Mutex<BTreeMap<ReceiverId, SessionRequests<P>>>,
    pub source_runners: Mutex<Vec<SourceRunner>>,
    pub source_added: Notify,
    pub shutdown: watch::Sender<bool>,
    #[cfg(feature = "test-support")]
    pub install_page_probe: crate::install_page_probe::InstallPageProbe,
    pub copies: Mutex<BTreeMap<DatabaseId, Arc<OnceCell<Arc<CopyOwner>>>>>,
    pub copy_added: Notify,
    pub delivery: Mutex<
        BTreeMap<
            crate::delivery_owner::DeliveryObject,
            Arc<OnceCell<Arc<crate::delivery_owner::DeliveryOwner>>>,
        >,
    >,
    pub delivery_added: Notify,
    pub sessions: OnceLock<Weak<ServiceSessions<P, C, A>>>,
}
impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>>
    ServiceState<P, C, A>
{
    pub(crate) fn sessions(&self) -> Result<Arc<ServiceSessions<P, C, A>>, ServiceError> {
        self.sessions
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| ServiceError::Rule {
                message: "session owner is not running".into(),
            })
    }
}
