use crate::{
    ServiceConsumer, ServiceError, ServiceHost, ServiceObject, ServiceOptions,
    call_router::ServiceCalls, copy_owner::CopyOwner, session_handler::ServiceSessionHandler,
};
use durable_steps::Engine;
use fleet_sync_protocol::{ConsumerProtocol, DatabaseId};
use fleet_sync_session::PeerAuthenticator;
use fleet_sync_tokio_runner::{ObjectStore, SessionHost};
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock, Weak},
};
use tokio::sync::{Mutex, Notify, OnceCell};

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
    pub copies: Mutex<BTreeMap<DatabaseId, Arc<OnceCell<Arc<CopyOwner>>>>>,
    pub copy_added: Notify,
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
