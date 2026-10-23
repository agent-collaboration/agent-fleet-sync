//! Session tasks borrow the same SQL journal and staged registrations as socket inputs.
use crate::session_host::SessionRuntime;
use crate::{SessionHandler, SessionHost, SessionHostError, TokioObject};
use durable_steps::{
    AtomicStore, BoxFuture, Engine, Host, JournalScope, StoreError, storage::TransactionScope,
    turso_store::TursoScope,
};
use fleet_sync_protocol::*;
use fleet_sync_session::{
    ConnectionRegistration, PeerAuthenticator, PublishSchemaSupport, PublishSchemaSupportInput,
    SessionEffect, SessionScope, schema_support_request_key,
};
use sqlx_turso::{Turso, TursoConnection};
use std::{
    collections::BTreeMap,
    marker::PhantomData,
    sync::{Arc, OnceLock, Weak},
};
use tokio::sync::Mutex;

pub(crate) struct SessionStore<
    P: ConsumerProtocol,
    A: PeerAuthenticator<Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute>,
    D: SessionHandler<P>,
    H: Host,
> {
    pub object: Arc<TokioObject<H>>,
    pub runtime: Arc<Mutex<SessionRuntime<P>>>,
    pub owner: OnceLock<Weak<SessionHost<P, A, D, H>>>,
    #[cfg(feature = "test-support")]
    pub finalize_probe: Arc<crate::schema_finalize_probe::SchemaFinalizeProbe>,
}
pub(crate) struct SessionTransaction<
    't,
    P: ConsumerProtocol,
    A: PeerAuthenticator<Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute>,
    D: SessionHandler<P>,
    H: Host,
> {
    pub inner: TursoScope<'t, H>,
    pub registrations: BTreeMap<ConnectionId, ConnectionRegistration>,
    pub effects: Vec<SessionEffect<P>>,
    pub wake_at: Option<SyncTime>,
    marker: PhantomData<fn() -> (A, D)>,
}
impl<
    't,
    P: ConsumerProtocol,
    A: PeerAuthenticator<Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute>,
    D: SessionHandler<P>,
    H: Host,
> SessionTransaction<'t, P, A, D, H>
{
    pub fn new(
        connection: &'t mut TursoConnection,
        host: &'t H,
        registrations: BTreeMap<ConnectionId, ConnectionRegistration>,
    ) -> Self {
        Self {
            inner: TursoScope::new(connection, host),
            registrations,
            effects: Vec::new(),
            wake_at: None,
            marker: PhantomData,
        }
    }
}
impl<
    P: ConsumerProtocol,
    A: PeerAuthenticator<Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute>,
    D: SessionHandler<P>,
    H: Host,
> TransactionScope for SessionTransaction<'_, P, A, D, H>
{
    type Database = Turso;
    type Executor<'q>
        = &'q mut TursoConnection
    where
        Self: 'q;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.inner.executor()
    }
}
impl<
    P: ConsumerProtocol,
    A: PeerAuthenticator<Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute>,
    D: SessionHandler<P>,
    H: Host,
> SessionScope for SessionTransaction<'_, P, A, D, H>
{
    fn registration(&self, connection: &ConnectionId) -> Option<ConnectionRegistration> {
        self.registrations.get(connection).copied()
    }
    fn set_registration(
        &mut self,
        connection: &ConnectionId,
        registration: ConnectionRegistration,
    ) {
        self.registrations.insert(*connection, registration);
    }
    async fn publish_schema_support(
        &mut self,
        input: PublishSchemaSupportInput,
        now: SyncTime,
    ) -> Result<(), fleet_sync_session::StoreError> {
        let key = schema_support_request_key(&input.report).map_err(session_error)?;
        self.admit::<PublishSchemaSupport<SessionStore<P, A, D, H>, Turso>>(&key, &input)
            .await
            .map_err(session_error)?;
        self.ensure_wake(now).await.map_err(session_error)?;
        self.wake_at = Some(self.wake_at.map_or(now, |existing| existing.min(now)));
        Ok(())
    }
    fn welcome_after_commit(
        &mut self,
        connection: ConnectionId,
        peer: PeerId,
        session_id: SessionId,
    ) {
        self.effects.push(SessionEffect::Welcome {
            connection,
            peer,
            session_id,
        });
    }
}
fn session_error(error: impl std::fmt::Display) -> fleet_sync_session::StoreError {
    fleet_sync_session::StoreError::Unavailable {
        message: error.to_string(),
    }
}
impl<
    P: ConsumerProtocol,
    A: PeerAuthenticator<Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute>,
    D: SessionHandler<P>,
    H: Host,
> AtomicStore for SessionStore<P, A, D, H>
{
    type Scope<'t> = SessionTransaction<'t, P, A, D, H>;
    async fn transaction<T, F>(&self, run: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: for<'s, 't> FnOnce(&'s mut Self::Scope<'t>) -> BoxFuture<'s, Result<T, StoreError>>
            + Send
            + 'static,
    {
        // Input transactions acquire these guards in this same order.
        let mut runtime = self.runtime.lock().await;
        let registrations = runtime.registrations.clone();
        let host = self.object.host();
        #[cfg(feature = "test-support")]
        let probe = self.finalize_probe.clone();
        let committed = self
            .object
            .transaction(move |inner| {
                Box::pin(async move {
                    let mut scope = SessionTransaction::<P, A, D, H>::new(
                        inner.executor(),
                        host.as_ref(),
                        registrations,
                    );
                    let value = run(&mut scope).await?;
                    #[cfg(feature = "test-support")]
                    if probe.rejects(&scope.effects) {
                        return Err(StoreError::backend("injected schema finalization rollback"));
                    }
                    Ok((value, scope.registrations, scope.effects, scope.wake_at))
                })
            })
            .await;
        let (value, registrations, effects, wake_at) = match committed {
            Ok(value) => value,
            Err(error) => {
                drop(runtime);
                #[cfg(feature = "test-support")]
                self.finalize_probe.after_failed_transaction().await;
                return Err(error);
            }
        };
        runtime.registrations = registrations;
        drop(runtime);
        if let Some(at) = wake_at
            && let Err(error) = self.object.host().wake_at(at).await
        {
            tracing::warn!(%error, "committed session publication wake failed");
        }
        // The local result is already committed. Welcome failure closes its socket
        // through the existing host path; it cannot turn this into a rollback.
        if !effects.is_empty()
            && let Some(owner) = self.owner.get().and_then(Weak::upgrade)
            && let Err(error) = owner.dispatch_direct(effects).await
        {
            tracing::warn!(%error, "post-commit session effect failed");
        }
        Ok(value)
    }
}

pub(crate) type SessionEngine<P, A, D, H> = Engine<SessionStore<P, A, D, H>, H>;

impl<P, A, D, R, C> SessionHost<P, A, D, crate::TokioHost<R, C>>
where
    P: ConsumerProtocol,
    A: PeerAuthenticator<Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute>,
    D: SessionHandler<P>,
    R: crate::CallRouter,
    C: runtime_primitives::Clock + 'static,
{
    /// Own and join the session publication engine alongside the service's other runners.
    pub async fn run(
        self: Arc<Self>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), SessionHostError> {
        self.store
            .owner
            .set(Arc::downgrade(&self))
            .map_err(|_| SessionHostError::rule("session runner already owned"))?;
        crate::TokioRunner::new(self.engine.clone(), self.object.host())
            .run(shutdown)
            .await
            .map_err(Into::into)
    }
}
