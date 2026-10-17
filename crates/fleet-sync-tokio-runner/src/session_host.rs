use crate::TokioObject;
use durable_steps::{
    AtomicStore, Host, StoreError, Uuid, storage::TransactionScope, turso_store::TursoScope,
};
use fleet_sync_protocol::*;
use fleet_sync_session::*;
use sqlx_core::migrate::Migrator;
use sqlx_turso::{Turso, TursoConnection};
use std::{collections::BTreeMap, future::Future, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc, oneshot, watch};

#[derive(Debug, thiserror::Error)]
pub enum SessionHostError {
    #[error(transparent)]
    Storage(#[from] StoreError),
    #[error("session rule failed: {message}")]
    Rule { message: String },
    #[error("session frame encoding failed: {message}")]
    Encoding { message: String },
    #[error("session handler unavailable: {message}")]
    Unavailable { message: String },
}
impl SessionHostError {
    pub(crate) fn rule(error: impl std::fmt::Display) -> Self {
        Self::Rule {
            message: error.to_string(),
        }
    }
    pub(crate) fn encoding(error: impl std::fmt::Display) -> Self {
        Self::Encoding {
            message: error.to_string(),
        }
    }
}

/// Gather fresh domain status after the session commit. Request identities and
/// deduplication remain with the operation's owner, rather than with frame IDs.
pub trait SessionHandler<P: ConsumerProtocol>: Send + Sync + 'static {
    fn gather(
        &self,
        peer: PeerId,
    ) -> impl Future<Output = Result<Vec<DatabaseStatus>, SessionHostError>> + Send;
    fn request(
        &self,
        session: SessionKey,
        operation: OperationRequest<P>,
    ) -> impl Future<Output = Result<OperationAnswer<P>, SessionHostError>> + Send;
}

/// One verified admission for its native host. The host consumes it once and
/// checks that no revocation occurred during verification or before Opening.
#[derive(Debug)]
pub struct SessionAdmission {
    pub(crate) verified: VerifiedPeer,
    pub(crate) revocation_generation: Option<Uuid>,
    pub(crate) owner: Arc<()>,
}

pub struct SessionHost<P: ConsumerProtocol, A: PeerAuthenticator, D: SessionHandler<P>, H: Host> {
    pub(crate) object: Arc<TokioObject<H>>,
    pub(crate) authenticator: Arc<A>,
    pub(crate) handler: Arc<D>,
    pub(crate) runtime: Mutex<SessionRuntime<P>>,
    pub(crate) send_timeout: Duration,
    pub(crate) admission_owner: Arc<()>,
    #[cfg(feature = "test-support")]
    registration_changes: watch::Sender<()>,
}
pub(crate) struct SocketPeer {
    pub binding: ConnectionBinding,
    pub revocation_generation: Option<Uuid>,
    pub outgoing: mpsc::Sender<Bytes>,
    pub close: watch::Sender<Option<CloseReason>>,
}
pub(crate) struct PendingPeerCall<P: ConsumerProtocol> {
    pub connection: ConnectionId,
    pub answer: oneshot::Sender<ClientAnswer<P>>,
}
pub(crate) struct SessionRuntime<P: ConsumerProtocol> {
    pub registrations: BTreeMap<ConnectionId, ConnectionRegistration>,
    pub sockets: BTreeMap<ConnectionId, SocketPeer>,
    pub revocations: Arc<BTreeMap<PeerId, Uuid>>,
    pub pending: BTreeMap<(SessionKey, FrameId), PendingPeerCall<P>>,
}
pub(crate) enum SessionInput {
    Opened,
    Frame(Bytes),
    Closed(CloseCause),
    Revoke,
}
struct SessionTransaction<'scope, 'transaction, H: Host> {
    inner: &'scope mut TursoScope<'transaction, H>,
    registrations: BTreeMap<ConnectionId, ConnectionRegistration>,
}
impl<H: Host> TransactionScope for SessionTransaction<'_, '_, H> {
    type Database = Turso;
    type Executor<'query>
        = &'query mut TursoConnection
    where
        Self: 'query;
    fn executor(&mut self) -> Self::Executor<'_> {
        self.inner.executor()
    }
}
impl<H: Host> SessionScope for SessionTransaction<'_, '_, H> {
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
}

impl<P: ConsumerProtocol, A: PeerAuthenticator, D: SessionHandler<P>, H: Host>
    SessionHost<P, A, D, H>
{
    /// Observe the committed host registration without changing session order.
    #[cfg(feature = "test-support")]
    pub async fn wait_opening(&self, peer: PeerId) -> Result<ConnectionId, SessionHostError> {
        let mut changes = self.registration_changes.subscribe();
        loop {
            {
                let runtime = self.runtime.lock().await;
                if let Some(connection) = runtime.sockets.iter().find_map(|(connection, socket)| {
                    (socket.binding.peer == peer
                        && matches!(
                            runtime.registrations.get(connection),
                            Some(ConnectionRegistration::Opening)
                        ))
                    .then_some(*connection)
                }) {
                    return Ok(connection);
                }
            }
            changes.changed().await.map_err(SessionHostError::rule)?;
        }
    }

    pub async fn open(
        object: Arc<TokioObject<H>>,
        authenticator: Arc<A>,
        handler: Arc<D>,
        send_timeout: Duration,
    ) -> Result<Self, SessionHostError> {
        object
            .transaction(|scope| {
                Box::pin(async move {
                    let mut migrator = Migrator::with_migrations(
                        SESSION_MIGRATOR.iter().cloned().collect::<Vec<_>>(),
                    );
                    migrator.dangerous_set_table_name("_fleet_sync_session_history");
                    migrator
                        .run_direct(None, scope.executor(), false)
                        .await
                        .map_err(StoreError::backend)
                })
            })
            .await?;
        Ok(Self {
            object,
            authenticator,
            handler,
            runtime: Mutex::new(SessionRuntime {
                registrations: BTreeMap::new(),
                sockets: BTreeMap::new(),
                revocations: Arc::new(BTreeMap::new()),
                pending: BTreeMap::new(),
            }),
            send_timeout,
            admission_owner: Arc::new(()),
            #[cfg(feature = "test-support")]
            registration_changes: watch::channel(()).0,
        })
    }

    pub async fn authenticate(
        &self,
        material: A::Material,
    ) -> Result<SessionAdmission, AuthRefusal> {
        // A shared immutable snapshot spans external verification without
        // holding the host lock or rejecting an unrelated peer's revocation.
        let before = self.runtime.lock().await.revocations.clone();
        let verified = self.authenticator.verify(material).await?;
        let runtime = self.runtime.lock().await;
        let revocation_generation = runtime.revocations.get(&verified.peer).copied();
        if before.get(&verified.peer).copied() != revocation_generation {
            return Err(AuthRefusal::Revoked);
        }
        if verified.expires_at <= self.object.host().now() {
            return Err(AuthRefusal::Expired);
        }
        Ok(SessionAdmission {
            verified,
            revocation_generation,
            owner: self.admission_owner.clone(),
        })
    }

    /// The consumer commits its credential revocation before calling this.
    /// One host lock closes Opening and Attached registrations before any
    /// later frame or asynchronously verified socket can attach.
    pub async fn revoke(&self, peer: PeerId) -> Result<(), SessionHostError> {
        let binding = ConnectionBinding::new(
            ConnectionId::new(self.object.host().new_id()).map_err(SessionHostError::rule)?,
            VerifiedPeer {
                peer,
                expires_at: self.object.host().now(),
            },
        );
        let effects = self.apply(binding, SessionInput::Revoke).await?;
        self.dispatch_direct(effects).await
    }

    pub(crate) async fn apply(
        &self,
        binding: ConnectionBinding,
        input: SessionInput,
    ) -> Result<Vec<SessionEffect<P>>, SessionHostError> {
        let mut runtime = self.runtime.lock().await;
        let revoke = matches!(input, SessionInput::Revoke);
        if revoke {
            Arc::make_mut(&mut runtime.revocations)
                .insert(binding.peer, self.object.host().new_id());
        }
        let generation = runtime.revocations.get(&binding.peer).copied();
        let revoked = runtime
            .sockets
            .get(&binding.connection)
            .is_none_or(|socket| socket.revocation_generation != generation);
        let registrations = runtime.registrations.clone();
        let connections = runtime
            .sockets
            .iter()
            .filter_map(|(id, socket)| (socket.binding.peer == binding.peer).then_some(*id))
            .collect::<Vec<_>>();
        let now = self.object.host().now();
        let result=self.object.transaction(move |inner| Box::pin(async move {
            let mut scope=SessionTransaction {inner,registrations};
            let mut effects=match input {
                SessionInput::Revoke=>end_sessions::<_,P>(&mut scope,&binding.peer,EndReason::PeerRevoked,now).await,
                SessionInput::Opened|SessionInput::Frame(_) if revoked=>{
                    scope.set_registration(&binding.connection,ConnectionRegistration::Closed);
                    Ok(vec![SessionEffect::Close {connection:binding.connection,reason:CloseReason::CredentialRejected(AuthRefusal::Revoked)}])
                }
                SessionInput::Opened=>connection_opened::<_,P>(&mut scope,&binding,now).await,
                SessionInput::Frame(bytes)=>frame_received::<_,P>(&mut scope,&binding,&bytes,now).await,
                SessionInput::Closed(cause)=>connection_closed::<_,P>(&mut scope,&binding,cause,now).await,
            }.map_err(StoreError::backend)?;
            if revoke {
                for connection in connections {
                    scope.set_registration(&connection,ConnectionRegistration::Closed);
                    if !effects.iter().any(|effect| matches!(effect,SessionEffect::Close {connection:closing,..} if *closing==connection)) {
                        effects.push(SessionEffect::Close {connection,reason:CloseReason::CredentialRejected(AuthRefusal::Revoked)});
                    }
                }
            }
            Ok((scope.registrations,effects))
        })).await;
        let (registrations, effects) = match result {
            Ok(value) => value,
            Err(error) => {
                if revoke {
                    // Credential revocation is already committed at its owner.
                    // SQL failure cannot keep an authenticated transport alive.
                    for socket in runtime
                        .sockets
                        .values()
                        .filter(|socket| socket.binding.peer == binding.peer)
                    {
                        socket
                            .close
                            .send_replace(Some(CloseReason::CredentialRejected(
                                AuthRefusal::Revoked,
                            )));
                    }
                }
                return Err(error.into());
            }
        };
        runtime.registrations = registrations;
        #[cfg(feature = "test-support")]
        self.registration_changes.send_replace(());
        Ok(effects)
    }

    pub(crate) async fn send(
        &self,
        connection: ConnectionId,
        frame: ServerFrame<P>,
    ) -> Result<(), SessionHostError> {
        if let ServerFrame::Closed { reason } = frame {
            self.close_socket(connection, reason).await;
            return Ok(());
        }
        let bytes = serde_json::to_vec(&frame)
            .map_err(SessionHostError::encoding)?
            .into();
        let outgoing = self
            .runtime
            .lock()
            .await
            .sockets
            .get(&connection)
            .map(|socket| socket.outgoing.clone());
        if let Some(outgoing) = outgoing
            && !matches!(
                tokio::time::timeout(self.send_timeout, outgoing.send(bytes)).await,
                Ok(Ok(()))
            )
        {
            self.close_socket(connection, CloseReason::ClientClosed)
                .await;
        }
        Ok(())
    }
    pub(crate) async fn close_socket(&self, connection: ConnectionId, reason: CloseReason) {
        if let Some(socket) = self.runtime.lock().await.sockets.get(&connection) {
            socket.close.send_replace(Some(reason));
        }
    }

    pub(crate) async fn dispatch_direct(
        &self,
        effects: Vec<SessionEffect<P>>,
    ) -> Result<(), SessionHostError> {
        for effect in effects {
            match effect {
                SessionEffect::Welcome {
                    connection,
                    peer,
                    session_id,
                } => match self.handler.gather(peer).await {
                    Ok(databases) => {
                        self.send(
                            connection,
                            ServerFrame::Welcome {
                                session_id,
                                databases,
                            },
                        )
                        .await?
                    }
                    Err(error) => {
                        self.close_socket(connection, CloseReason::ClientClosed)
                            .await;
                        return Err(error);
                    }
                },
                SessionEffect::Send { connection, frame } => self.send(connection, frame).await?,
                SessionEffect::Close { connection, reason } => {
                    self.close_socket(connection, reason).await
                }
                SessionEffect::RouteAnswer {
                    session,
                    frame,
                    answer,
                } => {
                    if let Some(pending) =
                        self.runtime.lock().await.pending.remove(&(session, frame))
                    {
                        let _ = pending.answer.send(answer);
                    }
                }
                SessionEffect::Route { .. } => {
                    return Err(SessionHostError::Unavailable {
                        message: "request route needs its owned connection task".into(),
                    });
                }
            }
        }
        Ok(())
    }
}
