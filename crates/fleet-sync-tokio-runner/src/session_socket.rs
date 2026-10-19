use crate::{
    SessionAdmission, SessionHandler, SessionHost, SessionHostError,
    session_host::{SessionInput, SocketPeer},
};
use durable_steps::Host;
use fleet_sync_protocol::*;
use fleet_sync_session::*;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};

impl<
    P: ConsumerProtocol,
    A: PeerAuthenticator<Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute>,
    D: SessionHandler<P>,
    H: Host,
> SessionHost<P, A, D, H>
{
    /// Run one owned transport. The server owns and joins this future and its
    /// request tasks; neither the session rules nor this method detaches work.
    pub async fn serve_connection<T: TransportConnection>(
        self: Arc<Self>,
        connection: T,
        admission: SessionAdmission<A::Route>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), SessionHostError> {
        if !Arc::ptr_eq(&self.admission_owner, &admission.owner) {
            connection
                .close(CloseReason::CredentialRejected(AuthRefusal::Revoked))
                .await;
            return Err(SessionHostError::rule(
                "admission belongs to another session host",
            ));
        }
        let binding = ConnectionBinding::new(
            ConnectionId::new(self.object.host().new_id()).map_err(SessionHostError::rule)?,
            admission.verified,
        );
        let (outgoing, incoming) = mpsc::channel(64);
        let (close, closed) = watch::channel(None);
        self.runtime.lock().await.sockets.insert(
            binding.connection,
            SocketPeer {
                binding,
                route: admission.route,
                revocation_generation: admission.revocation_generation,
                outgoing,
                close,
            },
        );
        let result = async {
            self.dispatch_direct(self.apply(binding, SessionInput::Opened).await?)
                .await?;
            self.socket_loop(connection, binding, incoming, closed, shutdown)
                .await
        }
        .await;
        let cause = result.as_ref().copied().unwrap_or(CloseCause::Dropped);
        let detached = self.apply(binding, SessionInput::Closed(cause)).await;
        {
            let mut runtime = self.runtime.lock().await;
            runtime.sockets.remove(&binding.connection);
            runtime.registrations.remove(&binding.connection);
            runtime
                .pending
                .retain(|_, pending| pending.connection != binding.connection);
        }
        #[cfg(feature = "test-support")]
        self.registration_changes.send_replace(());
        result?;
        self.dispatch_direct(detached?).await
    }

    async fn socket_loop<T: TransportConnection>(
        self: &Arc<Self>,
        mut connection: T,
        binding: ConnectionBinding,
        mut outgoing: mpsc::Receiver<Bytes>,
        mut closed: watch::Receiver<Option<CloseReason>>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<CloseCause, SessionHostError> {
        let remaining = u64::try_from(
            binding
                .expires_at
                .unix_millis()
                .saturating_sub(self.object.host().now().unix_millis()),
        )
        .unwrap_or(0);
        let deadline = tokio::time::sleep(Duration::from_millis(remaining));
        tokio::pin!(deadline);
        let mut tasks = JoinSet::new();
        let mut close_reason = None;
        let mut cause = CloseCause::Dropped;
        loop {
            if *shutdown.borrow() {
                break;
            }
            if let Some(reason) = closed.borrow().clone() {
                close_reason = Some(reason);
                break;
            }
            tokio::select! {
                incoming=connection.receive()=>match incoming {
                    Some(Ok(bytes))=>{
                        let effects=self.apply(binding,SessionInput::Frame(bytes)).await?;
                        for effect in effects {
                            if let SessionEffect::Route {session,frame,operation}=effect {
                                let host=self.clone();
                                tasks.spawn(async move {
                                    let Some(route)=host.admitted_request_route(binding,session).await else {return Ok(());};
                                    let answer=host.handler.request(session,route,operation).await?;
                                    host.send(binding.connection,ServerFrame::Answer {frame,body:answer}).await
                                });
                            } else {self.dispatch_direct(vec![effect]).await?;}
                        }
                    }
                    Some(Err(_))|None=>break,
                },
                bytes=outgoing.recv()=>match bytes {
                    Some(bytes)=>if connection.send(bytes).await.is_err() {break;},
                    None=>break,
                },
                changed=closed.changed()=>if changed.is_err() {break;},
                changed=shutdown.changed()=>if changed.is_err() {break;},
                _=&mut deadline=>{
                    cause=CloseCause::CredentialExpired;
                    close_reason=Some(CloseReason::CredentialRejected(AuthRefusal::Expired));
                    break;
                },
                joined=tasks.join_next(),if !tasks.is_empty()=>{
                    if let Some(result)=joined {
                        match result {
                            Ok(Ok(()))=>{},
                            Ok(Err(error))=>{tracing::warn!(%error,"session request failed");break;}
                            Err(error)=>{tracing::warn!(%error,"session request task stopped");break;}
                        }
                    }
                }
            }
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        if let Some(reason) = close_reason {
            let frame = ServerFrame::<P>::Closed {
                reason: reason.clone(),
            };
            let bytes = serde_json::to_vec(&frame).map_err(SessionHostError::encoding)?;
            let _ = connection.send(bytes.into()).await;
            connection.close(reason).await;
        }
        Ok(cause)
    }

    async fn admitted_request_route(
        &self,
        binding: ConnectionBinding,
        session: SessionKey,
    ) -> Option<<P::Delivery as DeliveryTypes>::StatusRoute> {
        let runtime = self.runtime.lock().await;
        let socket = runtime.sockets.get(&binding.connection)?;
        (socket.revocation_generation == runtime.revocations.get(&binding.peer).copied()
            && binding.peer == session.peer
            && binding.expires_at > self.object.host().now()
            && matches!(runtime.registrations.get(&binding.connection),Some(ConnectionRegistration::Attached {session_id,..}) if *session_id==session.session_id))
            .then(|| socket.route.clone())
    }
}
