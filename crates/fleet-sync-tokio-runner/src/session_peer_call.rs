use crate::{SessionHandler, SessionHost, session_host::PendingPeerCall};
use durable_steps::{CallFailure, Host, NotSentReason};
use fleet_sync_protocol::*;
use fleet_sync_session::*;
use std::time::Duration;
use tokio::sync::oneshot;

impl<
    P: ConsumerProtocol,
    A: PeerAuthenticator<Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute>,
    D: SessionHandler<P>,
    H: Host,
> SessionHost<P, A, D, H>
{
    /// The envelope carries the operation's CallId. A frame ID correlates only
    /// this attachment; a lost answer is retried by the operation owner.
    pub async fn request_peer(
        &self,
        peer: PeerId,
        request: ServerRequest<P>,
        answer_timeout: Duration,
    ) -> Result<ClientAnswer<P>, CallFailure> {
        let frame = FrameId::new(self.object.host().new_id()).map_err(|_| unsent())?;
        let (answer, received) = oneshot::channel();
        let (connection, session) = {
            let mut runtime = self.runtime.lock().await;
            let current = runtime
                .sockets
                .iter()
                .find_map(|(connection, socket)| {
                    if socket.binding.peer != peer
                        || socket.binding.expires_at <= self.object.host().now()
                        || socket.revocation_generation != runtime.revocations.get(&peer).copied()
                    {
                        return None;
                    }
                    match runtime.registrations.get(connection) {
                        Some(ConnectionRegistration::Attached { session_id, .. }) => Some((
                            *connection,
                            SessionKey {
                                peer,
                                session_id: *session_id,
                            },
                        )),
                        _ => None,
                    }
                })
                .ok_or_else(unsent)?;
            runtime.pending.insert(
                (current.1, frame),
                PendingPeerCall {
                    connection: current.0,
                    answer,
                },
            );
            current
        };
        if self
            .send(
                connection,
                ServerFrame::Request {
                    frame,
                    body: request,
                },
            )
            .await
            .is_err()
        {
            self.runtime.lock().await.pending.remove(&(session, frame));
            return Err(CallFailure::AnswerLost);
        }
        let result = tokio::time::timeout(answer_timeout, received).await;
        self.runtime.lock().await.pending.remove(&(session, frame));
        match result {
            Ok(Ok(answer)) => Ok(answer),
            _ => Err(CallFailure::AnswerLost),
        }
    }
}
fn unsent() -> CallFailure {
    CallFailure::NotSent {
        reason: NotSentReason::Unreachable { last_seen: None },
    }
}
