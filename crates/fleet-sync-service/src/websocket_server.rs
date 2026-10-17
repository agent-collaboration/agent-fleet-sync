use crate::{FleetService, ServiceConsumer};
use axum::{
    Router,
    extract::{State, ws::WebSocketUpgrade},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use fleet_sync_protocol::ConsumerProtocol;
use fleet_sync_session::{PeerAuthenticator, VerifiedPeer};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

// Axum owns upgrade tasks. A completed upgrade hands the transport to the
// service's owner channel and waits until that owned connection task is joined.
pub(crate) struct AcceptedConnection {
    pub socket: axum::extract::ws::WebSocket,
    pub peer: VerifiedPeer,
    pub done: oneshot::Sender<()>,
}
struct UpgradeState<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<Material = String>,
> {
    sessions: Arc<crate::service_state::ServiceSessions<P, C, A>>,
    accepted: mpsc::Sender<AcceptedConnection>,
}
impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>> Clone
    for UpgradeState<P, C, A>
{
    fn clone(&self) -> Self {
        Self {
            sessions: self.sessions.clone(),
            accepted: self.accepted.clone(),
        }
    }
}
impl<P: ConsumerProtocol, C: ServiceConsumer<P>, A: PeerAuthenticator<Material = String>>
    FleetService<P, C, A>
{
    pub(crate) fn session_router(&self) -> (Router, mpsc::Receiver<AcceptedConnection>) {
        let (accepted, incoming) = mpsc::channel::<AcceptedConnection>(64);
        (
            Router::new()
                .route("/session", get(upgrade::<P, C, A>))
                .with_state(UpgradeState {
                    sessions: self.sessions.clone(),
                    accepted,
                }),
            incoming,
        )
    }
}
async fn upgrade<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<Material = String>,
>(
    State(state): State<UpgradeState<P, C, A>>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let material = headers
        .get("authorization")
        .and_then(|header| header.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let peer = match state.sessions.authenticate(material).await {
        Ok(peer) => peer,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };
    upgrade.on_upgrade(move |socket| async move {
        let (done, finished) = oneshot::channel();
        if state
            .accepted
            .send(AcceptedConnection { socket, peer, done })
            .await
            .is_ok()
        {
            let _ = finished.await;
        }
    })
}
