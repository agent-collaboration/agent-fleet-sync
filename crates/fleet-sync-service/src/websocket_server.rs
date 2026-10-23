use crate::{FleetService, ServiceConsumer};
use axum::{
    Router,
    extract::{State, ws::WebSocketUpgrade},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use fleet_sync_protocol::{AuthRefusal, ConsumerProtocol};
use fleet_sync_session::PeerAuthenticator;
use fleet_sync_tokio_runner::SessionAdmission;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

// Axum owns upgrade tasks. A completed upgrade hands the transport to the
// service's owner channel and waits until that owned connection task is joined.
pub(crate) struct AcceptedConnection<R> {
    pub socket: axum::extract::ws::WebSocket,
    pub admission: SessionAdmission<R>,
    pub done: oneshot::Sender<()>,
}
struct UpgradeState<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
> {
    sessions: Arc<crate::service_state::ServiceSessions<P, C, A>>,
    accepted: mpsc::Sender<AcceptedConnection<A::Route>>,
}
impl<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
> Clone for UpgradeState<P, C, A>
{
    fn clone(&self) -> Self {
        Self {
            sessions: self.sessions.clone(),
            accepted: self.accepted.clone(),
        }
    }
}
impl<
    P: ConsumerProtocol,
    C: ServiceConsumer<P>,
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
> FleetService<P, C, A>
{
    pub(crate) fn session_router(&self) -> (Router, mpsc::Receiver<AcceptedConnection<A::Route>>) {
        let (accepted, incoming) = mpsc::channel::<AcceptedConnection<A::Route>>(64);
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
    A: PeerAuthenticator<
            Material = String,
            Route = <P::Delivery as fleet_sync_protocol::DeliveryTypes>::StatusRoute,
        >,
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
    let admission = match state.sessions.authenticate(material).await {
        Ok(admission) => admission,
        Err(reason) => return auth_refusal_response(reason),
    };
    upgrade.on_upgrade(move |socket| async move {
        let (done, finished) = oneshot::channel();
        if state
            .accepted
            .send(AcceptedConnection {
                socket,
                admission,
                done,
            })
            .await
            .is_ok()
        {
            let _ = finished.await;
        }
    })
}

fn auth_refusal_response(reason: AuthRefusal) -> Response {
    let status = if reason == AuthRefusal::WrongKind {
        StatusCode::FORBIDDEN
    } else {
        StatusCode::UNAUTHORIZED
    };
    let value = serde_json::to_vec(&reason)
        .ok()
        .and_then(|encoded| HeaderValue::from_bytes(&encoded).ok());
    let Some(value) = value else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let mut response = status.into_response();
    response
        .headers_mut()
        .insert(fleet_sync_transport_websocket::AUTH_REFUSAL_HEADER, value);
    response
}
