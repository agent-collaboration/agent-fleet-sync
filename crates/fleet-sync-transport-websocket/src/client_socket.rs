use fleet_sync_protocol::{AuthRefusal, CloseReason};
use fleet_sync_session::{
    Bytes, ConnectFailure, ConnectionClosed, Endpoint, PeerCredential, SessionTransport,
    TransportConnection,
};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderValue, StatusCode, header::AUTHORIZATION},
    },
};

#[derive(Clone, Debug)]
pub struct WebSocketTransport {
    pub connect_timeout: Duration,
    pub send_timeout: Duration,
}
impl Default for WebSocketTransport {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(30),
            send_timeout: Duration::from_secs(30),
        }
    }
}

pub struct WebSocketConnection {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    send_timeout: Duration,
    closed: bool,
}

impl SessionTransport for WebSocketTransport {
    type Connection = WebSocketConnection;
    async fn connect(
        &self,
        endpoint: &Endpoint,
        credential: &PeerCredential,
    ) -> Result<Self::Connection, ConnectFailure> {
        let mut request = endpoint
            .as_str()
            .into_client_request()
            .map_err(|_| ConnectFailure::InvalidEndpoint)?;
        let mut authorization = HeaderValue::from_str(credential.expose())
            .map_err(|_| ConnectFailure::InvalidCredential)?;
        authorization.set_sensitive(true);
        request.headers_mut().insert(AUTHORIZATION, authorization);
        let (socket, _) = timeout(
            self.connect_timeout,
            tokio_tungstenite::connect_async(request),
        )
        .await
        .map_err(|_| ConnectFailure::Unavailable)?
        .map_err(|error| match error {
            tokio_tungstenite::tungstenite::Error::Http(response)
                if matches!(
                    response.status(),
                    StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
                ) =>
            {
                ConnectFailure::CredentialRejected(AuthRefusal::Unknown)
            }
            _ => ConnectFailure::Unavailable,
        })?;
        Ok(WebSocketConnection {
            socket,
            send_timeout: self.send_timeout,
            closed: false,
        })
    }
}

impl TransportConnection for WebSocketConnection {
    async fn send(&mut self, frame: Bytes) -> Result<(), ConnectionClosed> {
        if self.closed {
            return Err(ConnectionClosed::Transport);
        }
        match timeout(self.send_timeout, self.socket.send(Message::Binary(frame))).await {
            Ok(Ok(())) => Ok(()),
            _ => {
                self.closed = true;
                Err(ConnectionClosed::Transport)
            }
        }
    }
    async fn receive(&mut self) -> Option<Result<Bytes, ConnectionClosed>> {
        if self.closed {
            return None;
        }
        loop {
            match self.socket.next().await {
                Some(Ok(Message::Binary(frame))) => return Some(Ok(frame)),
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) | None => {
                    self.closed = true;
                    return None;
                }
                Some(Ok(Message::Text(_) | Message::Frame(_))) => {
                    self.closed = true;
                    return Some(Err(ConnectionClosed::UnsupportedFrame));
                }
                Some(Err(_)) => {
                    self.closed = true;
                    return Some(Err(ConnectionClosed::Transport));
                }
            }
        }
    }
    async fn close(mut self, _reason: CloseReason) {
        let _ = timeout(self.send_timeout, self.socket.close(None)).await;
    }
}
