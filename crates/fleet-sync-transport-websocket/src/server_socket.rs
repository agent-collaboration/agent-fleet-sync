use axum::extract::ws::{Message, WebSocket};
use fleet_sync_protocol::CloseReason;
use fleet_sync_session::{Bytes, ConnectionClosed, TransportConnection};
use std::time::Duration;
use tokio::time::timeout;

/// Wrap the socket supplied by `WebSocketUpgrade::on_upgrade` after authentication.
/// The consumer owns the upgrade handler and the lifetime of its connection task.
pub struct AxumConnection {
    socket: WebSocket,
    send_timeout: Duration,
    closed: bool,
}
impl AxumConnection {
    pub fn new(socket: WebSocket, send_timeout: Duration) -> Self {
        Self {
            socket,
            send_timeout,
            closed: false,
        }
    }
}
impl TransportConnection for AxumConnection {
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
            match self.socket.recv().await {
                Some(Ok(Message::Binary(frame))) => return Some(Ok(frame)),
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) | None => {
                    self.closed = true;
                    return None;
                }
                Some(Ok(Message::Text(_))) => {
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
        let _ = timeout(self.send_timeout, self.socket.send(Message::Close(None))).await;
    }
}
