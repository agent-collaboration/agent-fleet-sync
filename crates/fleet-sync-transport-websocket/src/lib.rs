//! One binary WebSocket message carries one unchanged session frame.
//! Authentication and session state belong to the consumer and session rules.

mod client_socket;
mod server_socket;
pub use client_socket::{WebSocketConnection, WebSocketTransport};
pub use server_socket::AxumConnection;
