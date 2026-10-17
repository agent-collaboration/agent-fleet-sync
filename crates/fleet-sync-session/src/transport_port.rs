use bytes::Bytes;
use fleet_sync_protocol::{AuthRefusal, CloseReason};
use std::future::Future;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Endpoint(url::Url);

impl Endpoint {
    pub fn new(value: &str) -> Result<Self, ConnectFailure> {
        let endpoint = url::Url::parse(value).map_err(|_| ConnectFailure::InvalidEndpoint)?;
        if endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
        {
            return Err(ConnectFailure::InvalidEndpoint);
        }
        Ok(Self(endpoint))
    }
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Opaque authentication material. Only the consumer's authenticator interprets it.
pub struct PeerCredential(String);
impl PeerCredential {
    pub fn new(value: impl Into<String>) -> Result<Self, ConnectFailure> {
        let value = value.into();
        if value.is_empty() {
            return Err(ConnectFailure::InvalidCredential);
        }
        Ok(Self(value))
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Debug for PeerCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PeerCredential([redacted])")
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConnectFailure {
    #[error("invalid transport endpoint")]
    InvalidEndpoint,
    #[error("invalid credential material")]
    InvalidCredential,
    #[error("credential rejected: {0:?}")]
    CredentialRejected(AuthRefusal),
    #[error("transport unavailable")]
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConnectionClosed {
    #[error("connection closed")]
    Transport,
    #[error("only binary data frames are supported")]
    UnsupportedFrame,
}

pub trait SessionTransport: Send + Sync + 'static {
    type Connection: TransportConnection;
    fn connect(
        &self,
        endpoint: &Endpoint,
        credential: &PeerCredential,
    ) -> impl Future<Output = Result<Self::Connection, ConnectFailure>> + Send;
}

pub trait TransportConnection: Send + 'static {
    fn send(&mut self, frame: Bytes) -> impl Future<Output = Result<(), ConnectionClosed>> + Send;
    fn receive(&mut self) -> impl Future<Output = Option<Result<Bytes, ConnectionClosed>>> + Send;
    fn close(self, reason: CloseReason) -> impl Future<Output = ()> + Send;
}
