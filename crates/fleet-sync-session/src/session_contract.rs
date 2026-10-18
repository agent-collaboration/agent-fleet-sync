use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};
use std::future::Future;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StoreError {
    #[error("session store unavailable: {message}")]
    Unavailable { message: String },
    #[error("invalid stored session field: {field}")]
    Corrupt { field: String },
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error(transparent)]
    Storage(#[from] StoreError),
    #[error("notice position exhausted")]
    NoticePositionExhausted,
    #[error("attach sequence exhausted")]
    AttachSequenceExhausted,
    #[error("session does not exist or has ended")]
    SessionEnded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionKey {
    pub peer: PeerId,
    pub session_id: SessionId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EndReason {
    NewSession,
    PeerRevoked,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum SessionState {
    Active { connection: ConnectionId },
    Detached { since: SyncTime },
    Ended { reason: EndReason },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionRecord {
    pub peer: PeerId,
    pub session_id: SessionId,
    pub state: SessionState,
    pub notice_head: NoticePosition,
}
impl SessionRecord {
    pub const fn key(&self) -> SessionKey {
        SessionKey {
            peer: self.peer,
            session_id: self.session_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ConnectionRegistration {
    Opening,
    Attaching {
        session_id: SessionId,
        schema_support: SchemaSupport,
        attach_seq: AttachSequence,
    },
    Attached {
        session_id: SessionId,
        schema_support: SchemaSupport,
    },
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerifiedPeer {
    pub peer: PeerId,
    pub expires_at: SyncTime,
}

pub trait PeerAuthenticator: Send + Sync + 'static {
    type Material: Send;
    fn verify(
        &self,
        material: Self::Material,
    ) -> impl Future<Output = Result<VerifiedPeer, AuthRefusal>> + Send;
}

/// Host-created identity for one authenticated transport, never decoded from a frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionBinding {
    pub connection: ConnectionId,
    pub peer: PeerId,
    pub expires_at: SyncTime,
}
impl ConnectionBinding {
    pub const fn new(connection: ConnectionId, verified: VerifiedPeer) -> Self {
        Self {
            connection,
            peer: verified.peer,
            expires_at: verified.expires_at,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseCause {
    Dropped,
    Replaced,
    CredentialExpired,
    MalformedFrame,
    PeerRevoked,
}

pub enum SessionEffect<P: ConsumerProtocol = NoConsumer> {
    /// The host gathers current registry/copy status after commit, then sends Welcome.
    /// Report an unreadable copy as Unavailable; close if the overall gather fails.
    Welcome {
        connection: ConnectionId,
        peer: PeerId,
        session_id: SessionId,
    },
    Send {
        connection: ConnectionId,
        frame: ServerFrame<P>,
    },
    Close {
        connection: ConnectionId,
        reason: CloseReason,
    },
    Route {
        session: SessionKey,
        frame: FrameId,
        operation: OperationRequest<P>,
    },
    RouteAnswer {
        session: SessionKey,
        frame: FrameId,
        answer: ClientAnswer<P>,
    },
}

pub struct StoredNotice<P: ConsumerProtocol> {
    pub position: NoticePosition,
    pub body: Notice<P>,
}

/// Capabilities of the host's borrowed session transaction, not an owned store.
///
/// Session and notice writes commit atomically. Registration edits are staged
/// until that commit: on Tokio the host owns them beside its socket tasks; on
/// a Durable Object they are its hibernatable socket attachments. A rolled-back
/// session transaction must publish none of its registration edits or effects.
pub trait SessionScope: durable_steps::storage::TransactionScope {
    fn registration(&self, connection: &ConnectionId) -> Option<ConnectionRegistration>;
    fn set_registration(&mut self, connection: &ConnectionId, registration: ConnectionRegistration);
    /// Admit PublishSchemaSupport and ensure its durable deadline through this
    /// same borrowed journal transaction. The runtime activates it after commit.
    fn publish_schema_support(
        &mut self,
        input: crate::PublishSchemaSupportInput,
        now: SyncTime,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
    /// Stage Welcome as a host effect, never as a journaled local-step output.
    /// Commit publishes registration edits and exposes effects; rollback discards
    /// both. External effects run after transaction exclusion is released.
    fn welcome_after_commit(
        &mut self,
        connection: ConnectionId,
        peer: PeerId,
        session_id: SessionId,
    );
}
