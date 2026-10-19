use crate::{MaintenanceTransaction, OpenError};
use fleet_sync_protocol::{
    ConsumerProtocol, DatabaseId, Epoch, NoConsumer, Piece, ReceiverId, ReplicatedTransaction,
    ReplicationAnswer, ReplicationPosition,
};
use std::{future::Future, num::NonZeroUsize, time::Duration};

pub struct CapturedTransaction<'transaction> {
    pub position: ReplicationPosition,
    pub transaction: &'transaction ReplicatedTransaction,
}

pub trait CaptureBookkeeping: Send + Sync + 'static {
    fn record(
        &self,
        scope: &mut MaintenanceTransaction<'_>,
        captured: &CapturedTransaction<'_>,
    ) -> impl Future<Output = Result<(), BookkeepingError>> + Send;
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("capture bookkeeping failed: {message}")]
pub struct BookkeepingError {
    pub message: String,
}

impl CaptureBookkeeping for () {
    async fn record(
        &self,
        _: &mut MaintenanceTransaction<'_>,
        _: &CapturedTransaction<'_>,
    ) -> Result<(), BookkeepingError> {
        Ok(())
    }
}

pub trait PieceAnnotation: Send + Sync + 'static {
    type Protocol: ConsumerProtocol;
    /// Only captured input is available. For a split transaction the final
    /// part's annotation is computed from that complete captured transaction.
    /// Identical captured input must reproduce identical serialized annotation
    /// bytes, so retries retain the same part boundaries and digests.
    fn annotate(
        &self,
        transactions: &[CapturedTransaction<'_>],
    ) -> Option<<Self::Protocol as ConsumerProtocol>::Annotation>;
}

impl PieceAnnotation for () {
    type Protocol = NoConsumer;
    fn annotate(&self, _: &[CapturedTransaction<'_>]) -> Option<()> {
        None
    }
}

/// The runtime supplies session routing. The sender holds no transaction or
/// writer guard while this network request is pending.
pub trait ReplicationTransport<P: ConsumerProtocol>: Send + Sync + 'static {
    fn send_piece(
        &self,
        copy: ReceiverId,
        piece: Piece<P::Annotation>,
    ) -> impl Future<Output = Result<ReplicationAnswer, SendFailure>> + Send;
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SendFailure {
    #[error("copy is unavailable: {message}")]
    Unavailable { message: String },
    #[error("piece answer was lost")]
    AnswerLost,
}

#[derive(Clone, Copy, Debug)]
pub enum Retention {
    UntilAcknowledged,
    Bounded { max_age: Duration, max_changes: u64 },
}

#[derive(Clone, Copy, Debug)]
pub struct SenderOptions {
    pub max_part_items: NonZeroUsize,
    pub retry_after: Duration,
    pub retention: Retention,
}

impl Default for SenderOptions {
    fn default() -> Self {
        Self {
            max_part_items: NonZeroUsize::MIN.saturating_add(999),
            retry_after: Duration::from_millis(100),
            retention: Retention::UntilAcknowledged,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RejectedTail {
    pub database: DatabaseId,
    pub epoch: Epoch,
    pub ended_at: ReplicationPosition,
    pub last_commit: ReplicationPosition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicationEvents {
    Acknowledged {
        copy: ReceiverId,
        through: ReplicationPosition,
    },
    CopyNeedsSnapshot {
        copy: ReceiverId,
    },
    RejectedTail(RejectedTail),
    LeaseChanged(fleet_sync_lease::WriterState),
}

#[derive(Debug, thiserror::Error)]
pub enum SenderError {
    #[error(transparent)]
    UnitTooLarge(#[from] fleet_sync_protocol::unit_budget::UnitTooLarge),
    #[error("copy {copy:?} is unavailable: {reason:?}")]
    CopyUnavailable {
        copy: ReceiverId,
        reason: fleet_sync_protocol::CopyUnavailableReason,
    },
    #[error("copy {copy:?} refused replication: {reason:?}")]
    Refused {
        copy: ReceiverId,
        reason: fleet_sync_protocol::PieceRefusal,
    },
    #[error(transparent)]
    Storage(#[from] OpenError),
    #[error(transparent)]
    Bookkeeping(#[from] BookkeepingError),
    #[error(transparent)]
    Piece(#[from] fleet_sync_protocol::PieceError),
    #[error(transparent)]
    Send(#[from] SendFailure),
    #[error("sender state is invalid: {field}")]
    Invalid { field: &'static str },
}

impl From<sqlx_core::Error> for SenderError {
    fn from(error: sqlx_core::Error) -> Self {
        Self::Storage(error.into())
    }
}
