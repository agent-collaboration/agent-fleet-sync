use crate::StoreError;
use durable_steps::{CallId, storage::TransactionScope};
use fleet_sync_protocol::{
    ConsumerProtocol, DatabaseId, Epoch, FailureCode, HookRefusal, PeerId, PieceRefusal,
    ReplicatedTransaction, ReplicationPosition, TransactionKey,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::future::Future;

pub struct AppliedPiece<'piece, A> {
    pub database: DatabaseId,
    pub epoch: Epoch,
    pub through: ReplicationPosition,
    pub transactions: &'piece [ReplicatedTransaction],
    pub annotation: Option<&'piece A>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledSnapshot {
    pub database: DatabaseId,
    pub position: ReplicationPosition,
    pub schema: fleet_sync_protocol::SchemaIdentity,
}

/// Supplied by the authenticated host, never decoded from a replication frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SeedAuthority {
    Writer(PeerId),
    Admin,
}

#[derive(Debug)]
pub enum SeedDecision<TFollowUp> {
    Published {
        position: ReplicationPosition,
        follow_ups: Vec<TFollowUp>,
    },
    Answer(fleet_sync_protocol::SeedAnswer),
}

/// A consumer policy refusal is terminal; a storage failure keeps its storage class.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HookError {
    #[error("receiver rule refused: {0:?}")]
    Refused(HookRefusal),
    #[error(transparent)]
    Storage(#[from] StoreError),
}

impl From<HookRefusal> for HookError {
    fn from(refusal: HookRefusal) -> Self {
        Self::Refused(refusal)
    }
}

/// Transaction-time consumer rules return follow-up items for post-commit delivery.
pub trait ReceiverRules<S: TransactionScope>: Send + Sync + 'static {
    type Protocol: ConsumerProtocol;
    type FollowUp: Serialize + DeserializeOwned + Send + Sync;
    /// Local tables initialized by the host before any piece is applied.
    fn local_migrations(&self) -> &'static [sqlx_core::migrate::Migration] {
        &[]
    }
    /// Rebuild derived local state after snapshot rows are installed in this transaction.
    fn on_snapshot_installed(
        &self,
        _scope: &mut S,
        _installed: &InstalledSnapshot,
    ) -> impl Future<Output = Result<Vec<Self::FollowUp>, HookError>> + Send {
        async { Ok(Vec::new()) }
    }
    fn on_applied(
        &self,
        scope: &mut S,
        applied: &AppliedPiece<'_, <Self::Protocol as ConsumerProtocol>::Annotation>,
    ) -> impl Future<Output = Result<Vec<Self::FollowUp>, HookError>> + Send;
}

/// The host composes this handler with rules producing exactly its item type.
pub trait FollowUpHandler: Send + Sync + 'static {
    type Item: Serialize + DeserializeOwned + Send + Sync;
    fn follow_up(
        &self,
        item: &Self::Item,
        call: CallId,
    ) -> impl Future<Output = Result<FollowUpDone, FollowUpFailure>> + Send;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FollowUpDone;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum FollowUpAnswer {
    Done,
    Failed { reason: FailureCode },
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum FollowUpFailure {
    /// Dispatch as CallFailure::AnswerLost: the hook may have had effects. The
    /// engine retries the same CallId with its bounded backoff; no answer is saved.
    #[error("follow-up should retry")]
    Retry,
    #[error("follow-up failed: {reason:?}")]
    Failed { reason: FailureCode },
}

#[derive(Debug)]
pub enum ApplyDecision<TFollowUp> {
    Applied {
        through: ReplicationPosition,
        follow_ups: Vec<TFollowUp>,
    },
    PartStaged {
        transaction: TransactionKey,
        index: u32,
        acked: ReplicationPosition,
    },
    AlreadyApplied {
        through: ReplicationPosition,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ApplyFailure {
    #[error("piece refused: {0:?}")]
    Refused(PieceRefusal),
    #[error("piece refused and staged transaction must be discarded: {refusal:?}")]
    RefusedDiscardingParts {
        refusal: PieceRefusal,
        transaction: TransactionKey,
    },
    #[error(transparent)]
    Storage(StoreError),
}

impl ApplyFailure {
    pub(crate) fn from_storage(error: StoreError) -> Self {
        match error {
            StoreError::Full => Self::Refused(PieceRefusal::CopyFull),
            error => Self::Storage(error),
        }
    }
}

impl From<StoreError> for ApplyFailure {
    fn from(error: StoreError) -> Self {
        Self::from_storage(error)
    }
}

impl From<ReceiverError> for ApplyFailure {
    fn from(error: ReceiverError) -> Self {
        match error {
            ReceiverError::Storage(error) => Self::from_storage(error),
            ReceiverError::NotInitialized => Self::Refused(PieceRefusal::NotInitialized),
            ReceiverError::Retired => Self::Refused(PieceRefusal::Retired),
            ReceiverError::NotWriter { current } => {
                Self::Refused(PieceRefusal::NotWriter { current })
            }
            ReceiverError::InvalidInput { field } => {
                Self::Storage(StoreError::Constraint { message: field })
            }
            ReceiverError::FenceConflict => Self::Storage(StoreError::Corrupt {
                field: "unexpected fence conflict during apply".into(),
            }),
            ReceiverError::SeedRefused { .. } => Self::Storage(StoreError::Corrupt {
                field: "unexpected seed refusal during apply".into(),
            }),
            ReceiverError::SchemaUnsupported { form } => {
                Self::Refused(PieceRefusal::SchemaUnsupported { form })
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ReceiverError {
    #[error("unsupported replicated schema: {form:?}")]
    SchemaUnsupported {
        form: fleet_sync_protocol::SchemaStatementForm,
    },
    #[error(transparent)]
    Storage(#[from] StoreError),
    #[error("invalid receiver input: {field}")]
    InvalidInput { field: String },
    #[error("copy is not initialized")]
    NotInitialized,
    #[error("copy is retired")]
    Retired,
    /// Host dispatch answers any unauthorized replication request, including a
    /// seed request, with ReplicationAnswer::Refused(PieceRefusal::NotWriter).
    #[error("seed authority is not this database's writer; current epoch: {current:?}")]
    NotWriter { current: Option<Epoch> },
    #[error("fence identity has different content")]
    FenceConflict,
    #[error("seed publication refused: {answer:?}; host must roll back")]
    SeedRefused {
        answer: fleet_sync_protocol::SeedAnswer,
    },
}
