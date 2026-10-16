use crate::StoreError;
use durable_steps::{CallId, storage::TransactionScope};
use fleet_sync_protocol::{
    ConsumerProtocol, DatabaseId, Epoch, FailureCode, HookRefusal, PieceRefusal,
    ReplicatedTransaction, ReplicationPosition, TransactionKey,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{future::Future, time::Duration};

pub struct AppliedPiece<'piece, A> {
    pub database: DatabaseId,
    pub epoch: Epoch,
    pub through: ReplicationPosition,
    pub transactions: &'piece [ReplicatedTransaction],
    pub annotation: Option<&'piece A>,
}

pub trait ReceiverHook<S: TransactionScope>: Send + Sync + 'static {
    type Protocol: ConsumerProtocol;
    type FollowUp: Serialize + DeserializeOwned + Send + Sync;
    fn on_applied(
        &self,
        scope: &mut S,
        applied: &AppliedPiece<'_, <Self::Protocol as ConsumerProtocol>::Annotation>,
    ) -> impl Future<Output = Result<Vec<Self::FollowUp>, HookRefusal>> + Send;
    fn follow_up(
        &self,
        item: &Self::FollowUp,
        call: CallId,
    ) -> impl Future<Output = Result<FollowUpDone, FollowUpFailure>> + Send;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FollowUpDone;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum FollowUpFailure {
    #[error("follow-up should retry after {after:?}")]
    Retry { after: Duration },
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
            ReceiverError::InvalidInput { field } => {
                Self::Storage(StoreError::Constraint { message: field })
            }
            ReceiverError::FenceConflict => Self::Storage(StoreError::Corrupt {
                field: "unexpected fence conflict during apply".into(),
            }),
            ReceiverError::SeedRefused { .. } => Self::Storage(StoreError::Corrupt {
                field: "unexpected seed refusal during apply".into(),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ReceiverError {
    #[error(transparent)]
    Storage(#[from] StoreError),
    #[error("invalid receiver input: {field}")]
    InvalidInput { field: String },
    #[error("copy is not initialized")]
    NotInitialized,
    #[error("copy is retired")]
    Retired,
    #[error("fence identity has different content")]
    FenceConflict,
    #[error("seed publication refused: {answer:?}; host must roll back")]
    SeedRefused {
        answer: fleet_sync_protocol::SeedAnswer,
    },
}
