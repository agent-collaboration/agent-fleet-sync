use durable_steps::{CallId, OperationFailure};
use fleet_sync_protocol::{DatabaseId, GateAnswer, HolderChangeRequest, PeerId, SettleOutcome};
use std::future::Future;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LeaseError {
    #[error("registry storage: {message}")]
    Storage { message: String },
    #[error("invalid persisted registry field {field}")]
    Corrupt { field: String },
    #[error("invalid registry input: {field}")]
    InvalidInput { field: String },
    #[error("database is retired")]
    Retired,
    #[error("database is not registered or imported")]
    NotRegistered,
    #[error("authoritative copy is not initialized")]
    NoAuthoritativeCopy,
    #[error("takeover preview changed while it was being read")]
    PreviewStale,
    #[error("database has no current holder")]
    NotHeld,
    #[error(transparent)]
    Operation(#[from] OperationFailure),
}
impl From<sqlx_core::Error> for LeaseError {
    fn from(error: sqlx_core::Error) -> Self {
        Self::Storage {
            message: error.to_string(),
        }
    }
}
impl From<durable_steps::StepError> for LeaseError {
    fn from(error: durable_steps::StepError) -> Self {
        Self::Operation(error.into())
    }
}
impl From<LeaseError> for OperationFailure {
    fn from(error: LeaseError) -> Self {
        match error {
            LeaseError::Operation(failure) => failure,
            LeaseError::Storage { message } => Self::Storage {
                error: durable_steps::StoreError::Backend { message },
            },
            error => Self::Refused {
                code: "registry_refused".into(),
                detail: error.to_string(),
            },
        }
    }
}

/// A transient gate failure. Permanent policy refusals are GateAnswer::Refused.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("gate unavailable: {detail}")]
pub struct GateFailure {
    pub detail: String,
}
pub trait LifecycleGate: Send + Sync + 'static {
    fn admit_registration(
        &self,
        database: &DatabaseId,
        writer: &PeerId,
        call: CallId,
    ) -> impl Future<Output = Result<GateAnswer, GateFailure>> + Send;
    fn admit_retirement(
        &self,
        database: &DatabaseId,
        call: CallId,
    ) -> impl Future<Output = Result<GateAnswer, GateFailure>> + Send;
}
pub trait HolderChangeGate: Send + Sync + 'static {
    fn reserve(
        &self,
        change: &HolderChangeRequest,
        call: CallId,
    ) -> impl Future<Output = Result<GateAnswer, GateFailure>> + Send;
    fn settle(
        &self,
        change: &HolderChangeRequest,
        outcome: SettleOutcome,
        call: CallId,
    ) -> impl Future<Output = Result<(), GateFailure>> + Send;
}
