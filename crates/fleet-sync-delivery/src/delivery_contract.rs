use crate::DeliveryLimits;
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};
use std::future::Future;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum GateDecision {
    Release,
    Hold { until: Option<SyncTime> },
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum LocalOutcome<A> {
    Held { until: Option<SyncTime> },
    Delivered { acceptance: A },
    Uncertain,
    Expired,
    Dropped,
    Refused { reason: DeliveryRefusal },
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum DispatchOutcome<A> {
    Answered(A),
    Unresolvable { reason: DeliveryFailureKind },
}
pub type SubmitOutcome<A> = DispatchOutcome<SubmitResult<A>>;
pub type LocalDeliveryOutcome<A> = DispatchOutcome<LocalOutcome<A>>;

#[derive(Debug, thiserror::Error)]
pub enum DeliveryError {
    #[error("delivery storage: {0}")]
    Storage(String),
    #[error(transparent)]
    Config(#[from] crate::DeliveryConfigError),
    #[error("delivery refused: {reason:?}")]
    Refused { reason: DeliveryRefusal },
    #[error("invalid stored delivery field {field}")]
    Corrupt { field: &'static str },
    #[error("delivery encoding: {0}")]
    Encoding(String),
    #[error("delivery sequence exhausted")]
    SequenceExhausted,
    #[error("call identity was reused with other content")]
    CallConflict,
    #[error("delivery batch does not continue the requested stream")]
    InvalidBatch,
    #[error("inbox has no room for the fetched batch yet")]
    InboxFull,
    #[error("custody row or payload is missing")]
    CustodyMissing,
    #[error("custody digest differs from its call reference")]
    DigestMismatch,
    #[error("delivery processing failed: {}",kind.as_str())]
    ProcessingFailed { kind: DeliveryFailureKind },
}

impl DeliveryError {
    pub fn failure_kind(&self) -> DeliveryFailureKind {
        match self {
            Self::ProcessingFailed { kind } => *kind,
            Self::CustodyMissing => DeliveryFailureKind::CustodyMissing,
            Self::DigestMismatch => DeliveryFailureKind::DigestMismatch,
            Self::Corrupt { .. } => DeliveryFailureKind::CustodyCorrupt,
            _ => DeliveryFailureKind::ExecutionFailed,
        }
    }
    pub fn failure_code(&self) -> FailureCode {
        self.failure_kind().into()
    }
}

pub trait DeliveryConsumer<S: TransactionScope>: Send + Sync + 'static {
    type Protocol: ConsumerProtocol;
    fn release_gate(
        &self,
        scope: &mut S,
        item: &DeliveryItem<<Self::Protocol as ConsumerProtocol>::Delivery>,
    ) -> impl Future<Output = Result<GateDecision, DeliveryError>> + Send;
    fn deliver_locally(
        &self,
        item: &DeliveryItem<<Self::Protocol as ConsumerProtocol>::Delivery>,
        call: CallId,
    ) -> impl Future<
        Output = LocalOutcome<
            <<Self::Protocol as ConsumerProtocol>::Delivery as DeliveryTypes>::Acceptance,
        >,
    > + Send;
    fn may_cancel(
        &self,
        requester: &<<Self::Protocol as ConsumerProtocol>::Delivery as DeliveryTypes>::Sender,
        item: &DeliveryHeader<<Self::Protocol as ConsumerProtocol>::Delivery>,
    ) -> bool;
}

#[derive(Clone, Copy)]
pub struct DeliveryTaskConfig {
    pub owner: PeerId,
    pub relay: PeerId,
    pub limits: DeliveryLimits,
    pub fetch_items: u32,
    pub fetch_bytes: u64,
}

/// Only the relay's atomic settlement can establish an unknown item's final failure.
pub fn submit_settlement_outcome<A>(result: SettleResult<A>) -> SubmitOutcome<A> {
    match result {
        SettleResult::Settled { reason, .. } => DispatchOutcome::Unresolvable { reason },
        SettleResult::Replayed {
            status,
            version,
            cancel,
        } => DispatchOutcome::Answered(SubmitResult::Replayed {
            status,
            version,
            cancel,
        }),
        SettleResult::ItemConflict { existing } => {
            DispatchOutcome::Answered(SubmitResult::Refused {
                reason: DeliveryRefusal::ItemConflict { existing },
            })
        }
    }
}

impl From<durable_steps::StoreError> for DeliveryError {
    fn from(error: durable_steps::StoreError) -> Self {
        Self::Storage(error.to_string())
    }
}
