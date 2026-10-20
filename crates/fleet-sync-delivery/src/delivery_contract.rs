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
/// The consumer's result, before the local call adapter enforces receipt bounds.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, bound = "")]
pub struct LocalDelivery<D: DeliveryTypes> {
    pub outcome: LocalOutcome<D::Acceptance>,
    pub observation: D::LocalObservation,
}

/// A validated local answer journals either the complete observation or an
/// uncertain outcome when an invalid receipt cannot establish what happened.
#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields,
    bound = ""
)]
pub enum LocalDeliveryAnswer<D: DeliveryTypes> {
    Observed {
        outcome: LocalOutcome<D::Acceptance>,
        observation: D::LocalObservation,
    },
    Uncertain,
}
pub type LocalDeliveryOutcome<D> = DispatchOutcome<LocalDeliveryAnswer<D>>;

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
    /// Project a newly received item using the custody transaction. Returning an
    /// error aborts that transaction; replays do not call this hook.
    fn on_received(
        &self,
        _scope: &mut S,
        _item: &DeliveryItem<<Self::Protocol as ConsumerProtocol>::Delivery>,
    ) -> impl Future<Output = Result<(), DeliveryError>> + Send {
        async { Ok(()) }
    }
    /// Project each recorded inbox outcome using its custody transaction,
    /// including held attempts and cancellation controls. Returning an error
    /// aborts that transaction. The ID remains usable for corrupt-custody failures.
    /// The observation is present only for a validated local-attempt answer;
    /// receive-time outcomes, expiry checks, failures and cancellations carry none.
    fn on_outcome(
        &self,
        _scope: &mut S,
        _item: ItemId,
        _outcome: &RecipientOutcome<
            <<Self::Protocol as ConsumerProtocol>::Delivery as DeliveryTypes>::Acceptance,
        >,
        _observation: Option<
            &<<Self::Protocol as ConsumerProtocol>::Delivery as DeliveryTypes>::LocalObservation,
        >,
    ) -> impl Future<Output = Result<(), DeliveryError>> + Send {
        async { Ok(()) }
    }
    /// Project a control's outcome with its target in the custody transaction,
    /// immediately after on_outcome. Called once when the control settles, on
    /// Local and remote routes; replays do not call it. An error aborts the
    /// transaction, including both outcome projections.
    fn on_cancel_outcome(
        &self,
        _scope: &mut S,
        _cancel: ItemId,
        _target: ItemId,
        _outcome: &RecipientOutcome<
            <<Self::Protocol as ConsumerProtocol>::Delivery as DeliveryTypes>::Acceptance,
        >,
    ) -> impl Future<Output = Result<(), DeliveryError>> + Send {
        async { Ok(()) }
    }
    fn release_gate(
        &self,
        scope: &mut S,
        item: &DeliveryItem<<Self::Protocol as ConsumerProtocol>::Delivery>,
    ) -> impl Future<Output = Result<GateDecision, DeliveryError>> + Send;
    fn deliver_locally(
        &self,
        item: &DeliveryItem<<Self::Protocol as ConsumerProtocol>::Delivery>,
        call: CallId,
    ) -> impl Future<Output = LocalDelivery<<Self::Protocol as ConsumerProtocol>::Delivery>> + Send;
    fn may_cancel(
        &self,
        requester: &<<Self::Protocol as ConsumerProtocol>::Delivery as DeliveryTypes>::Sender,
        item: &DeliveryHeader<<Self::Protocol as ConsumerProtocol>::Delivery>,
    ) -> bool;
}

/// Recipient custody and local execution do not depend on relay enrollment.
#[derive(Clone, Copy)]
pub struct LocalDeliveryConfig {
    pub owner: PeerId,
    pub limits: DeliveryLimits,
}

/// Relay-facing tasks add transport settings to the recipient's local settings.
#[derive(Clone, Copy)]
pub struct DeliveryTaskConfig {
    pub local: LocalDeliveryConfig,
    pub relay: PeerId,
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
