use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeliveryStream {
    Items,
    Controls,
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields,
    bound = ""
)]
pub enum DeliveryRequest<D: DeliveryTypes = NoDelivery> {
    Submit {
        call: CallId,
        item: DeliveryEnvelope<D>,
    },
    Report {
        call: CallId,
        report: RecipientReport<D>,
    },
    StatusFetch {
        call: CallId,
        after: StatusCursor,
        limit: u32,
    },
    StatusAck {
        call: CallId,
        through: StatusCursor,
    },
    Query {
        call: CallId,
        mailbox: PeerId,
        items: Vec<ItemId>,
    },
    Settle {
        call: CallId,
        mailbox: PeerId,
        /// Independent owner claim; the host authorizes it before settlement.
        submitter: Submitter,
        item: ItemId,
        digest: ItemDigest,
    },
}
impl<D: DeliveryTypes> DeliveryRequest<D> {
    pub fn call(&self) -> &CallId {
        match self {
            Self::Submit { call, .. }
            | Self::Report { call, .. }
            | Self::StatusFetch { call, .. }
            | Self::StatusAck { call, .. }
            | Self::Query { call, .. }
            | Self::Settle { call, .. } => call,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields,
    bound = ""
)]
pub enum DeliveryAnswer<D: DeliveryTypes = NoDelivery> {
    /// The owner is unavailable without deciding any item's outcome.
    /// Durable dispatchers retry the same call rather than saving an item refusal.
    DeliveryUnavailable {
        reason: DeliveryRefusal,
    },
    Submit(SubmitResult<D::Acceptance>),
    Report(ReportResult),
    StatusFetch(StatusFetchResult<D::Acceptance>),
    StatusAck(StatusAckResult),
    Query(QueryResult<D::Acceptance>),
    Settle(SettleResult<D::Acceptance>),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum SubmitResult<A> {
    BindingUnavailable {
        reason: SenderBindingFailure,
    },
    /// The relay's canonical snapshot at first admission, including an already expired item.
    Accepted {
        status: DeliveryStatus<A>,
        version: StatusVersion,
        cancel: Option<CancelResult>,
    },
    Replayed {
        status: DeliveryStatus<A>,
        version: StatusVersion,
        cancel: Option<CancelResult>,
    },
    /// A different cancel is canonical; this submission took no relay custody.
    CancelAlreadyRequested {
        existing: DeliveryItemRef,
        status: DeliveryStatus<A>,
        version: StatusVersion,
        cancel: Option<CancelResult>,
    },
    Refused {
        reason: DeliveryRefusal,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, bound = "")]
pub struct RecipientReport<D: DeliveryTypes> {
    pub item: ItemId,
    pub report_seq: ReportSeq,
    pub outcome: RecipientOutcome<D::Acceptance>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[serde(tag = "kind", content = "value", rename_all_fields = "camelCase")]
pub enum ReportResult {
    Ack { report_seq: ReportSeq },
    Replayed { report_seq: ReportSeq },
    Refused { reason: DeliveryRefusal },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StatusPublication<A = ()> {
    pub item: ItemId,
    pub status: DeliveryStatus<A>,
    pub version: StatusVersion,
    pub cancel: Option<CancelResult>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StatusEvent<A> {
    pub cursor: StatusCursor,
    pub item: ItemId,
    pub status: DeliveryStatus<A>,
    pub version: StatusVersion,
    pub cancel: Option<CancelResult>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StatusFetchResult<A> {
    pub events: Vec<StatusEvent<A>>,
    pub through: StatusCursor,
}

/// The remote retirement floor never advances a consumer's applied cursor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum StatusAckResult {
    Acknowledged {
        floor: StatusCursor,
    },
    /// Diagnostic only: unread events and the acknowledged floor are unchanged.
    Refused {
        head: StatusCursor,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ItemStatusState<A> {
    Unknown,
    Known {
        digest: ItemDigest,
        status: DeliveryStatus<A>,
        version: StatusVersion,
        cancel: Option<CancelResult>,
    },
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ItemStatus<A> {
    pub item: ItemId,
    pub state: ItemStatusState<A>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryResult<A> {
    pub statuses: Vec<ItemStatus<A>>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum SettleResult<A> {
    Settled {
        reason: DeliveryFailureKind,
        version: StatusVersion,
    },
    Replayed {
        status: DeliveryStatus<A>,
        version: StatusVersion,
        cancel: Option<CancelResult>,
    },
    ItemConflict {
        existing: ItemDigest,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum StatusPublicationResult {
    Applied,
    Refused { reason: DeliveryRefusal },
}

#[cfg(test)]
#[path = "delivery_settle_test.rs"]
mod tests;

#[cfg(test)]
#[path = "delivery_cutover_test.rs"]
mod cutover_tests;
