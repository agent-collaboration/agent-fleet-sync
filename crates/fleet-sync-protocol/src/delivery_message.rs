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
    Fetch {
        call: CallId,
        stream: DeliveryStream,
        after: DeliverySeq,
        max_items: u32,
        max_bytes: u64,
    },
    Received {
        call: CallId,
        stream: DeliveryStream,
        through: DeliverySeq,
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
        item: ItemId,
        digest: ItemDigest,
    },
    Retire {
        call: CallId,
        mailbox: PeerId,
        items: Vec<ItemId>,
    },
}
impl<D: DeliveryTypes> DeliveryRequest<D> {
    pub fn call(&self) -> &CallId {
        match self {
            Self::Submit { call, .. }
            | Self::Fetch { call, .. }
            | Self::Received { call, .. }
            | Self::Report { call, .. }
            | Self::StatusFetch { call, .. }
            | Self::StatusAck { call, .. }
            | Self::Query { call, .. }
            | Self::Retire { call, .. }
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
    MailboxUnavailable {
        reason: DeliveryRefusal,
    },
    Submit(SubmitResult<D::Acceptance>),
    Fetch(FetchResult<D>),
    Received(ReceivedResult),
    Report(ReportResult<D::Acceptance>),
    StatusFetch(StatusFetchResult<D::Acceptance>),
    StatusAck(StatusAckResult),
    Query(QueryResult<D::Acceptance>),
    Retire(RetireResult),
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
    /// The relay's canonical snapshot at first admission, including an already expired item.
    Accepted {
        seq: DeliverySeq,
        status: DeliveryStatus<A>,
        version: StatusVersion,
        cancel: Option<CancelResult>,
    },
    Replayed {
        status: DeliveryStatus<A>,
        version: StatusVersion,
        cancel: Option<CancelResult>,
    },
    Refused {
        reason: DeliveryRefusal,
    },
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
pub enum FetchEntry<D: DeliveryTypes> {
    Item {
        seq: DeliverySeq,
        item: DeliveryEnvelope<D>,
    },
    Retired {
        seq: DeliverySeq,
        items: Vec<ItemId>,
    },
}
impl<D: DeliveryTypes> FetchEntry<D> {
    pub const fn sequence(&self) -> DeliverySeq {
        match self {
            Self::Item { seq, .. } | Self::Retired { seq, .. } => *seq,
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, bound = "")]
pub struct FetchResult<D: DeliveryTypes> {
    pub stream: DeliveryStream,
    pub entries: Vec<FetchEntry<D>>,
    pub through: DeliverySeq,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReceivedResult {
    pub stream: DeliveryStream,
    pub released_through: DeliverySeq,
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
pub enum ReportResult<A> {
    Applied {
        item: ItemId,
        acknowledged: ReportSeq,
        status: DeliveryStatus<A>,
        version: StatusVersion,
    },
    Refused {
        reason: DeliveryRefusal,
    },
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetireResult {
    pub retired: Vec<ItemId>,
    pub unknown: Vec<ItemId>,
    pub not_current_target: Vec<ItemId>,
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
