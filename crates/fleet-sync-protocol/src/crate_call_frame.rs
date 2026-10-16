use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeliveryStream {
    Items,
    Controls,
}

/// Routing shells for the delivery milestone. Delivery owns their body types;
/// session code does not invent payloads or acknowledge these operations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DeliveryRequest {
    Submit,
    Fetch {
        stream: DeliveryStream,
        after: u64,
    },
    Received {
        stream: DeliveryStream,
        through: u64,
    },
    Report,
    StatusFetch {
        after: u64,
    },
    Query,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum DeliveryAnswer {
    Submit,
    Fetch,
    Received,
    Report,
    StatusFetch,
    Query,
}

/// Reserved for the delivery-owned publication body.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StatusPublication;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ServerCall {
    Release {
        call: CallId,
        request: ReleaseRequest,
    },
    PublishStatus {
        call: CallId,
        publication: StatusPublication,
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
pub enum ServerCallAnswer {
    Release(ReleaseAnswer),
    PublishStatus,
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields,
    bound = ""
)]
pub enum ServerRequest<P: ConsumerProtocol = NoConsumer> {
    Crate(ServerCall),
    Consumer(P::ServerRequest),
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields,
    bound = ""
)]
pub enum ClientAnswer<P: ConsumerProtocol = NoConsumer> {
    Crate(ServerCallAnswer),
    Consumer(P::ClientAnswer),
}
