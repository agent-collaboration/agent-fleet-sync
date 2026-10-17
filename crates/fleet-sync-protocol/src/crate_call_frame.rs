use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields,
    bound = ""
)]
pub enum ServerCall<D: DeliveryTypes = NoDelivery> {
    WithdrawRelease {
        call: CallId,
        request: WithdrawRelease,
    },
    Release {
        call: CallId,
        request: ReleaseRequest,
    },
    PublishStatus {
        call: CallId,
        publication: StatusPublication<D::Acceptance>,
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
    WithdrawRelease(WithdrawAnswer),
    Release(ReleaseAnswer),
    Published(StatusPublicationResult),
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
    Crate(ServerCall<P::Delivery>),
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
