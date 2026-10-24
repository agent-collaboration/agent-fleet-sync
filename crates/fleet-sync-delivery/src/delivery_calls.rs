use crate::*;
use durable_steps::CallTarget;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, bound = "")]
pub struct SubmitCall<D> {
    pub relay: PeerId,
    pub mailbox: PeerId,

    #[serde(skip)]
    payload: PhantomData<fn() -> D>,
}
impl<D> SubmitCall<D> {
    pub const fn new(relay: PeerId, mailbox: PeerId) -> Self {
        Self {
            relay,
            mailbox,

            payload: PhantomData,
        }
    }
}
impl<D: DeliveryTypes> CallTarget for SubmitCall<D> {
    const KIND: &'static str = "fleet-sync.delivery.submit";
    type Request = DeliveryItemRef;
    type Answer = SubmitOutcome<D::Acceptance>;
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, bound = "")]
pub struct DeliverCall<D> {
    #[serde(skip)]
    payload: PhantomData<fn() -> D>,
}
impl<D> DeliverCall<D> {
    pub const fn new() -> Self {
        Self {
            payload: PhantomData,
        }
    }
}
impl<D> Default for DeliverCall<D> {
    fn default() -> Self {
        Self::new()
    }
}
impl<D: DeliveryTypes> CallTarget for DeliverCall<D> {
    const KIND: &'static str = "fleet-sync.delivery.deliver";
    const DEDUPLICATES: bool = false;
    type Request = DeliveryItemRef;
    type Answer = LocalDeliveryOutcome<D>;
}
impl<D: DeliveryTypes> DeliverCall<D> {
    /// Legacy hosts validate their result before returning it for journaling.
    /// Opted-in hosts use `Inbox::resolve_local_delivery` and the resolved value's
    /// validator; an unbound Delivered answer cannot establish their receipt.
    pub fn validated_answer(delivery: LocalDelivery<D>) -> LocalDeliveryOutcome<D> {
        if D::PER_ITEM_SETTLEMENT && matches!(delivery.outcome, LocalOutcome::Delivered { .. }) {
            return DispatchOutcome::Answered(LocalDeliveryAnswer::Uncertain);
        }
        Self::validated_answer_with_bound(delivery, D::MAX_ACCEPTANCE_BYTES)
    }
    pub(crate) fn validated_answer_with_bound(
        delivery: LocalDelivery<D>,
        maximum: u32,
    ) -> LocalDeliveryOutcome<D> {
        let answer = if crate::delivery_sizes::check_local_outcome::<D>(&delivery.outcome, maximum)
            .is_ok()
            && crate::delivery_sizes::canonical_size(&delivery.observation)
                .is_ok_and(|bytes| bytes <= u64::from(D::MAX_LOCAL_OBSERVATION_BYTES))
        {
            LocalDeliveryAnswer::Observed {
                outcome: delivery.outcome,
                observation: delivery.observation,
            }
        } else {
            LocalDeliveryAnswer::Uncertain
        };
        DispatchOutcome::Answered(answer)
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FetchInput {
    pub stream: DeliveryStream,
    pub after: DeliverySeq,
    pub max_items: u32,
    pub max_bytes: u64,
    /// Absent preserves the legacy payload-only fetch budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox_room_bytes: Option<u64>,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReceiptInput {
    pub stream: DeliveryStream,
    pub through: DeliverySeq,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, bound = "")]
pub struct FetchCall<D> {
    pub relay: PeerId,
    pub mailbox: PeerId,
    #[serde(skip)]
    payload: PhantomData<fn() -> D>,
}
impl<D> FetchCall<D> {
    pub const fn new(relay: PeerId, mailbox: PeerId) -> Self {
        Self {
            relay,
            mailbox,
            payload: PhantomData,
        }
    }
}
impl<D: DeliveryTypes> CallTarget for FetchCall<D> {
    const KIND: &'static str = "fleet-sync.delivery.fetch";
    type Request = FetchInput;
    type Answer = FetchResult<D>;
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReceivedCall {
    pub relay: PeerId,
    pub mailbox: PeerId,
}
impl CallTarget for ReceivedCall {
    const KIND: &'static str = "fleet-sync.delivery.received";
    type Request = ReceiptInput;
    type Answer = ReceivedResult;
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, bound = "")]
pub struct ReportCall<D> {
    pub relay: PeerId,
    pub mailbox: PeerId,
    #[serde(skip)]
    payload: PhantomData<fn() -> D>,
}
impl<D> ReportCall<D> {
    pub const fn new(relay: PeerId, mailbox: PeerId) -> Self {
        Self {
            relay,
            mailbox,
            payload: PhantomData,
        }
    }
}
impl<D: DeliveryTypes> CallTarget for ReportCall<D> {
    const KIND: &'static str = "fleet-sync.delivery.report";
    type Request = RecipientReport<D>;
    type Answer = ReportResult<D::Acceptance>;
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, bound = "")]
pub struct PublishCall<D: DeliveryTypes> {
    pub target: PublicationTarget<D::StatusRoute>,
    #[serde(skip)]
    payload: PhantomData<fn() -> D>,
}
impl<D: DeliveryTypes> PublishCall<D> {
    pub const fn new(target: PublicationTarget<D::StatusRoute>) -> Self {
        Self {
            target,
            payload: PhantomData,
        }
    }
}
impl<D: DeliveryTypes> CallTarget for PublishCall<D> {
    const KIND: &'static str = "fleet-sync.delivery.publish";
    type Request = StatusPublication<D::Acceptance>;
    type Answer = StatusPublicationResult;
}
