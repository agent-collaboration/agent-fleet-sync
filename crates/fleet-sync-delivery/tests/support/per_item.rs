#![allow(dead_code, clippy::unwrap_used)]
use crate::support::*;
use fleet_sync_delivery::*;
pub struct VariableTypes;
impl DeliveryTypes for VariableTypes {
    const MAX_SENDER_BYTES: u32 = 64;
    const MAX_ADDRESS_BYTES: u32 = 64;
    const MAX_ACCEPTANCE_BYTES: u32 = 1_049_600;
    const PER_ITEM_SETTLEMENT: bool = true;
    const MAX_STATUS_ROUTE_BYTES: u32 = 64;
    const MAX_LOCAL_OBSERVATION_BYTES: u32 = 4;
    type Body = String;
    type Sender = String;
    type Address = String;
    type Acceptance = Acceptance;
    type LocalObservation = ();
    type StatusRoute = String;
    fn max_acceptance_bytes_for(body: &String) -> u32 {
        match body.as_str() {
            "large" => Self::MAX_ACCEPTANCE_BYTES,
            "invalid" => Self::MAX_ACCEPTANCE_BYTES + 1,
            _ => 64,
        }
    }
}
pub fn variable_item(identity: u64, body: &str) -> DeliveryItem<VariableTypes> {
    DeliveryItem::new(
        item_id(identity),
        "sender".into(),
        Recipient {
            peer: peer(2),
            address: "inbox".into(),
        },
        None,
        body.into(),
    )
    .unwrap()
}
pub fn variable_limits(bytes: u64) -> DeliveryLimits {
    DeliveryLimits::new::<VariableTypes>(
        bytes,
        20,
        bytes,
        20,
        bytes,
        minimum_settlement_allowance::<VariableTypes>().unwrap(),
    )
    .unwrap()
}
pub fn now() -> SyncTime {
    SyncTime::from_unix_millis(0)
}

pub struct VariableProtocol;
impl ConsumerProtocol for VariableProtocol {
    type Delivery = VariableTypes;
    type Request = ();
    type Answer = ();
    type ServerRequest = ();
    type ClientAnswer = ();
    type Notice = ();
    type Annotation = ();
}
pub struct VariableConsumer;
impl<S: durable_steps::storage::TransactionScope> DeliveryConsumer<S> for VariableConsumer {
    type Protocol = VariableProtocol;
    async fn release_gate(
        &self,
        _scope: &mut S,
        _item: &DeliveryItem<VariableTypes>,
    ) -> Result<GateDecision, DeliveryError> {
        Ok(GateDecision::Release)
    }
    async fn deliver_locally(
        &self,
        _item: &DeliveryItem<VariableTypes>,
        _call: durable_steps::CallId,
    ) -> LocalDelivery<VariableTypes> {
        LocalDelivery {
            outcome: LocalOutcome::Delivered {
                acceptance: Acceptance {
                    receipt: "ok".into(),
                },
            },
            observation: (),
        }
    }
    fn may_cancel(&self, requester: &String, item: &DeliveryHeader<VariableTypes>) -> bool {
        requester == item.sender()
    }
}
