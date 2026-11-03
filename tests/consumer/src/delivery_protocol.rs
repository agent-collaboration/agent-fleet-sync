use fleet_sync_delivery::{
    DeliveryConfigError, DeliveryLimits, DeliveryTypes, minimum_settlement_allowance,
};

pub struct SampleDelivery;
impl DeliveryTypes for SampleDelivery {
    const MAX_SENDER_BYTES: u32 = 128;
    const MAX_ADDRESS_BYTES: u32 = 128;
    const MAX_ACCEPTANCE_BYTES: u32 = 256;
    const MAX_STATUS_ROUTE_BYTES: u32 = 128;
    type Body = String;
    type Sender = String;
    fn submitter(_: &Self::Sender) -> fleet_sync_protocol::Submitter {
        // This sample consumer has one authorized producer, machine 2.
        match fleet_sync_protocol::PeerId::new(durable_steps::Uuid::from_u128(
            0x019eef00000070008000000000000002,
        )) {
            Ok(peer) => fleet_sync_protocol::Submitter::Machine(peer),
            Err(error) => panic!("invalid sample producer identity: {error}"),
        }
    }
    type Address = String;
    type Acceptance = String;
    type LocalObservation = ();
    const MAX_LOCAL_OBSERVATION_BYTES: u32 = 4;
    type StatusRoute = String;
}
pub fn delivery_limits() -> Result<DeliveryLimits, DeliveryConfigError> {
    DeliveryLimits::new::<SampleDelivery>(
        10_000_000,
        200,
        10_000_000,
        200,
        10_000_000,
        minimum_settlement_allowance::<SampleDelivery>()?,
    )
}
