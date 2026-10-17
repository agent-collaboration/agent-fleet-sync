use fleet_sync_delivery::{
    DeliveryConfigError, DeliveryLimits, DeliveryTypes, minimum_settlement_allowance,
};

pub struct SampleDelivery;
impl DeliveryTypes for SampleDelivery {
    const MAX_SENDER_BYTES: u32 = 128;
    const MAX_ADDRESS_BYTES: u32 = 128;
    const MAX_ACCEPTANCE_BYTES: u32 = 256;
    type Body = String;
    type Sender = String;
    type Address = String;
    type Acceptance = String;
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
