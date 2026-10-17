use crate::{DeliveryTypes, minimum_settlement_allowance};
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryLimits {
    pub(crate) mailbox_payload_bytes: u64,
    pub(crate) mailbox_items: u64,
    pub(crate) outbox_bytes: u64,
    pub(crate) outbox_items: u64,
    pub(crate) inbox_bytes: u64,
    pub(crate) settlement_allowance: u32,
}
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DeliveryConfigError {
    #[error("settlement allowance {supplied} is below the required {minimum}")]
    SettlementAllowanceTooSmall { supplied: u32, minimum: u32 },
    #[error("declared delivery bounds exceed the settlement reserve range")]
    ReserveOverflow,
    #[error("cannot encode delivery settlement metadata: {message}")]
    Encoding { message: String },
}
impl DeliveryLimits {
    pub fn new<D: DeliveryTypes>(
        mailbox_payload_bytes: u64,
        mailbox_items: u64,
        outbox_bytes: u64,
        outbox_items: u64,
        inbox_bytes: u64,
        settlement_allowance: u32,
    ) -> Result<Self, DeliveryConfigError> {
        let limits = Self {
            mailbox_payload_bytes,
            mailbox_items,
            outbox_bytes,
            outbox_items,
            inbox_bytes,
            settlement_allowance,
        };
        limits.validate::<D>()?;
        Ok(limits)
    }
    pub(crate) fn validate<D: DeliveryTypes>(&self) -> Result<(), DeliveryConfigError> {
        let minimum = minimum_settlement_allowance::<D>()?;
        if self.settlement_allowance < minimum {
            return Err(DeliveryConfigError::SettlementAllowanceTooSmall {
                supplied: self.settlement_allowance,
                minimum,
            });
        }
        Ok(())
    }
    pub const fn mailbox_payload_bytes(&self) -> u64 {
        self.mailbox_payload_bytes
    }
    pub const fn mailbox_items(&self) -> u64 {
        self.mailbox_items
    }
    pub const fn outbox_bytes(&self) -> u64 {
        self.outbox_bytes
    }
    pub const fn outbox_items(&self) -> u64 {
        self.outbox_items
    }
    pub const fn inbox_bytes(&self) -> u64 {
        self.inbox_bytes
    }
    pub const fn settlement_allowance(&self) -> u32 {
        self.settlement_allowance
    }
}
