use crate::DeliveryError;
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct OutboxHeader<A> {
    pub id: ItemId,
    pub digest: String,
    pub relay: PeerId,

    pub recipient: PeerId,
    pub valid_until: Option<SyncTime>,
    pub status: DeliveryStatus<A>,
    pub version: StatusVersion,
    pub submit_attempted: bool,
    pub cancel: Option<CancelResult>,
    pub cancel_target: Option<ItemId>,
    pub settlement_bytes: u32,
    pub capacity_slot: bool,
}
impl<A> OutboxHeader<A> {
    pub fn validate(&self) -> Result<(), DeliveryError> {
        let processing_failed = matches!(
            self.status,
            DeliveryStatus::Refused {
                reason: DeliveryRefusal::ProcessingFailed { .. }
            }
        );
        if !processing_failed {
            ItemDigest::try_from(self.digest.clone()).map_err(|_| DeliveryError::Corrupt {
                field: "outbox header digest",
            })?;
        }
        if self.cancel.is_some() != self.cancel_target.is_some()
            || !self.submit_attempted && self.version.value() != 0
            || self.version.value() > i64::MAX as u64
        {
            return Err(DeliveryError::Corrupt {
                field: "outbox header state",
            });
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum OutboxHeaderState<A> {
    Item(OutboxHeader<A>),
    Recovered(RecoveredOutbox<A>),
    Unresolvable {
        item: ItemId,
        kind: DeliveryFailureKind,
    },
}

pub(crate) struct OutboxRecord<D: DeliveryTypes> {
    pub header: OutboxHeader<D::Acceptance>,
    pub payload: Option<DeliveryEnvelope<D>>,
}
impl<D: DeliveryTypes> OutboxRecord<D> {
    pub fn validate(&self) -> Result<(), DeliveryError> {
        self.header.validate()?;
        if let Some(payload) = &self.payload
            && (payload.id() != self.header.id
                || payload.digest().as_str() != self.header.digest
                || payload.recipient().peer != self.header.recipient
                || payload.valid_until() != self.header.valid_until
                || self.header.cancel_target
                    != match payload {
                        DeliveryEnvelope::Item(_) => None,
                        DeliveryEnvelope::Cancel(cancel) => Some(cancel.target()),
                    })
        {
            return Err(DeliveryError::Corrupt {
                field: "outbox payload identity",
            });
        }
        if matches!(self.header.status, DeliveryStatus::Queued) && self.payload.is_none() {
            return Err(DeliveryError::Corrupt {
                field: "queued outbox payload missing",
            });
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, bound = "")]
pub struct ResolvedSubmit<D: DeliveryTypes> {
    pub relay: PeerId,
    pub item: DeliveryEnvelope<D>,
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum OutboxAdmission<A> {
    Queued {
        reference: DeliveryItemRef,
    },
    Replayed {
        status: DeliveryStatus<A>,
        version: StatusVersion,
    },
    /// A different cancel is canonical; the requested ID was not admitted.
    CancelAlreadyRequested {
        existing: DeliveryItemRef,
        status: DeliveryStatus<A>,
        version: StatusVersion,
    },
    Refused {
        reason: DeliveryRefusal,
    },
}

/// Sender-local knowledge can survive losing the canonical digest header.
#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum OutboxStatusState<A> {
    Unknown,
    Known {
        status: DeliveryStatus<A>,
        version: StatusVersion,
        cancel: Option<CancelResult>,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutboxStatus<A> {
    pub item: ItemId,
    pub state: OutboxStatusState<A>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubmitInput {
    pub item: ItemId,
    pub digest: ItemDigest,
    pub relay: PeerId,
    pub mailbox: PeerId,
}
impl SubmitInput {
    pub fn reference(&self) -> DeliveryItemRef {
        DeliveryItemRef {
            item: self.item,
            digest: self.digest.clone(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveredOutbox<A> {
    pub item: ItemId,
    pub digest: ItemDigest,
    pub status: DeliveryStatus<A>,
    pub version: StatusVersion,
    pub cancel: Option<CancelResult>,
}
