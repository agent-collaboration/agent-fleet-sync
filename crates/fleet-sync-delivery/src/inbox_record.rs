use crate::DeliveryError;
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum InboxProgress<A> {
    Pending {
        attempt: u64,
        not_before: SyncTime,
    },
    Attempting {
        attempt: u64,
    },
    Held {
        attempt: u64,
    },
    Final {
        attempt: u64,
        outcome: RecipientOutcome<A>,
    },
    Control {
        outcome: ControlOutcome,
    },
}
impl<A> InboxProgress<A> {
    pub const fn attempt(&self) -> u64 {
        match self {
            Self::Pending { attempt, .. }
            | Self::Attempting { attempt }
            | Self::Held { attempt }
            | Self::Final { attempt, .. } => *attempt,
            Self::Control { .. } => 0,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, bound = "")]
pub(crate) struct InboxHeader<D: DeliveryTypes> {
    pub id: ItemId,
    pub digest: String,
    pub sender: D::Sender,
    pub recipient: Recipient<D::Address>,
    pub cancel_target: Option<ItemId>,
    pub valid_until: Option<SyncTime>,
    pub stream: DeliveryStream,
    pub route: InboxRoute,
    pub progress: InboxProgress<D::Acceptance>,
    pub report_seq: ReportSeq,
    pub settlement_bytes: u32,
    pub release_requested: bool,
}

pub(crate) struct InboxRecord<D: DeliveryTypes> {
    pub header: InboxHeader<D>,
    pub payload: Option<DeliveryEnvelope<D>>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum InboxRoute {
    Local,
    Relayed,
}
impl<D: DeliveryTypes> InboxHeader<D> {
    pub fn compact_local(&self) -> bool {
        matches!(self.route, InboxRoute::Local)
            && matches!(self.progress, InboxProgress::Final { .. })
    }
    pub fn delivery_header(&self) -> Result<DeliveryHeader<D>, DeliveryError> {
        let digest =
            ItemDigest::try_from(self.digest.clone()).map_err(|_| DeliveryError::Corrupt {
                field: "inbox header digest",
            })?;
        crate::custody_record::delivery_header::<D>(
            self.id,
            &self.sender,
            &self.recipient,
            self.valid_until,
            &digest,
        )
    }
    pub fn validate(&self, owner: PeerId) -> Result<(), DeliveryError> {
        let processing_failed = matches!(
            &self.progress,
            InboxProgress::Final {
                outcome: RecipientOutcome::Refused {
                    reason: DeliveryRefusal::ProcessingFailed { .. }
                },
                ..
            }
        );
        if !processing_failed {
            ItemDigest::try_from(self.digest.clone()).map_err(|_| DeliveryError::Corrupt {
                field: "inbox header digest",
            })?;
        }
        if (self.stream == DeliveryStream::Controls) != self.cancel_target.is_some()
            || self.recipient.peer != owner
            || self.progress.attempt() > i64::MAX as u64
            || self.report_seq.value() > i64::MAX as u64
            || (self.stream == DeliveryStream::Controls)
                != matches!(self.progress, InboxProgress::Control { .. })
        {
            return Err(DeliveryError::Corrupt {
                field: "inbox custody state",
            });
        }
        if matches!(
            self.progress,
            InboxProgress::Final {
                outcome: RecipientOutcome::Held,
                ..
            }
        ) {
            return Err(DeliveryError::Corrupt {
                field: "inbox final held outcome",
            });
        }
        if self.release_requested
            && !matches!(
                self.progress,
                InboxProgress::Pending { .. } | InboxProgress::Attempting { .. }
            )
        {
            return Err(DeliveryError::Corrupt {
                field: "inbox release signal",
            });
        }
        Ok(())
    }
}
impl<D: DeliveryTypes> InboxRecord<D> {
    pub fn new(
        payload: DeliveryEnvelope<D>,
        route: InboxRoute,
        progress: InboxProgress<D::Acceptance>,
        settlement_bytes: u32,
    ) -> Result<Self, DeliveryError> {
        Ok(Self {
            header: InboxHeader {
                id: payload.id(),
                digest: payload.digest().as_str().into(),
                sender: crate::custody_record::copy_wire(payload.sender())?,
                recipient: crate::custody_record::copy_wire(payload.recipient())?,
                cancel_target: match &payload {
                    DeliveryEnvelope::Item(_) => None,
                    DeliveryEnvelope::Cancel(cancel) => Some(cancel.target()),
                },
                valid_until: payload.valid_until(),
                stream: payload.stream(),
                route,
                progress,
                report_seq: ReportSeq::new(0),
                settlement_bytes,
                release_requested: false,
            },
            payload: Some(payload),
        })
    }
    pub fn validate(&self, owner: PeerId) -> Result<(), DeliveryError> {
        self.header.validate(owner)?;
        match &self.payload {
            Some(payload) => {
                if self.header.id != payload.id()
                    || self.header.digest != payload.digest().as_str()
                    || self.header.recipient.peer != payload.recipient().peer
                    || self.header.valid_until != payload.valid_until()
                    || self.header.stream != payload.stream()
                    || self.header.cancel_target
                        != match payload {
                            DeliveryEnvelope::Item(_) => None,
                            DeliveryEnvelope::Cancel(cancel) => Some(cancel.target()),
                        }
                    || crate::status_stream::canonical_encode(&self.header.sender)?
                        != crate::status_stream::canonical_encode(payload.sender())?
                    || crate::status_stream::canonical_encode(&self.header.recipient)?
                        != crate::status_stream::canonical_encode(payload.recipient())?
                {
                    return Err(DeliveryError::Corrupt {
                        field: "inbox payload identity",
                    });
                }
            }
            None => {
                if !self.header.compact_local()
                    && !matches!(
                        self.header.progress,
                        InboxProgress::Final {
                            outcome: RecipientOutcome::Refused {
                                reason: DeliveryRefusal::ProcessingFailed { .. }
                            },
                            ..
                        }
                    )
                {
                    return Err(DeliveryError::CustodyMissing);
                }
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum ControlOutcome {
    Pending,
    Prevented,
    TooLate { at: TooLateAt },
    Refused { reason: DeliveryRefusal },
    Expired,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReceiveInput {
    pub peer: PeerId,
    pub stream: DeliveryStream,
    pub cycle: ReceiveCycle,
    pub after: DeliverySeq,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeliveryAttempt {
    pub item: ItemId,
    pub attempt: u64,
    pub not_before: SyncTime,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReportRef {
    pub item: ItemId,
    pub report_seq: ReportSeq,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct StreamState {
    pub received: DeliverySeq,
    pub acknowledged: DeliverySeq,
    pub next_cycle: ReceiveCycle,
    pub active: Option<ReceiveInput>,
    pub rerun_requested: bool,
}
impl StreamState {
    pub fn new() -> Self {
        Self {
            received: DeliverySeq::new(0),
            acknowledged: DeliverySeq::new(0),
            next_cycle: ReceiveCycle::new(1),
            active: None,
            rerun_requested: false,
        }
    }
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct InboxState {
    pub owner: PeerId,
    pub items: StreamState,
    pub controls: StreamState,
}
impl InboxState {
    pub fn new(owner: PeerId) -> Self {
        Self {
            owner,
            items: StreamState::new(),
            controls: StreamState::new(),
        }
    }
    pub fn stream(&mut self, stream: DeliveryStream) -> &mut StreamState {
        match stream {
            DeliveryStream::Items => &mut self.items,
            DeliveryStream::Controls => &mut self.controls,
        }
    }
    pub fn validate(&self, owner: PeerId) -> Result<(), DeliveryError> {
        if owner != self.owner {
            return Err(DeliveryError::Corrupt {
                field: "inbox owner",
            });
        }
        for (stream, state) in [
            (DeliveryStream::Items, self.items),
            (DeliveryStream::Controls, self.controls),
        ] {
            if state.acknowledged > state.received
                || state.next_cycle.value() == 0
                || state.next_cycle.value() > i64::MAX as u64
                || state.active.is_some_and(|run| {
                    run.peer != owner
                        || run.stream != stream
                        || run.cycle >= state.next_cycle
                        || run.after > state.received
                })
                || state.rerun_requested && state.active.is_none()
            {
                return Err(DeliveryError::Corrupt {
                    field: "inbox receive cycle",
                });
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct InboxEffects {
    pub attempts: Vec<DeliveryAttempt>,
    pub reports: Vec<ReportRef>,
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DeliveryCheck<A> {
    Deliver {
        reference: DeliveryItemRef,
    },
    Wait {
        until: SyncTime,
    },
    Finish {
        outcome: Option<RecipientOutcome<A>>,
    },
}
