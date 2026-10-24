use crate::{
    DeliveryConfigError, custody_record::*, delivery_sizes::canonical_size, inbox_record::*,
    outbox_record::*, *,
};
use serde::Serialize;

fn bytes(value: &impl Serialize) -> Result<u64, DeliveryConfigError> {
    canonical_size(value).map_err(|error| DeliveryConfigError::Encoding {
        message: error.to_string(),
    })
}
fn with_acceptance(
    value: &impl Serialize,
    delivered: bool,
    acceptance_bound: u32,
) -> Result<u64, DeliveryConfigError> {
    let size = bytes(value)?;
    if delivered {
        size.checked_sub(4)
            .and_then(|size| size.checked_add(u64::from(acceptance_bound)))
            .ok_or(DeliveryConfigError::ReserveOverflow)
    } else {
        Ok(size)
    }
}
fn with_route<D: DeliveryTypes>(size: u64) -> Result<u64, DeliveryConfigError> {
    size.checked_sub(4)
        .and_then(|size| size.checked_add(u64::from(D::MAX_STATUS_ROUTE_BYTES)))
        .ok_or(DeliveryConfigError::ReserveOverflow)
}
/// The allowance covers both item headers, one cancel, and the largest owed
/// reports and all four historical status targets per item at the busiest hop.
/// Unit route and receipt placeholders encode as `null`;
/// their four bytes are replaced by the consumer's canonical size bounds.
pub fn minimum_settlement_allowance<D: DeliveryTypes>() -> Result<u32, DeliveryConfigError> {
    allowance_with_bound::<D>(None)
}

pub(crate) fn allowance_with_bound<D: DeliveryTypes>(
    acceptance_bound: Option<u32>,
) -> Result<u32, DeliveryConfigError> {
    let maximum = acceptance_bound.unwrap_or(D::MAX_ACCEPTANCE_BYTES);
    let invalid = |error: Box<dyn std::fmt::Display>| DeliveryConfigError::Encoding {
        message: error.to_string(),
    };
    let item = ItemId::new(durable_steps::Uuid::from_u128(
        0x01900000000070008000000000000001,
    ))
    .map_err(|error| invalid(Box::new(error)))?;
    let peer = PeerId::new(durable_steps::Uuid::from_u128(
        0x019eef00000070008000000000000001,
    ))
    .map_err(|error| invalid(Box::new(error)))?;
    let digest = ItemDigest::try_from("f".repeat(64)).map_err(|error| invalid(Box::new(error)))?;
    let time = SyncTime::from_unix_millis(i64::MIN);
    let mut refusals = vec![
        DeliveryRefusal::RecipientRefused {
            code: RecipientRefusalCode::new("R".repeat(MAX_RECIPIENT_REFUSAL_CODE_BYTES))
                .map_err(|error| invalid(Box::new(error)))?,
        },
        DeliveryRefusal::ItemConflict {
            existing: digest.clone(),
        },
        DeliveryRefusal::CancelAlreadyRequested {
            existing: DeliveryItemRef {
                item,
                digest: digest.clone(),
            },
        },
        DeliveryRefusal::TooLarge {
            field: DeliverySizeField::Acceptance,
        },
        DeliveryRefusal::TooLarge {
            field: DeliverySizeField::StatusRoute,
        },
    ];
    for kind in [
        DeliveryFailureKind::CustodyMissing,
        DeliveryFailureKind::CustodyUnresolvable,
        DeliveryFailureKind::CustodyCorrupt,
        DeliveryFailureKind::DigestMismatch,
        DeliveryFailureKind::VersionMismatch,
        DeliveryFailureKind::Nondeterministic,
        DeliveryFailureKind::InvalidPayload,
        DeliveryFailureKind::ExecutionFailed,
    ] {
        refusals.push(DeliveryRefusal::ProcessingFailed { kind });
    }
    let mut largest = DeliveryRefusal::MailboxFull;
    let mut largest_size = 0;
    for refusal in refusals {
        let size = bytes(&refusal)?;
        if size > largest_size {
            largest = refusal;
            largest_size = size;
        }
    }
    let cancel = DeliveryEnvelope::<NoDelivery>::Cancel(
        DeliveryCancel::new(
            item,
            (),
            Recipient { peer, address: () },
            Some(time),
            CancelBody { target: item },
        )
        .map_err(|error| invalid(Box::new(error)))?,
    );
    let cancel_bytes = bytes(&cancel)?
        .checked_sub(8)
        .and_then(|size| size.checked_add(u64::from(D::MAX_SENDER_BYTES)))
        .and_then(|size| size.checked_add(u64::from(D::MAX_ADDRESS_BYTES)))
        .ok_or(DeliveryConfigError::ReserveOverflow)?;
    let statuses = [
        DeliveryStatus::Queued,
        DeliveryStatus::Sent {
            seq: DeliverySeq::new(u64::MAX),
        },
        DeliveryStatus::AtRecipient,
        DeliveryStatus::Held,
        DeliveryStatus::Delivered { acceptance: () },
        DeliveryStatus::Uncertain,
        DeliveryStatus::Expired,
        DeliveryStatus::Cancelled,
        DeliveryStatus::Refused {
            reason: largest.clone(),
        },
    ];
    let mut outbox_header = 0;
    let mut mailbox_header = 0;
    let mut publication = 0;
    for status in statuses {
        let delivered = matches!(status, DeliveryStatus::Delivered { .. });
        outbox_header = outbox_header.max(with_acceptance(
            &OutboxHeaderState::Item(OutboxHeader {
                id: item,
                digest: digest.as_str().into(),
                relay: peer,
                recipient: peer,
                valid_until: Some(time),
                status: status.clone(),
                version: StatusVersion::new(u64::MAX),
                submit_attempted: false,
                cancel: Some(CancelResult::TooLate {
                    at: TooLateAt::Uncertain,
                }),
                cancel_target: Some(item),
                acceptance_bound,
                settlement_bytes: u32::MAX,
                capacity_slot: false,
            }),
            delivered,
            maximum,
        )?);
        mailbox_header = mailbox_header.max(with_route::<D>(with_acceptance(
            &MailRecord::<NoDelivery>::Live(LiveMail {
                id: item,
                digest: digest.clone(),
                origin: peer,
                status_target: CurrentStatusTarget {
                    peer,
                    route: (),
                    rank: Some(u64::MAX),
                },
                stream: DeliveryStream::Controls,
                seq: DeliverySeq::new(u64::MAX),
                payload: None,
                status: status.clone(),
                version: StatusVersion::new(u64::MAX),
                last_report_seq: ReportSeq::new(u64::MAX),
                accepted_at: time,
                fetched: false,
                released_at: Some(time),
                cancel: Some(CancelResult::TooLate {
                    at: TooLateAt::Uncertain,
                }),
                cancel_target: Some(item),
                acceptance_bound,
                settlement_bytes: u32::MAX,
                capacity_slot: false,
            }),
            delivered,
            maximum,
        )?)?);
        publication = publication.max(with_route::<D>(with_acceptance(
            &(
                PublicationTarget::StatusStream {
                    sender: peer,
                    route: (),
                },
                StatusPublication {
                    item,
                    status,
                    version: StatusVersion::new(u64::MAX),
                    cancel: Some(CancelResult::TooLate {
                        at: TooLateAt::Uncertain,
                    }),
                },
            ),
            delivered,
            maximum,
        )?)?);
    }
    let outcomes = [
        RecipientOutcome::Held,
        RecipientOutcome::Delivered { acceptance: () },
        RecipientOutcome::Uncertain,
        RecipientOutcome::Expired,
        RecipientOutcome::Prevented { cancel: item },
        RecipientOutcome::TooLate {
            cancel: item,
            at: TooLateAt::Uncertain,
        },
        RecipientOutcome::Dropped,
        RecipientOutcome::Refused { reason: largest },
    ];
    let mut inbox_header = 0;
    let mut report = 0;
    for outcome in outcomes {
        let delivered = matches!(outcome, RecipientOutcome::Delivered { .. });
        report = report.max(with_acceptance(
            &RecipientReport::<NoDelivery> {
                item,
                report_seq: ReportSeq::new(u64::MAX),
                outcome: outcome.clone(),
            },
            delivered,
            maximum,
        )?);
        inbox_header = inbox_header.max(
            with_acceptance(
                &InboxHeader::<NoDelivery> {
                    id: item,
                    digest: digest.as_str().into(),
                    sender: (),
                    recipient: Recipient { peer, address: () },
                    cancel_target: Some(item),
                    valid_until: Some(time),
                    stream: DeliveryStream::Controls,
                    route: InboxRoute::Relayed,
                    progress: InboxProgress::Final {
                        attempt: u64::MAX,
                        outcome,
                    },
                    report_seq: ReportSeq::new(u64::MAX),
                    acceptance_bound,
                    settlement_bytes: u32::MAX,
                    release_requested: false,
                },
                delivered,
                maximum,
            )?
            .checked_sub(8)
            .and_then(|size| size.checked_add(u64::from(D::MAX_SENDER_BYTES)))
            .and_then(|size| size.checked_add(u64::from(D::MAX_ADDRESS_BYTES)))
            .ok_or(DeliveryConfigError::ReserveOverflow)?,
        );
    }
    const PUBLISHED_STATUS_RANKS: u64 = 4;
    let retained_publications = 2 * PUBLISHED_STATUS_RANKS * publication;
    let headers = (2 * outbox_header)
        .max(2 * mailbox_header + retained_publications)
        .max(2 * inbox_header + 2 * report);
    u32::try_from(headers + cancel_bytes).map_err(|_| DeliveryConfigError::ReserveOverflow)
}
