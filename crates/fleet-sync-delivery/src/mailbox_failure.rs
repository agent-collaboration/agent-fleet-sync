use crate::DeliveryTransactionFailure;
use fleet_sync_protocol::{
    DeliveryAnswer, DeliveryFailureKind, DeliveryRefusal, DeliveryTypes, PeerId, ReportResult,
    SubmitResult,
};

/// Status-owner operations deliberately do not enter the mailbox failure table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MailboxOperation {
    Submit,
    Fetch,
    Received,
    Report,
    Query,
    Retire,
    Settle,
}

#[derive(Debug, Eq, PartialEq)]
pub struct MailboxCorruption {
    pub mailbox: PeerId,
    pub field: String,
}

pub enum MailboxFailureDisposition<D: DeliveryTypes> {
    Answer {
        answer: DeliveryAnswer<D>,
        corruption: Option<MailboxCorruption>,
    },
    Unhandled(DeliveryTransactionFailure),
}

/// Decide the mailbox answer without mutating host-owned health. The host applies
/// any corruption signal only after its transaction returns.
pub fn mailbox_failure_disposition<D: DeliveryTypes>(
    mailbox: PeerId,
    operation: MailboxOperation,
    failure: DeliveryTransactionFailure,
) -> MailboxFailureDisposition<D> {
    match (operation, failure) {
        (MailboxOperation::Submit, DeliveryTransactionFailure::Refused(reason)) => {
            MailboxFailureDisposition::Answer {
                answer: DeliveryAnswer::Submit(SubmitResult::Refused { reason }),
                corruption: None,
            }
        }
        (MailboxOperation::Report, DeliveryTransactionFailure::Refused(reason)) => {
            MailboxFailureDisposition::Answer {
                answer: DeliveryAnswer::Report(ReportResult::Refused { reason }),
                corruption: None,
            }
        }
        (_, DeliveryTransactionFailure::Corrupt { field }) => MailboxFailureDisposition::Answer {
            answer: DeliveryAnswer::MailboxUnavailable {
                reason: DeliveryRefusal::ProcessingFailed {
                    kind: DeliveryFailureKind::CustodyCorrupt,
                },
            },
            corruption: Some(MailboxCorruption { mailbox, field }),
        },
        (_, failure) => MailboxFailureDisposition::Unhandled(failure),
    }
}
