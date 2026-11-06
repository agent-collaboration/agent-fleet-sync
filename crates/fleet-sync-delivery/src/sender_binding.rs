use crate::{DeliveryError, DeliveryTypes, ItemId, SenderBindingFailure, Submitter};
use durable_steps::{CallFailure, CallRetryReason, storage::TransactionScope};
use serde::{Deserialize, Serialize};
use std::future::Future;

/// Resolve a consumer sender from independent host truth in the custody scope.
/// The claimed/stored owner is deliberately absent from this interface.
///
/// A missing binding must signal the host's existing lookup/refresh owner,
/// without awaiting that owner while holding the custody transaction. The host
/// refreshes outside the failed transaction, then retries through this seam;
/// returning Unknown must not leave refresh waiting for a periodic timer.
/// The resolver must not write business or directory rows, perform I/O in
/// Serde, use a global cache, or derive an owner from its stored claim.
/// The separate outcome hook may write consumer projections in this scope.
pub trait SenderBinding<S: TransactionScope, D: DeliveryTypes>: Send + Sync {
    fn resolve_submitter<'a>(
        &'a self,
        scope: &'a mut S,
        sender: &'a D::Sender,
    ) -> impl Future<Output = Result<Submitter, SenderBindingFailure>> + Send + 'a;

    /// Project the first terminal outbox status in its custody transaction.
    /// Replays and non-terminal advances do not call this hook. The item ID
    /// remains available after compaction and during missing-custody recovery.
    /// An error aborts the transaction, including the projection's writes.
    /// In library tasks, Storage retries (as does SenderBindingUnavailable);
    /// other errors fail the task and on_failed records ProcessingFailed.
    fn on_outbox_outcome(
        &self,
        _scope: &mut S,
        _item: ItemId,
        _status: &crate::DeliveryStatus<D::Acceptance>,
    ) -> impl Future<Output = Result<(), DeliveryError>> + Send {
        async { Ok(()) }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "item",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum CustodyRow {
    Outbox(ItemId),
    Inbox(ItemId),
}
impl CustodyRow {
    pub(crate) const fn table(self) -> &'static str {
        match self {
            Self::Outbox(_) => "_fleet_sync_outbox_items",
            Self::Inbox(_) => "_fleet_sync_inbox_items",
        }
    }
    pub const fn item(self) -> ItemId {
        match self {
            Self::Outbox(item) | Self::Inbox(item) => item,
        }
    }
}

pub(crate) struct VerifiedSender<D: DeliveryTypes> {
    owner: Submitter,
    source: D::Sender,
}
impl<D: DeliveryTypes> VerifiedSender<D> {
    pub const fn owner(&self) -> Submitter {
        self.owner
    }
    pub fn into_parts(self) -> (Submitter, D::Sender) {
        (self.owner, self.source)
    }
}
#[expect(
    clippy::manual_async_fn,
    reason = "explicit Send preserves higher-ranked borrowed transaction futures"
)]
pub(crate) fn verify_sender<'a, S, D, B>(
    scope: &'a mut S,
    binding: &'a B,
    sender: &'a D::Sender,
    row: CustodyRow,
) -> impl std::future::Future<Output = Result<VerifiedSender<D>, DeliveryError>> + Send + 'a
where
    S: TransactionScope,
    D: DeliveryTypes,
    B: SenderBinding<S, D>,
{
    async move {
        let owner = binding
            .resolve_submitter(scope, sender)
            .await
            .map_err(|reason| DeliveryError::SenderBindingUnavailable { row, reason })?;
        Ok(VerifiedSender {
            owner,
            source: crate::custody_record::copy_wire(sender)?,
        })
    }
}

pub(crate) fn binding_retry(reason: SenderBindingFailure) -> CallFailure {
    CallFailure::Retryable {
        reason: CallRetryReason {
            code: reason.code().into(),
            detail: "consumer sender binding is not established".into(),
        },
    }
}
