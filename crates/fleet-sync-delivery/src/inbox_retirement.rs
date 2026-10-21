use crate::{
    DeliveryError, checked_queries as queries,
    custody_record::{item_key, sql_error},
    inbox_record::{InboxProgress, InboxRoute},
    inbox_storage::Inbox,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

/// The current custody result, without a retained retirement tombstone.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum LocalRetirement {
    Retired,
    /// No row exists now; this does not establish a previous retirement or digest.
    Absent,
    Refused(LocalRetirementRefusal),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LocalRetirementRefusal {
    DigestMismatch,
    Relayed,
    Pending,
    Attempting,
    Held,
    ControlItem,
}

impl<S, D> Inbox<'_, S, D>
where
    S: TransactionScope,
    D: DeliveryTypes,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database> + Encode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    /// Retire a terminal Local original inside the caller's custody transaction.
    /// The caller owns eligibility, including any acknowledged frontier. Propagate
    /// storage errors to roll back the transaction. Refusals perform no writes.
    /// Distinct cancel rows remain; only links involving this item are removed.
    /// No history remains to deduplicate later admission under the same item ID.
    pub async fn retire_local(
        &mut self,
        reference: &DeliveryItemRef,
    ) -> Result<LocalRetirement, DeliveryError> {
        let Some(record) = self.load(reference.item).await? else {
            return Ok(LocalRetirement::Absent);
        };
        if record.header.digest != reference.digest.as_str() {
            return Ok(LocalRetirement::Refused(
                LocalRetirementRefusal::DigestMismatch,
            ));
        }
        if matches!(record.header.route, InboxRoute::Relayed) {
            return Ok(LocalRetirement::Refused(LocalRetirementRefusal::Relayed));
        }
        let refusal = match record.header.progress {
            InboxProgress::Pending { .. } => Some(LocalRetirementRefusal::Pending),
            InboxProgress::Attempting { .. } => Some(LocalRetirementRefusal::Attempting),
            InboxProgress::Held { .. } => Some(LocalRetirementRefusal::Held),
            InboxProgress::Control { .. } => Some(LocalRetirementRefusal::ControlItem),
            InboxProgress::Final { .. } => None,
        };
        if let Some(reason) = refusal {
            return Ok(LocalRetirement::Refused(reason));
        }
        // Inbox charges are derived from these rows, so deletion also removes
        // this item's accounting without changing another item's reservation.
        queries::drop_inbox_item(self.scope, item_key(reference.item))
            .await
            .map_err(sql_error)?;
        queries::drop_inbox_cancel_link(self.scope, item_key(reference.item))
            .await
            .map_err(sql_error)?;
        queries::drop_inbox_reports(self.scope, item_key(reference.item))
            .await
            .map_err(sql_error)?;
        Ok(LocalRetirement::Retired)
    }
}
