//! Terminal projections share the same persistence boundary as custody writes.
use crate::{DeliveryError, Outbox, outbox_record::OutboxHeaderState};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

#[cfg(test)]
#[path = "outbox_outcome_test.rs"]
mod tests;

impl<S, D, B> Outbox<'_, S, D, B>
where
    S: TransactionScope,
    D: DeliveryTypes,
    B: crate::SenderBinding<S, D>,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database> + Encode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send preserves higher-ranked borrowed transaction futures"
    )]
    pub(crate) fn project_terminal_outcome<'call>(
        &'call mut self,
        item: ItemId,
        status: &'call DeliveryStatus<D::Acceptance>,
    ) -> impl std::future::Future<Output = Result<(), DeliveryError>> + Send + 'call {
        async move {
            if !status.is_terminal() {
                return Ok(());
            }
            // Recovery reads the surviving header independently of the body and
            // accounting metadata. Neither compaction nor corrupt payload bytes
            // should prevent projection or make a terminal replay look fresh.
            let already_terminal = match self.recovery_header(item).await {
                Ok(Some(OutboxHeaderState::Item(header))) => header.status.is_terminal(),
                Ok(Some(OutboxHeaderState::Recovered(record))) => record.status.is_terminal(),
                Ok(Some(OutboxHeaderState::Legacy { status, .. })) => status.is_terminal(),
                Ok(Some(OutboxHeaderState::Unresolvable { .. })) => true,
                Ok(None) | Err(DeliveryError::Corrupt { .. }) => false,
                Err(error) => return Err(error),
            };
            if !already_terminal {
                self.binding
                    .on_outbox_outcome(self.scope, item, status)
                    .await?;
            }
            Ok(())
        }
    }
}
