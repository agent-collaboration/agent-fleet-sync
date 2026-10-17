use crate::{
    DeliveryError, ReceiveInput, checked_queries as queries, custody_record::*,
    inbox_storage::Inbox,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

impl<S, D> Inbox<'_, S, D>
where
    S: TransactionScope,
    D: DeliveryTypes,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database> + Encode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub async fn begin_receive_and_admit<T: durable_steps::Task<Input = ReceiveInput>>(
        &mut self,
        stream: DeliveryStream,
    ) -> Result<Option<durable_steps::Admitted>, DeliveryError>
    where
        S: durable_steps::JournalScope,
    {
        let Some(input) = self.begin_receive(stream).await? else {
            return Ok(None);
        };
        self.scope
            .admit::<T>(
                &crate::delivery_request_key(&(input.stream, input.cycle))?,
                &input,
            )
            .await
            .map(Some)
            .map_err(|error| DeliveryError::Storage(error.to_string()))
    }
    pub async fn begin_receive(
        &mut self,
        stream: DeliveryStream,
    ) -> Result<Option<ReceiveInput>, DeliveryError> {
        let mut state = self.state().await?;
        let owner = self.owner;
        let progress = state.stream(stream);
        let next = if progress.active.is_some() {
            progress.rerun_requested = true;
            None
        } else {
            let cycle = progress.next_cycle;
            progress.next_cycle = cycle
                .next()
                .filter(|next| next.value() <= i64::MAX as u64)
                .ok_or(DeliveryError::SequenceExhausted)?;
            let input = ReceiveInput {
                peer: owner,
                stream,
                cycle,
                after: progress.received,
            };
            progress.active = Some(input);
            Some(input)
        };
        self.save_state(&state).await?;
        Ok(next)
    }

    pub async fn finish_receive(
        &mut self,
        input: ReceiveInput,
    ) -> Result<Option<ReceiveInput>, DeliveryError> {
        let mut state = self.state().await?;
        let progress = state.stream(input.stream);
        if !progress.active.is_some_and(|run| {
            run.cycle == input.cycle && run.after == input.after && run.peer == input.peer
        }) {
            return Err(DeliveryError::InvalidBatch);
        }
        if progress.acknowledged < progress.received {
            return Err(DeliveryError::InvalidBatch);
        }
        let again = progress.rerun_requested;
        progress.active = None;
        progress.rerun_requested = false;
        self.save_state(&state).await?;
        if again {
            self.begin_receive(input.stream).await
        } else {
            Ok(None)
        }
    }

    pub async fn acknowledge_receive(
        &mut self,
        input: ReceiveInput,
        answer: ReceivedResult,
    ) -> Result<(), DeliveryError> {
        let mut state = self.state().await?;
        let progress = state.stream(input.stream);
        if answer.stream != input.stream
            || answer.released_through > progress.received
            || answer.released_through < input.after
            || !progress.active.is_some_and(|run| run.cycle == input.cycle)
        {
            return Err(DeliveryError::InvalidBatch);
        }
        progress.acknowledged = progress.acknowledged.max(answer.released_through);
        self.save_state(&state).await
    }

    pub async fn receive_budget(
        &mut self,
        input: ReceiveInput,
        max_items: u32,
        max_bytes: u64,
    ) -> Result<(u32, u64), DeliveryError> {
        let mut state = self.state().await?;
        let progress = state.stream(input.stream);
        if !progress
            .active
            .is_some_and(|run| run.cycle == input.cycle && run.after == input.after)
        {
            return Err(DeliveryError::InvalidBatch);
        }
        if input.stream == DeliveryStream::Controls {
            return Ok((max_items, max_bytes));
        }
        let accounting = queries::read_inbox_accounting(self.scope)
            .await
            .map_err(sql_error)?;
        let used = accounting
            .bytes
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(DeliveryError::Corrupt {
                field: "inbox occupied bytes",
            })?;
        let available = self.limits.inbox_bytes.saturating_sub(used);
        // One item guarantees progress for any item that fits this inbox. The
        // next cycle drains the next item; concurrent triggers coalesce.
        let count = if max_items == 0 || available <= u64::from(self.limits.settlement_allowance) {
            0
        } else {
            1
        };
        Ok((
            count,
            available
                .saturating_sub(u64::from(self.limits.settlement_allowance))
                .min(max_bytes),
        ))
    }
}
