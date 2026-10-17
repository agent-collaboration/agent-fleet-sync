use crate::{
    DeliveryAttempt, DeliveryConsumer, DeliveryError, InboxEffects, ReceiveInput,
    checked_queries as queries, custody_record::*, inbox_record::*, inbox_storage::Inbox,
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
    pub async fn enqueue_local_and_admit<T: durable_steps::Task<Input = DeliveryAttempt>>(
        &mut self,
        item: DeliveryItem<D>,
        now: SyncTime,
    ) -> Result<Option<durable_steps::Admitted>, DeliveryError>
    where
        S: durable_steps::JournalScope,
    {
        let Some(input) = self.enqueue_local(item, now).await? else {
            return Ok(None);
        };
        self.scope
            .admit::<T>(
                &crate::delivery_request_key(&(input.item, input.attempt))?,
                &input,
            )
            .await
            .map(Some)
            .map_err(|error| DeliveryError::Storage(error.to_string()))
    }
    pub async fn store_batch<C>(
        &mut self,
        input: ReceiveInput,
        batch: FetchResult<D>,
        consumer: &C,
        now: SyncTime,
    ) -> Result<InboxEffects, DeliveryError>
    where
        C: DeliveryConsumer<S>,
        C::Protocol: ConsumerProtocol<Delivery = D>,
    {
        self.limits.validate::<D>()?;
        let mut state = self.state().await?;
        let progress = state.stream(input.stream);
        if batch.stream != input.stream
            || batch.through < input.after
            || !progress.active.is_some_and(|run| {
                run.cycle == input.cycle && run.after == input.after && run.peer == input.peer
            })
        {
            return Err(DeliveryError::InvalidBatch);
        }
        let mut previous = input.after;
        for entry in &batch.entries {
            if entry.sequence() <= previous || entry.sequence() > batch.through {
                return Err(DeliveryError::InvalidBatch);
            }
            previous = entry.sequence();
            if let FetchEntry::Item { item, .. } = entry {
                crate::delivery_sizes::check_envelope(item)?;
            }
            match entry {
                FetchEntry::Item { item, .. }
                    if item.stream() != input.stream || item.recipient().peer != self.owner =>
                {
                    return Err(DeliveryError::InvalidBatch);
                }
                FetchEntry::Retired { .. } if input.stream != DeliveryStream::Controls => {
                    return Err(DeliveryError::InvalidBatch);
                }
                _ => {}
            }
        }
        let mut effects = InboxEffects::default();
        let advanced = batch.through > input.after;
        for entry in batch.entries {
            match entry {
                FetchEntry::Item { item, .. } => {
                    let id = item.id();
                    if let Some(mut record) = self.load(id).await? {
                        if record.header.digest != item.digest().as_str() {
                            return Err(DeliveryError::Corrupt {
                                field: "received item conflict",
                            });
                        }
                        if matches!(record.header.route, InboxRoute::Local) {
                            record.header.route = InboxRoute::Relayed;
                            if record.payload.is_none() {
                                record.payload = Some(item);
                            }
                            if let InboxProgress::Final { outcome, .. } = &record.header.progress {
                                let outcome = copy_wire(outcome)?;
                                effects
                                    .reports
                                    .extend(self.record_outcome(&mut record, outcome).await?);
                            }
                            self.save(&record).await?;
                        }
                    } else {
                        if input.after < progress.received {
                            return Err(DeliveryError::InvalidBatch);
                        }
                        let size = u64::try_from(encode(&item)?.len())
                            .map_err(|_| DeliveryError::SequenceExhausted)?;
                        let mut settlement = self.limits.settlement_allowance;
                        let item_progress = match &item {
                            DeliveryEnvelope::Item(_) => {
                                if let Some(cancel) = self.pending_cancel(id).await? {
                                    settlement=settlement.checked_sub(u32::try_from(cancel.payload.as_ref().map(encode).transpose()?.map_or(0,|raw|raw.len())).map_err(|_|DeliveryError::Corrupt{field:"cancel exceeds inbox settlement allowance"})?).ok_or(DeliveryError::Corrupt{field:"cancel exceeds inbox settlement allowance"})?;
                                }
                                let accounting = queries::read_inbox_accounting(self.scope)
                                    .await
                                    .map_err(sql_error)?;
                                let used = accounting
                                    .bytes
                                    .and_then(|bytes| bytes.parse::<u64>().ok())
                                    .ok_or(DeliveryError::Corrupt {
                                        field: "inbox accounting",
                                    })?;
                                if used
                                    .checked_add(size)
                                    .and_then(|bytes| bytes.checked_add(u64::from(settlement)))
                                    .is_none_or(|bytes| bytes > self.limits.inbox_bytes)
                                {
                                    return Err(DeliveryError::InboxFull);
                                }
                                effects.attempts.push(DeliveryAttempt {
                                    item: id,
                                    attempt: 0,
                                    not_before: now,
                                });
                                InboxProgress::Pending {
                                    attempt: 0,
                                    not_before: now,
                                }
                            }
                            DeliveryEnvelope::Cancel(cancel) => {
                                settlement = 0;
                                if let Some(mut original) = self.load(cancel.target()).await? {
                                    original.header.settlement_bytes = original
                                        .header
                                        .settlement_bytes
                                        .checked_sub(u32::try_from(size).map_err(|_| {
                                            DeliveryError::Corrupt {
                                                field: "cancel exceeds inbox settlement allowance",
                                            }
                                        })?)
                                        .ok_or(DeliveryError::Corrupt {
                                            field: "cancel exceeds inbox settlement allowance",
                                        })?;
                                    self.save(&original).await?;
                                }
                                queries::write_inbox_cancel_link(
                                    self.scope,
                                    item_key(cancel.target()),
                                    item_key(id),
                                )
                                .await
                                .map_err(sql_error)?;
                                InboxProgress::Control {
                                    outcome: ControlOutcome::Pending,
                                }
                            }
                        };
                        let record =
                            InboxRecord::new(item, InboxRoute::Relayed, item_progress, settlement)?;
                        self.save(&record).await?;
                    }
                    let target = self
                        .header(id)
                        .await?
                        .ok_or(DeliveryError::CustodyMissing)?
                        .cancel_target
                        .unwrap_or(id);
                    let cancel_effects = self
                        .apply_pending_cancel(target, consumer, now, false)
                        .await?;
                    effects.attempts.extend(cancel_effects.attempts);
                    effects.reports.extend(cancel_effects.reports);
                }
                FetchEntry::Retired { items, .. } => {
                    for item in items {
                        queries::drop_inbox_item(self.scope, item_key(item))
                            .await
                            .map_err(sql_error)?;
                        queries::drop_inbox_cancel_link(self.scope, item_key(item))
                            .await
                            .map_err(sql_error)?;
                    }
                }
            }
        }
        progress.received = progress.received.max(batch.through);
        progress.rerun_requested |= advanced;
        self.save_state(&state).await?;
        // Cancellation may have ended an item in this same receive commit.
        let mut admitted = Vec::new();
        for attempt in effects.attempts {
            if self.load(attempt.item).await?.is_some_and(|record|matches!(record.header.progress,InboxProgress::Pending{attempt:current,..} if current==attempt.attempt)) {admitted.push(attempt);}
        }
        effects.attempts = admitted;
        Ok(effects)
    }

    pub async fn enqueue_local(
        &mut self,
        item: DeliveryItem<D>,
        now: SyncTime,
    ) -> Result<Option<DeliveryAttempt>, DeliveryError> {
        self.limits.validate::<D>()?;
        crate::delivery_sizes::check_item(&item)?;
        if item.recipient().peer != self.owner {
            return Err(DeliveryError::InvalidBatch);
        }
        if let Some(existing) = self.load(item.id()).await? {
            if existing.header.digest != item.digest().as_str() {
                return Err(DeliveryError::Corrupt {
                    field: "local item conflict",
                });
            }
            return Ok(None);
        }
        let input = DeliveryAttempt {
            item: item.id(),
            attempt: 0,
            not_before: now,
        };
        let record = InboxRecord::new(
            DeliveryEnvelope::Item(item),
            InboxRoute::Local,
            InboxProgress::Pending {
                attempt: 0,
                not_before: now,
            },
            self.limits.settlement_allowance,
        )?;
        let accounting = queries::read_inbox_accounting(self.scope)
            .await
            .map_err(sql_error)?;
        let used = accounting
            .bytes
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(DeliveryError::Corrupt {
                field: "local inbox accounting",
            })?;
        if used
            .checked_add(
                u64::try_from(
                    record
                        .payload
                        .as_ref()
                        .map(encode)
                        .transpose()?
                        .map_or(0, |raw| raw.len()),
                )
                .map_err(|_| DeliveryError::SequenceExhausted)?,
            )
            .and_then(|bytes| bytes.checked_add(u64::from(record.header.settlement_bytes)))
            .is_none_or(|bytes| bytes > self.limits.inbox_bytes)
        {
            return Err(DeliveryError::InvalidBatch);
        }
        self.save(&record).await?;
        Ok(Some(input))
    }
}
