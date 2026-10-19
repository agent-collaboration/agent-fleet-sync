use crate::{
    DeliveryAttempt, DeliveryCheck, DeliveryConsumer, DeliveryError, InboxEffects, LocalOutcome,
    ReportRef, checked_queries as queries, custody_record::*, inbox_record::*,
    inbox_storage::Inbox,
};
use durable_steps::{JournalScope, Task, storage::TransactionScope};
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
    pub async fn record_failure<C>(
        &mut self,
        input: DeliveryAttempt,
        kind: DeliveryFailureKind,
        consumer: &C,
        _now: SyncTime,
    ) -> Result<InboxEffects, DeliveryError>
    where
        C: DeliveryConsumer<S>,
        C::Protocol: ConsumerProtocol<Delivery = D>,
    {
        self.record_terminal(
            input,
            RecipientOutcome::Refused {
                reason: DeliveryRefusal::ProcessingFailed { kind },
            },
            consumer,
        )
        .await
    }
    pub(crate) async fn record_terminal<C>(
        &mut self,
        input: DeliveryAttempt,
        outcome: RecipientOutcome<D::Acceptance>,
        consumer: &C,
    ) -> Result<InboxEffects, DeliveryError>
    where
        C: DeliveryConsumer<S>,
        C::Protocol: ConsumerProtocol<Delivery = D>,
    {
        let mut header = self
            .header(input.item)
            .await?
            .ok_or(DeliveryError::CustodyMissing)?;
        if header.progress.attempt() != input.attempt
            || matches!(
                header.progress,
                InboxProgress::Final { .. } | InboxProgress::Control { .. }
            )
        {
            return Ok(InboxEffects::default());
        }
        header.progress = InboxProgress::Final {
            attempt: input.attempt,
            outcome: copy_wire(&outcome)?,
        };
        header.release_requested = false;
        let mut effects = InboxEffects::default();
        effects.reports.extend(
            self.record_header_outcome(&mut header, outcome, consumer)
                .await?,
        );
        self.update_header(&header).await?;
        Ok(effects)
    }
    pub async fn local_outcome(
        &mut self,
        item: ItemId,
    ) -> Result<Option<RecipientOutcome<D::Acceptance>>, DeliveryError> {
        let Some(header) = self.header(item).await? else {
            return Ok(None);
        };
        header.validate(self.owner)?;
        match header.progress {
            InboxProgress::Final { outcome, .. } => Ok(Some(outcome)),
            InboxProgress::Held { .. } => Ok(Some(RecipientOutcome::Held)),
            InboxProgress::Pending { attempt, .. } | InboxProgress::Attempting { attempt }
                if attempt > 0 =>
            {
                Ok(Some(RecipientOutcome::Held))
            }
            InboxProgress::Pending { .. }
            | InboxProgress::Attempting { .. }
            | InboxProgress::Control { .. } => Ok(None),
        }
    }
    pub async fn check_delivery(
        &mut self,
        input: DeliveryAttempt,
        now: SyncTime,
    ) -> Result<DeliveryCheck<D::Acceptance>, DeliveryError> {
        let Some(mut header) = self.header(input.item).await? else {
            return Err(DeliveryError::Corrupt {
                field: "delivery item missing",
            });
        };
        if header.progress.attempt() != input.attempt
            || matches!(header.progress, InboxProgress::Final { .. })
        {
            return Ok(DeliveryCheck::Finish { outcome: None });
        }
        if header.valid_until.is_some_and(|until| now >= until) {
            return Ok(DeliveryCheck::Finish {
                outcome: Some(RecipientOutcome::Expired),
            });
        }
        match header.progress {
            InboxProgress::Pending { not_before, .. } if now < not_before => {
                Ok(DeliveryCheck::Wait { until: not_before })
            }
            InboxProgress::Pending { .. } | InboxProgress::Attempting { .. } => {
                header.progress = InboxProgress::Attempting {
                    attempt: input.attempt,
                };
                let reference = DeliveryItemRef {
                    item: input.item,
                    digest: ItemDigest::try_from(header.digest.clone()).map_err(|_| {
                        DeliveryError::Corrupt {
                            field: "inbox header digest",
                        }
                    })?,
                };
                self.update_header(&header).await?;
                Ok(DeliveryCheck::Deliver { reference })
            }
            InboxProgress::Held { .. } => Ok(DeliveryCheck::Finish { outcome: None }),
            InboxProgress::Final { .. } => Ok(DeliveryCheck::Finish { outcome: None }),
            InboxProgress::Control { .. } => Err(DeliveryError::InvalidBatch),
        }
    }

    pub(crate) async fn complete_attempt<C>(
        &mut self,
        input: DeliveryAttempt,
        outcome: LocalOutcome<D::Acceptance>,
        consumer: &C,
        now: SyncTime,
    ) -> Result<InboxEffects, DeliveryError>
    where
        C: DeliveryConsumer<S>,
        C::Protocol: ConsumerProtocol<Delivery = D>,
    {
        if let LocalOutcome::Delivered { acceptance } = &outcome {
            crate::delivery_sizes::check_status::<D>(&DeliveryStatus::Delivered {
                acceptance: copy_wire(acceptance)?,
            })?;
        }
        let mut effects = if matches!(&outcome, LocalOutcome::Held { .. }) {
            self.apply_pending_cancel(input.item, consumer, now, true)
                .await?
        } else {
            InboxEffects::default()
        };
        let mut record = self.load(input.item).await?.ok_or(DeliveryError::Corrupt {
            field: "delivery outcome item missing",
        })?;
        if record.header.progress.attempt() != input.attempt
            || !matches!(
                record.header.progress,
                InboxProgress::Pending { .. } | InboxProgress::Attempting { .. }
            )
        {
            return Ok(effects);
        }
        let recipient_outcome = match outcome {
            LocalOutcome::Held { until } => {
                let deadline = if record.header.release_requested {
                    Some(now)
                } else {
                    match (until, record.header.valid_until) {
                        (Some(until), Some(expiry)) => Some(until.min(expiry)),
                        (Some(until), None) => Some(until),
                        (None, expiry) => expiry,
                    }
                };
                if let Some(deadline) = deadline {
                    let attempt = input
                        .attempt
                        .checked_add(1)
                        .filter(|attempt| *attempt <= i64::MAX as u64)
                        .ok_or(DeliveryError::SequenceExhausted)?;
                    record.header.progress = InboxProgress::Pending {
                        attempt,
                        not_before: deadline,
                    };
                    effects.attempts.push(DeliveryAttempt {
                        item: input.item,
                        attempt,
                        not_before: deadline,
                    });
                } else {
                    record.header.progress = InboxProgress::Held {
                        attempt: input.attempt,
                    };
                }
                RecipientOutcome::Held
            }
            LocalOutcome::Delivered { acceptance } => RecipientOutcome::Delivered { acceptance },
            LocalOutcome::Uncertain => RecipientOutcome::Uncertain,
            LocalOutcome::Expired => RecipientOutcome::Expired,
            LocalOutcome::Dropped => RecipientOutcome::Dropped,
            LocalOutcome::Refused { reason } => RecipientOutcome::Refused { reason },
        };
        if !matches!(recipient_outcome, RecipientOutcome::Held) {
            record.header.progress = InboxProgress::Final {
                attempt: input.attempt,
                outcome: copy_wire(&recipient_outcome)?,
            };
        }
        record.header.release_requested = false;
        effects.reports.extend(
            self.record_outcome(&mut record, recipient_outcome, consumer)
                .await?,
        );
        self.save(&record).await?;
        let cancellation = self
            .apply_pending_cancel(input.item, consumer, now, false)
            .await?;
        effects.attempts.extend(cancellation.attempts);
        effects.reports.extend(cancellation.reports);
        Ok(effects)
    }

    pub async fn release_held<T: Task<Input = DeliveryAttempt>>(
        &mut self,
        item: ItemId,
        now: SyncTime,
    ) -> Result<Option<durable_steps::Admitted>, DeliveryError>
    where
        S: JournalScope,
    {
        let Some(mut header) = self.header(item).await? else {
            return Ok(None);
        };
        if header.valid_until.is_some_and(|until| now >= until) {
            return Ok(None);
        }
        let next = match header.progress {
            InboxProgress::Pending { .. } | InboxProgress::Attempting { .. } => {
                header.release_requested = true;
                None
            }
            InboxProgress::Held { attempt } => {
                let attempt = attempt
                    .checked_add(1)
                    .filter(|attempt| *attempt <= i64::MAX as u64)
                    .ok_or(DeliveryError::SequenceExhausted)?;
                header.progress = InboxProgress::Pending {
                    attempt,
                    not_before: now,
                };
                Some(DeliveryAttempt {
                    item,
                    attempt,
                    not_before: now,
                })
            }
            InboxProgress::Final { .. } | InboxProgress::Control { .. } => None,
        };
        self.update_header(&header).await?;
        match next {
            Some(input) => {
                let key = durable_steps::RequestKey::new(encode(&(input.item, input.attempt))?)
                    .map_err(|error| DeliveryError::Encoding(error.to_string()))?;
                let admitted = self
                    .scope
                    .admit::<T>(&key, &input)
                    .await
                    .map_err(|error| DeliveryError::Storage(error.to_string()))?;
                Ok(Some(admitted))
            }
            None => Ok(None),
        }
    }

    pub async fn pending_report(
        &mut self,
        item: ItemId,
    ) -> Result<Option<ReportRef>, DeliveryError> {
        let Some(header) = self.header(item).await? else {
            return Ok(None);
        };
        if header.report_seq.value() == 0 {
            return Ok(None);
        }
        let reference = ReportRef {
            item,
            report_seq: header.report_seq,
        };
        Ok(queries::read_report(
            self.scope,
            item_key(item),
            header.report_seq.value().to_string(),
        )
        .await
        .map_err(sql_error)?
        .map(|_| reference))
    }
}
