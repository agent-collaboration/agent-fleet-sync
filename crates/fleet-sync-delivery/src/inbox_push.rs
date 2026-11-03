use crate::{
    DeliveryAttempt, DeliveryConsumer, DeliveryError, Inbox, InboxEffects, ReportRef,
    checked_queries as queries, custody_record::*, inbox_record::*,
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
    /// The host exposes the returned receipt only after committing this scope.
    /// Custody, the consumer projection and delivery/report admissions share it.
    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send avoids async-fn/GAT lifetime generality errors in journal transactions"
    )]
    pub fn accept_push<'call, TAttempt, TReport, TRetention, C>(
        &'call mut self,
        item: DeliveryEnvelope<D>,
        relay: PeerId,
        consumer: &'call C,
        now: SyncTime,
    ) -> impl std::future::Future<Output = Result<PushResult<D::Acceptance>, DeliveryError>> + Send + 'call
    where
        S: JournalScope + durable_steps::storage::JournalStorage,
        TRetention: Task<Input = ItemId>,
        TAttempt: Task<Input = DeliveryAttempt>,
        TReport: Task<Input = ReportRef>,
        C: DeliveryConsumer<S>,
        C::Protocol: ConsumerProtocol<Delivery = D>,
    {
        async move {
            let identity = item.id();
            let (answer, effects) = self.store_push(item, relay, consumer, now).await?;
            if matches!(
                answer,
                PushResult::Accepted { .. } | PushResult::Replayed { .. }
            ) {
                crate::admit_record_removal::<_, TRetention>(self.scope, identity).await?;
            }
            for attempt in effects.attempts {
                if self.load(attempt.item).await?.is_some_and(|record|matches!(record.header.progress,InboxProgress::Pending {attempt:current,..} if current==attempt.attempt)) {
                self.scope.admit::<TAttempt>(&crate::delivery_request_key(&(attempt.item,attempt.attempt))?,&attempt).await.map_err(|error|DeliveryError::Storage(error.to_string()))?;
            }
            }
            for report in effects.reports {
                self.scope
                    .admit::<TReport>(
                        &crate::delivery_request_key(&(report.item, report.report_seq))?,
                        &report,
                    )
                    .await
                    .map_err(|error| DeliveryError::Storage(error.to_string()))?;
            }
            Ok(answer)
        }
    }

    async fn store_push<C>(
        &mut self,
        item: DeliveryEnvelope<D>,
        relay: PeerId,
        consumer: &C,
        now: SyncTime,
    ) -> Result<(PushResult<D::Acceptance>, InboxEffects), DeliveryError>
    where
        C: DeliveryConsumer<S>,
        C::Protocol: ConsumerProtocol<Delivery = D>,
    {
        if crate::item_is_too_old(item.id(), now) {
            return Ok((
                PushResult::Refused {
                    reason: DeliveryRefusal::TooOld,
                },
                InboxEffects::default(),
            ));
        }
        self.limits.validate::<D>()?;
        crate::delivery_sizes::check_envelope(&item)?;
        if item.recipient().peer != self.owner {
            return Ok((
                PushResult::Refused {
                    reason: DeliveryRefusal::NotThisMachine,
                },
                InboxEffects::default(),
            ));
        }
        if let Some(existing) = self.load(item.id()).await? {
            if existing.header.digest != item.digest().as_str() {
                let digest = ItemDigest::try_from(existing.header.digest).map_err(|_| {
                    DeliveryError::Corrupt {
                        field: "push replay digest",
                    }
                })?;
                return Ok((
                    PushResult::Conflict { existing: digest },
                    InboxEffects::default(),
                ));
            }
            let (status, cancel) = inbox_status(&existing.header)?;
            return Ok((
                PushResult::Replayed { status, cancel },
                InboxEffects::default(),
            ));
        }
        if let DeliveryEnvelope::Cancel(cancel) = &item
            && let Some(link) = self.pending_cancel(cancel.target()).await?
        {
            return Ok((
                PushResult::Refused {
                    reason: DeliveryRefusal::CancelAlreadyRequested {
                        existing: DeliveryItemRef {
                            item: link.header.id,
                            digest: ItemDigest::try_from(link.header.digest).map_err(|_| {
                                DeliveryError::Corrupt {
                                    field: "canonical push cancel digest",
                                }
                            })?,
                        },
                    },
                },
                InboxEffects::default(),
            ));
        }
        let identity = item.id();
        let payload_size =
            u64::try_from(encode(&item)?.len()).map_err(|_| DeliveryError::SequenceExhausted)?;
        let mut original = match &item {
            DeliveryEnvelope::Cancel(cancel) => self.load(cancel.target()).await?,
            DeliveryEnvelope::Item(_) => None,
        };
        let reserve = match &item {
            DeliveryEnvelope::Item(item) => self.limits.settlement_for::<D>(item.body())?,
            DeliveryEnvelope::Cancel(_) => 0,
        };
        let prepaid = if let Some(original) = &mut original {
            if original.header.cancel_target.is_some() {
                return Err(DeliveryError::InvalidBatch);
            }
            if original.header.compact_local() {
                0
            } else {
                original.header.settlement_bytes = original
                    .header
                    .settlement_bytes
                    .checked_sub(
                        u32::try_from(payload_size)
                            .map_err(|_| DeliveryError::SequenceExhausted)?,
                    )
                    .ok_or(DeliveryError::Corrupt {
                        field: "push cancel exceeds reserve",
                    })?;
                payload_size
            }
        } else {
            0
        };
        let accounting = queries::read_inbox_accounting(self.scope)
            .await
            .map_err(sql_error)?;
        let used = accounting
            .bytes
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(DeliveryError::Corrupt {
                field: "push inbox accounting",
            })?;
        if used
            .checked_sub(prepaid)
            .and_then(|used| used.checked_add(payload_size))
            .and_then(|used| used.checked_add(u64::from(reserve)))
            .is_none_or(|total| total > self.limits.inbox_bytes)
        {
            return Ok((
                PushResult::Refused {
                    reason: DeliveryRefusal::MailboxFull,
                },
                InboxEffects::default(),
            ));
        }
        if let Some(original) = original {
            self.save(&original).await?;
        }
        let progress = match &item {
            DeliveryEnvelope::Item(_) => InboxProgress::Pending {
                attempt: 0,
                not_before: now,
            },
            DeliveryEnvelope::Cancel(cancel) => {
                queries::write_inbox_cancel_link(
                    self.scope,
                    item_key(cancel.target()),
                    item_key(identity),
                )
                .await
                .map_err(sql_error)?;
                InboxProgress::Control {
                    outcome: ControlOutcome::Pending,
                }
            }
        };
        let mut record = InboxRecord::new(item, InboxRoute::Relayed, progress, reserve, now)?;
        record.header.relay = Some(relay);
        self.save(&record).await?;
        if let Some(DeliveryEnvelope::Item(item)) = &record.payload {
            consumer.on_received(self.scope, item).await?;
        }
        let mut effects = self
            .apply_pending_cancel(
                record.header.cancel_target.unwrap_or(identity),
                consumer,
                now,
                false,
            )
            .await?;
        if let InboxProgress::Pending {
            attempt,
            not_before,
        } = record.header.progress
        {
            effects.attempts.push(DeliveryAttempt {
                item: identity,
                attempt,
                not_before,
            });
        }
        Ok((PushResult::Accepted { accepted_at: now }, effects))
    }
}

pub(crate) fn inbox_status<D: DeliveryTypes>(
    header: &InboxHeader<D>,
) -> Result<(DeliveryStatus<D::Acceptance>, Option<CancelResult>), DeliveryError> {
    let result = match &header.progress {
        InboxProgress::Pending { attempt, .. } if *attempt > 0 => (DeliveryStatus::Held, None),
        InboxProgress::Pending { .. } | InboxProgress::Attempting { .. } => {
            (DeliveryStatus::AtRecipient, None)
        }
        InboxProgress::Held { .. } => (DeliveryStatus::Held, None),
        InboxProgress::Final { outcome, .. } => match outcome {
            RecipientOutcome::Held => (DeliveryStatus::Held, None),
            RecipientOutcome::Delivered { acceptance } => (
                DeliveryStatus::Delivered {
                    acceptance: copy_wire(acceptance)?,
                },
                None,
            ),
            RecipientOutcome::Uncertain => (DeliveryStatus::Uncertain, None),
            RecipientOutcome::Expired => (DeliveryStatus::Expired, None),
            RecipientOutcome::Prevented { .. } => (DeliveryStatus::Cancelled, None),
            RecipientOutcome::TooLate { .. } => {
                return Err(DeliveryError::Corrupt {
                    field: "original too-late outcome",
                });
            }
            RecipientOutcome::Dropped => (
                DeliveryStatus::Refused {
                    reason: DeliveryRefusal::DroppedByPolicy,
                },
                None,
            ),
            RecipientOutcome::Refused { reason } => (
                DeliveryStatus::Refused {
                    reason: reason.clone(),
                },
                None,
            ),
        },
        InboxProgress::Control { outcome } => match outcome {
            ControlOutcome::Pending => (DeliveryStatus::AtRecipient, Some(CancelResult::Pending)),
            ControlOutcome::Prevented => (DeliveryStatus::Cancelled, Some(CancelResult::Prevented)),
            ControlOutcome::TooLate { at } => (
                DeliveryStatus::Cancelled,
                Some(CancelResult::TooLate { at: *at }),
            ),
            ControlOutcome::Refused { reason } => (
                DeliveryStatus::Refused {
                    reason: reason.clone(),
                },
                None,
            ),
            ControlOutcome::Expired => (DeliveryStatus::Expired, None),
        },
    };
    Ok(result)
}
