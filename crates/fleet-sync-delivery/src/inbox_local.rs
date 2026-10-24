use crate::{
    DeliveryAttempt, DeliveryConsumer, DeliveryError, InboxEffects, LocalCancelAdmission,
    LocalItemAdmission, checked_queries as queries, custody_record::*, inbox_record::*,
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
    /// Admit an original only after resolving any pending Local control.
    /// Relayed controls keep their existing receive and completion paths.
    /// An Attempt payload has already been admitted as T before this returns.
    pub async fn enqueue_local_and_admit<T, C>(
        &mut self,
        item: DeliveryItem<D>,
        consumer: &C,
        now: SyncTime,
    ) -> Result<LocalItemAdmission, DeliveryError>
    where
        S: durable_steps::JournalScope,
        T: durable_steps::Task<Input = DeliveryAttempt>,
        C: DeliveryConsumer<S>,
        C::Protocol: ConsumerProtocol<Delivery = D>,
    {
        let admission = self.enqueue_local(item, consumer, now).await?;
        let LocalItemAdmission::Attempt(input) = admission else {
            return Ok(admission);
        };
        self.scope
            .admit::<T>(
                &crate::delivery_request_key(&(input.item, input.attempt))?,
                &input,
            )
            .await
            .map(|_| LocalItemAdmission::Attempt(input))
            .map_err(|error| DeliveryError::Storage(error.to_string()))
    }
    /// Take local item custody and resolve pending Local controls before delivery.
    /// Relayed controls are left untouched by this admission path.
    pub async fn enqueue_local<C>(
        &mut self,
        item: DeliveryItem<D>,
        consumer: &C,
        now: SyncTime,
    ) -> Result<LocalItemAdmission, DeliveryError>
    where
        C: DeliveryConsumer<S>,
        C::Protocol: ConsumerProtocol<Delivery = D>,
    {
        self.limits.validate::<D>()?;
        crate::delivery_sizes::check_item(&item)?;
        if item.recipient().peer != self.owner {
            return Err(DeliveryError::InvalidBatch);
        }
        if let Some(existing) = self.load(item.id()).await? {
            if existing.header.digest != item.digest().as_str() {
                let existing = ItemDigest::try_from(existing.header.digest).map_err(|_| {
                    DeliveryError::Corrupt {
                        field: "inbox header digest",
                    }
                })?;
                return Err(DeliveryError::Refused {
                    reason: DeliveryRefusal::ItemConflict { existing },
                });
            }
            return Ok(LocalItemAdmission::Replayed);
        }
        let input = DeliveryAttempt {
            item: item.id(),
            attempt: 0,
            not_before: now,
        };
        let mut settlement = self.limits.settlement_for::<D>(item.body())?;
        if let Some(cancel) = self.pending_cancel(item.id()).await? {
            let cancel_bytes = u32::try_from(
                encode(
                    cancel
                        .payload
                        .as_ref()
                        .ok_or(DeliveryError::CustodyMissing)?,
                )?
                .len(),
            )
            .map_err(|_| DeliveryError::Corrupt {
                field: "cancel exceeds inbox settlement allowance",
            })?;
            settlement = settlement
                .checked_sub(cancel_bytes)
                .ok_or(DeliveryError::Corrupt {
                    field: "cancel exceeds inbox settlement allowance",
                })?;
        }
        let record = InboxRecord::new(
            DeliveryEnvelope::Item(item),
            InboxRoute::Local,
            InboxProgress::Pending {
                attempt: 0,
                not_before: now,
            },
            settlement,
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
            return Err(DeliveryError::Refused {
                reason: DeliveryRefusal::MailboxFull,
            });
        }
        self.save(&record).await?;
        // Local admission returns an attempt, while relay intake owns its reports.
        // A pending local control is resolved before an attempt can be offered for delivery.
        if self
            .pending_cancel(input.item)
            .await?
            .is_some_and(|cancel| matches!(cancel.header.route, InboxRoute::Local))
        {
            self.apply_pending_cancel(input.item, consumer, now, false)
                .await?;
        }
        let stored = self.load(input.item).await?.ok_or(DeliveryError::Corrupt {
            field: "local item admission missing",
        })?;
        match stored.header.progress {
            InboxProgress::Pending { attempt: 0, .. } => Ok(LocalItemAdmission::Attempt(input)),
            InboxProgress::Final { .. } => Ok(LocalItemAdmission::SettledOnAdmission),
            InboxProgress::Pending { .. }
            | InboxProgress::Attempting { .. }
            | InboxProgress::Held { .. }
            | InboxProgress::Control { .. } => Err(DeliveryError::Corrupt {
                field: "local item admission state",
            }),
        }
    }

    /// Admit local cancellation custody and return the first cancel for its target.
    /// Replays add no rows or hooks. Controls remain charged for durable replay.
    pub async fn enqueue_local_cancel<C>(
        &mut self,
        cancel: DeliveryCancel<D>,
        consumer: &C,
        now: SyncTime,
    ) -> Result<LocalCancelAdmission, DeliveryError>
    where
        C: DeliveryConsumer<S>,
        C::Protocol: ConsumerProtocol<Delivery = D>,
    {
        self.limits.validate::<D>()?;
        let envelope = DeliveryEnvelope::Cancel(cancel);
        crate::delivery_sizes::check_envelope(&envelope)?;
        if envelope.recipient().peer != self.owner {
            return Err(DeliveryError::InvalidBatch);
        }
        if let Some(existing) = self.load(envelope.id()).await? {
            let digest = ItemDigest::try_from(existing.header.digest).map_err(|_| {
                DeliveryError::Corrupt {
                    field: "inbox header digest",
                }
            })?;
            if digest != *envelope.digest() {
                return Err(DeliveryError::Refused {
                    reason: DeliveryRefusal::ItemConflict { existing: digest },
                });
            }
            return Ok(LocalCancelAdmission {
                reference: DeliveryItemRef {
                    item: existing.header.id,
                    digest,
                },
                effects: InboxEffects::default(),
            });
        }
        let DeliveryEnvelope::Cancel(cancel) = &envelope else {
            return Err(DeliveryError::InvalidBatch);
        };
        let target = cancel.target();
        if let Some(existing) = self.pending_cancel(target).await? {
            let digest = ItemDigest::try_from(existing.header.digest).map_err(|_| {
                DeliveryError::Corrupt {
                    field: "inbox header digest",
                }
            })?;
            return Ok(LocalCancelAdmission {
                reference: DeliveryItemRef {
                    item: existing.header.id,
                    digest,
                },
                effects: InboxEffects::default(),
            });
        }
        let size = u64::try_from(encode(&envelope)?.len())
            .map_err(|_| DeliveryError::SequenceExhausted)?;
        let mut original = self.load(target).await?;
        let reserved = if let Some(original) = &mut original {
            if original.header.cancel_target.is_some() {
                return Err(DeliveryError::InvalidBatch);
            }
            // Compact local originals have no live charge, even when their header
            // retains the old settlement allowance. Only a live reserve can pay.
            if original.header.compact_local() {
                0
            } else {
                let charge = u32::try_from(size).map_err(|_| DeliveryError::Corrupt {
                    field: "cancel exceeds inbox settlement allowance",
                })?;
                original.header.settlement_bytes =
                    original.header.settlement_bytes.checked_sub(charge).ok_or(
                        DeliveryError::Corrupt {
                            field: "cancel exceeds inbox settlement allowance",
                        },
                    )?;
                size
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
                field: "local inbox accounting",
            })?;
        if used
            .checked_sub(reserved)
            .and_then(|bytes| bytes.checked_add(size))
            .is_none_or(|bytes| bytes > self.limits.inbox_bytes)
        {
            return Err(DeliveryError::Refused {
                reason: DeliveryRefusal::MailboxFull,
            });
        }
        if let Some(original) = original {
            self.save(&original).await?;
        }
        let reference = DeliveryItemRef {
            item: envelope.id(),
            digest: envelope.digest().clone(),
        };
        let record = InboxRecord::new(
            envelope,
            InboxRoute::Local,
            InboxProgress::Control {
                outcome: ControlOutcome::Pending,
            },
            0,
        )?;
        self.save(&record).await?;
        queries::write_inbox_cancel_link(self.scope, item_key(target), item_key(reference.item))
            .await
            .map_err(sql_error)?;
        let effects = self
            .apply_pending_cancel(target, consumer, now, false)
            .await?;
        Ok(LocalCancelAdmission { reference, effects })
    }
}
