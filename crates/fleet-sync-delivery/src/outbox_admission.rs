use crate::{DeliveryError, checked_queries as queries, custody_record::*, outbox_record::*};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

use crate::Outbox;
impl<S, D> Outbox<'_, S, D>
where
    S: TransactionScope,
    D: DeliveryTypes,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database> + Encode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub async fn enqueue_and_admit<T: durable_steps::Task<Input = SubmitInput>>(
        &mut self,
        item: DeliveryItem<D>,
        relay: PeerId,
    ) -> Result<
        (
            OutboxAdmission<D::Acceptance>,
            Option<durable_steps::Admitted>,
        ),
        DeliveryError,
    >
    where
        S: durable_steps::JournalScope,
    {
        let mailbox = item.recipient().peer;
        let answer = self.enqueue(item, relay).await?;
        let admitted = self.admit_submission::<T>(&answer, relay, mailbox).await?;
        Ok((answer, admitted))
    }
    pub async fn enqueue_cancel_and_admit<T: durable_steps::Task<Input = SubmitInput>, C>(
        &mut self,
        cancel: DeliveryCancel<D>,
        relay: PeerId,

        consumer: &C,
    ) -> Result<
        (
            OutboxAdmission<D::Acceptance>,
            Option<durable_steps::Admitted>,
        ),
        DeliveryError,
    >
    where
        S: durable_steps::JournalScope,
        C: crate::DeliveryConsumer<S>,
        C::Protocol: ConsumerProtocol<Delivery = D>,
    {
        let mailbox = cancel.recipient().peer;
        let answer = self.enqueue_cancel(cancel, relay, consumer).await?;
        let admitted = self.admit_submission::<T>(&answer, relay, mailbox).await?;
        Ok((answer, admitted))
    }
    async fn admit_submission<T: durable_steps::Task<Input = SubmitInput>>(
        &mut self,
        answer: &OutboxAdmission<D::Acceptance>,
        relay: PeerId,
        mailbox: PeerId,
    ) -> Result<Option<durable_steps::Admitted>, DeliveryError>
    where
        S: durable_steps::JournalScope,
    {
        let OutboxAdmission::Queued { reference } = answer else {
            return Ok(None);
        };
        let input = SubmitInput {
            item: reference.item,
            digest: reference.digest.clone(),
            relay,
            mailbox,
        };
        self.scope
            .admit::<T>(&crate::delivery_request_key(&input.item)?, &input)
            .await
            .map(Some)
            .map_err(|error| DeliveryError::Storage(error.to_string()))
    }
    pub async fn enqueue(
        &mut self,
        item: DeliveryItem<D>,
        relay: PeerId,
    ) -> Result<OutboxAdmission<D::Acceptance>, DeliveryError> {
        self.enqueue_envelope(DeliveryEnvelope::Item(item), relay, false)
            .await
    }
    pub async fn enqueue_cancel<C>(
        &mut self,
        cancel: DeliveryCancel<D>,
        relay: PeerId,

        consumer: &C,
    ) -> Result<OutboxAdmission<D::Acceptance>, DeliveryError>
    where
        C: crate::DeliveryConsumer<S>,
        C::Protocol: ConsumerProtocol<Delivery = D>,
    {
        self.limits.validate::<D>()?;
        let envelope = DeliveryEnvelope::Cancel(cancel);
        crate::delivery_sizes::check_envelope(&envelope)?;
        if let Some(answer) = self.existing_admission(&envelope).await? {
            return Ok(answer);
        }
        let DeliveryEnvelope::Cancel(cancel) = envelope else {
            return Err(DeliveryError::InvalidBatch);
        };
        if queries::read_outbox_cancel_link(self.scope, item_key(cancel.target()))
            .await
            .map_err(sql_error)?
            .is_some()
        {
            return self
                .enqueue_envelope(DeliveryEnvelope::Cancel(cancel), relay, false)
                .await;
        }
        let mut withdrawn = false;
        if let Some(mut original) = self.load(cancel.target()).await? {
            if original.header.recipient != cancel.recipient().peer
                || original.header.relay != relay
            {
                return Err(DeliveryError::InvalidBatch);
            }
            if !original.header.submit_attempted && !original.header.status.is_terminal() {
                let Some(DeliveryEnvelope::Item(item)) = &original.payload else {
                    return Err(DeliveryError::CustodyMissing);
                };
                let header = delivery_header::<D>(
                    item.id(),
                    item.sender(),
                    item.recipient(),
                    item.valid_until(),
                    item.digest(),
                )?;
                if !consumer.may_cancel(cancel.sender(), &header) {
                    return Ok(OutboxAdmission::Refused {
                        reason: DeliveryRefusal::CancelSenderMismatch,
                    });
                }
                original.header.status = DeliveryStatus::Cancelled;
                original.payload = None;
                self.save(&original).await?;
                withdrawn = true;
            }
        }
        self.enqueue_envelope(DeliveryEnvelope::Cancel(cancel), relay, withdrawn)
            .await
    }
    async fn existing_admission(
        &mut self,
        envelope: &DeliveryEnvelope<D>,
    ) -> Result<Option<OutboxAdmission<D::Acceptance>>, DeliveryError> {
        let Some(state) = self.header_state(envelope.id()).await? else {
            return Ok(None);
        };
        let (digest, status, version) = match state {
            OutboxHeaderState::Unresolvable { kind, .. } => {
                return Err(DeliveryError::ProcessingFailed { kind });
            }
            OutboxHeaderState::Recovered(record) => (record.digest, record.status, record.version),
            OutboxHeaderState::Item(header) => {
                header.validate()?;
                (
                    ItemDigest::try_from(header.digest).map_err(|_| DeliveryError::Corrupt {
                        field: "outbox replay digest",
                    })?,
                    header.status,
                    header.version,
                )
            }
        };
        Ok(Some(if digest == *envelope.digest() {
            OutboxAdmission::Replayed { status, version }
        } else {
            OutboxAdmission::Refused {
                reason: DeliveryRefusal::ItemConflict { existing: digest },
            }
        }))
    }
    async fn enqueue_envelope(
        &mut self,
        envelope: DeliveryEnvelope<D>,
        relay: PeerId,

        withdrawn: bool,
    ) -> Result<OutboxAdmission<D::Acceptance>, DeliveryError> {
        self.limits.validate::<D>()?;
        crate::delivery_sizes::check_envelope(&envelope)?;
        if let Some(answer) = self.existing_admission(&envelope).await? {
            return Ok(answer);
        }
        let cancel_target = match &envelope {
            DeliveryEnvelope::Item(_) => None,
            DeliveryEnvelope::Cancel(cancel) => Some(cancel.target()),
        };
        let mut capacity_slot = true;
        let mut acceptance_bound = crate::delivery_sizes::envelope_acceptance_bound(&envelope)?;
        let mut settlement = match &envelope {
            DeliveryEnvelope::Item(item) => self.limits.settlement_for::<D>(item.body())?,
            DeliveryEnvelope::Cancel(_) => self.limits.settlement_allowance,
        };
        if let Some(target) = cancel_target {
            if let Some(link) = queries::read_outbox_cancel_link(self.scope, item_key(target))
                .await
                .map_err(sql_error)?
            {
                let cancel_id: ItemId = decode(
                    link.cancel_item.map(|id| format!("\"{id}\"")),
                    "outbox cancel link",
                )?;
                let (digest, status, version) = match self.header_state(cancel_id).await? {
                    Some(OutboxHeaderState::Item(header)) => (
                        ItemDigest::try_from(header.digest).map_err(|_| {
                            DeliveryError::Corrupt {
                                field: "outbox cancel digest",
                            }
                        })?,
                        header.status,
                        header.version,
                    ),
                    Some(OutboxHeaderState::Recovered(record)) => {
                        (record.digest, record.status, record.version)
                    }
                    Some(OutboxHeaderState::Unresolvable { kind, .. }) => {
                        return Err(DeliveryError::ProcessingFailed { kind });
                    }
                    None => {
                        return Err(DeliveryError::Corrupt {
                            field: "outbox cancel link missing item",
                        });
                    }
                };
                return Ok(OutboxAdmission::CancelAlreadyRequested {
                    existing: DeliveryItemRef {
                        item: cancel_id,
                        digest,
                    },
                    status,
                    version,
                });
            }
            if let Some(OutboxHeaderState::Item(mut original)) = self.header_state(target).await? {
                if original.recipient != envelope.recipient().peer
                    || original.cancel_target.is_some()
                {
                    return Err(DeliveryError::InvalidBatch);
                }
                let charge = u32::try_from(encode(&envelope)?.len())
                    .map_err(|_| DeliveryError::SequenceExhausted)?;
                original.settlement_bytes = original.settlement_bytes.checked_sub(charge).ok_or(
                    DeliveryError::Corrupt {
                        field: "cancel exceeds outbox settlement allowance",
                    },
                )?;
                self.update_header(&original).await?;
                acceptance_bound = original.acceptance_bound;
                capacity_slot = false;
                settlement = 0;
            }
        }
        let bytes = u64::try_from(encode(&envelope)?.len())
            .map_err(|_| DeliveryError::SequenceExhausted)?;
        let accounting = queries::read_outbox_accounting(self.scope)
            .await
            .map_err(sql_error)?;
        let count = decimal(accounting.items, "outbox count")?;
        let used = decimal(accounting.bytes, "outbox occupied bytes")?;
        if capacity_slot
            && (count >= self.limits.outbox_items
                || used
                    .checked_add(bytes)
                    .and_then(|bytes| bytes.checked_add(u64::from(settlement)))
                    .is_none_or(|bytes| bytes > self.limits.outbox_bytes))
        {
            return Ok(OutboxAdmission::Refused {
                reason: DeliveryRefusal::MailboxFull,
            });
        }
        let reference = DeliveryItemRef {
            item: envelope.id(),
            digest: envelope.digest().clone(),
        };
        let header = OutboxHeader {
            id: reference.item,
            digest: reference.digest.as_str().to_owned(),
            relay,

            recipient: envelope.recipient().peer,
            valid_until: envelope.valid_until(),
            status: if withdrawn {
                DeliveryStatus::Cancelled
            } else {
                DeliveryStatus::Queued
            },
            version: StatusVersion::new(0),
            submit_attempted: false,
            cancel: cancel_target.map(|_| {
                if withdrawn {
                    CancelResult::Prevented
                } else {
                    CancelResult::Pending
                }
            }),
            cancel_target,
            acceptance_bound,
            settlement_bytes: settlement,
            capacity_slot,
        };
        let record = OutboxRecord {
            header,
            payload: if withdrawn { None } else { Some(envelope) },
        };
        self.save(&record).await?;
        if let Some(target) = cancel_target {
            queries::write_outbox_cancel_link(
                self.scope,
                item_key(target),
                item_key(reference.item),
            )
            .await
            .map_err(sql_error)?;
        }
        Ok(OutboxAdmission::Queued { reference })
    }
}
fn decimal(value: Option<String>, field: &'static str) -> Result<u64, DeliveryError> {
    value
        .and_then(|value| value.parse().ok())
        .ok_or(DeliveryError::Corrupt { field })
}
