use crate::{
    DeliveryConsumer, DeliveryError, InboxEffects, checked_queries as queries, custody_record::*,
    inbox_record::*, inbox_storage::Inbox,
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
    pub(crate) async fn pending_cancel(
        &mut self,
        target: ItemId,
    ) -> Result<Option<InboxRecord<D>>, DeliveryError> {
        let Some(link) = queries::read_inbox_cancel_link(self.scope, item_key(target))
            .await
            .map_err(sql_error)?
        else {
            return Ok(None);
        };
        if link.target.as_deref() != Some(item_key(target).as_str()) {
            return Err(DeliveryError::Corrupt {
                field: "inbox cancel link target",
            });
        }
        let id = link.cancel_item.ok_or(DeliveryError::Corrupt {
            field: "inbox cancel link identity",
        })?;
        let id = ItemId::new(id.parse().map_err(|_| DeliveryError::Corrupt {
            field: "inbox cancel identity",
        })?)
        .map_err(|_| DeliveryError::Corrupt {
            field: "inbox cancel identity",
        })?;
        let cancel = self.load(id).await?.ok_or(DeliveryError::Corrupt {
            field: "inbox cancel item missing",
        })?;
        if !matches!(&cancel.payload,DeliveryEnvelope::Cancel(cancel) if cancel.target()==target) {
            return Err(DeliveryError::Corrupt {
                field: "inbox cancel target mismatch",
            });
        }
        Ok(Some(cancel))
    }

    pub(crate) async fn apply_pending_cancel<C>(
        &mut self,
        target: ItemId,
        consumer: &C,
        now: SyncTime,
        held_answer: bool,
    ) -> Result<InboxEffects, DeliveryError>
    where
        C: DeliveryConsumer<S>,
        C::Protocol: ConsumerProtocol<Delivery = D>,
    {
        let mut effects = InboxEffects::default();
        let Some(mut cancel) = self.pending_cancel(target).await? else {
            return Ok(effects);
        };
        if !matches!(
            cancel.header.progress,
            InboxProgress::Control {
                outcome: ControlOutcome::Pending
            }
        ) {
            return Ok(effects);
        }
        if let Some(header) = self.header(target).await?
            && let InboxProgress::Final {
                outcome:
                    RecipientOutcome::Refused {
                        reason: DeliveryRefusal::ProcessingFailed { code },
                    },
                ..
            } = header.progress
        {
            // Rejection needs no sender authorization or damaged original body.
            let reason = DeliveryRefusal::ProcessingFailed { code };
            cancel.header.progress = InboxProgress::Control {
                outcome: ControlOutcome::Refused {
                    reason: reason.clone(),
                },
            };
            effects.reports.extend(
                self.record_outcome(&mut cancel, RecipientOutcome::Refused { reason })
                    .await?,
            );
            self.save(&cancel).await?;
            return Ok(effects);
        }
        let Some(mut original) = self.load(target).await? else {
            return Ok(effects);
        };
        let (requester, item) = match (&cancel.payload, &original.payload) {
            (DeliveryEnvelope::Cancel(cancel), DeliveryEnvelope::Item(item)) => {
                (cancel.sender(), item)
            }
            _ => return Err(DeliveryError::InvalidBatch),
        };
        let outcome = if cancel
            .payload
            .valid_until()
            .is_some_and(|until| now >= until)
        {
            ControlOutcome::Expired
        } else if !consumer.may_cancel(requester, item) {
            ControlOutcome::Refused {
                reason: DeliveryRefusal::CancelSenderMismatch,
            }
        } else {
            match &original.header.progress {
                InboxProgress::Pending { .. } | InboxProgress::Held { .. } => {
                    ControlOutcome::Prevented
                }
                InboxProgress::Attempting { .. } if held_answer => ControlOutcome::Prevented,
                InboxProgress::Attempting { .. } => return Ok(effects),
                InboxProgress::Final {
                    outcome: RecipientOutcome::Delivered { .. },
                    ..
                } => ControlOutcome::TooLate {
                    at: TooLateAt::Delivered,
                },
                InboxProgress::Final {
                    outcome: RecipientOutcome::Uncertain,
                    ..
                } => ControlOutcome::TooLate {
                    at: TooLateAt::Uncertain,
                },
                InboxProgress::Final {
                    outcome: RecipientOutcome::Prevented { .. },
                    ..
                } => ControlOutcome::Prevented,
                InboxProgress::Final { .. } | InboxProgress::Control { .. } => {
                    ControlOutcome::Refused {
                        reason: DeliveryRefusal::DroppedByPolicy,
                    }
                }
            }
        };
        let cancel_id = cancel.payload.id();
        let report_outcome = match &outcome {
            ControlOutcome::Pending => return Ok(effects),
            ControlOutcome::Prevented => {
                if !matches!(original.header.progress, InboxProgress::Final { .. }) {
                    let original_outcome = RecipientOutcome::Prevented { cancel: cancel_id };
                    original.header.progress = InboxProgress::Final {
                        attempt: original.header.progress.attempt(),
                        outcome: copy_wire(&original_outcome)?,
                    };
                    original.header.release_requested = false;
                    effects
                        .reports
                        .extend(self.record_outcome(&mut original, original_outcome).await?);
                    self.save(&original).await?;
                }
                RecipientOutcome::Prevented { cancel: cancel_id }
            }
            ControlOutcome::TooLate { at } => {
                if let Some(reference) = self.pending_report(target).await? {
                    effects.reports.push(reference);
                }
                RecipientOutcome::TooLate {
                    cancel: cancel_id,
                    at: *at,
                }
            }
            ControlOutcome::Refused { reason } => RecipientOutcome::Refused {
                reason: reason.clone(),
            },
            ControlOutcome::Expired => RecipientOutcome::Expired,
        };
        cancel.header.progress = InboxProgress::Control { outcome };
        effects
            .reports
            .extend(self.record_outcome(&mut cancel, report_outcome).await?);
        self.save(&cancel).await?;
        Ok(effects)
    }
}
