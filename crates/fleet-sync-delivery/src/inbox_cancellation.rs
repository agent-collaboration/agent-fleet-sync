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
        let id = ItemId::try_from(id).map_err(|_| DeliveryError::Corrupt {
            field: "inbox cancel identity",
        })?;
        let cancel = self.load(id).await?.ok_or(DeliveryError::Corrupt {
            field: "inbox cancel item missing",
        })?;
        if cancel.header.cancel_target != Some(target) {
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
        let Some(mut original) = self.header(target).await? else {
            return Ok(effects);
        };
        original.validate(self.owner)?;
        let authority = original.delivery_header();
        let outcome = if cancel.header.valid_until.is_some_and(|until| now >= until) {
            ControlOutcome::Expired
        } else if let Err(DeliveryError::Corrupt { .. }) = authority {
            ControlOutcome::Refused {
                reason: DeliveryRefusal::ProcessingFailed {
                    kind: DeliveryFailureKind::CustodyCorrupt,
                },
            }
        } else if !consumer.may_cancel(&cancel.header.sender, &authority?) {
            ControlOutcome::Refused {
                reason: DeliveryRefusal::CancelSenderMismatch,
            }
        } else {
            match &original.progress {
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
                InboxProgress::Final {
                    outcome: RecipientOutcome::Expired,
                    ..
                } => ControlOutcome::Expired,
                InboxProgress::Final {
                    outcome: RecipientOutcome::Refused { reason },
                    ..
                } => ControlOutcome::Refused {
                    reason: reason.clone(),
                },
                InboxProgress::Final {
                    outcome: RecipientOutcome::Dropped,
                    ..
                } => ControlOutcome::Refused {
                    reason: DeliveryRefusal::DroppedByPolicy,
                },
                InboxProgress::Final { .. } | InboxProgress::Control { .. } => {
                    return Err(DeliveryError::Corrupt {
                        field: "original item control outcome",
                    });
                }
            }
        };
        let cancel_id = cancel.header.id;
        let report_outcome = match &outcome {
            ControlOutcome::Pending => return Ok(effects),
            ControlOutcome::Prevented => {
                if !matches!(original.progress, InboxProgress::Final { .. }) {
                    let original_outcome = RecipientOutcome::Prevented { cancel: cancel_id };
                    original.progress = InboxProgress::Final {
                        attempt: original.progress.attempt(),
                        outcome: copy_wire(&original_outcome)?,
                    };
                    original.release_requested = false;
                    effects.reports.extend(
                        self.record_header_outcome(&mut original, original_outcome, consumer)
                            .await?,
                    );
                    self.update_header(&original).await?;
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
        effects.reports.extend(
            self.record_outcome(&mut cancel, report_outcome, consumer)
                .await?,
        );
        self.save(&cancel).await?;
        Ok(effects)
    }
}
