use crate::{
    DeliveryError, Outbox, OutboxAdmission, PublicationRef, StatusStream,
    checked_queries as queries, custody_record::*, outbox_record::*,
};
use durable_steps::{JournalScope, Task, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

impl<S, D> Outbox<'_, S, D>
where
    S: TransactionScope,
    D: DeliveryTypes,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database> + Encode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    /// Admit the next hop and its Push task in the caller's custody transaction.
    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send avoids async-fn/GAT lifetime generality errors in journal transactions"
    )]
    pub fn submit_relay<'call, TPush>(
        &'call mut self,
        submitter: PeerId,
        item: DeliveryEnvelope<D>,
        relay: PeerId,
        route: D::StatusRoute,
        now: SyncTime,
    ) -> impl std::future::Future<Output = Result<SubmitResult<D::Acceptance>, DeliveryError>>
    + Send
    + 'call
    where
        S: JournalScope,
        TPush: Task<Input = PushInput>,
    {
        async move {
            crate::delivery_sizes::check_status_route::<D>(&route)?;
            let reference = DeliveryItemRef {
                item: item.id(),
                digest: item.digest().clone(),
            };
            let answer = self
                .enqueue_envelope(item, relay, submitter, now, false)
                .await?;
            match answer {
                OutboxAdmission::Refused { reason } => return Ok(SubmitResult::Refused { reason }),
                OutboxAdmission::Replayed { status, version } => {
                    let cancel = self
                        .load(reference.item)
                        .await?
                        .ok_or(DeliveryError::CustodyMissing)?
                        .header
                        .cancel;
                    return Ok(SubmitResult::Replayed {
                        status,
                        version,
                        cancel,
                    });
                }
                OutboxAdmission::CancelAlreadyRequested {
                    existing,
                    status,
                    version,
                } => {
                    return Ok(SubmitResult::CancelAlreadyRequested {
                        existing,
                        status,
                        version,
                        cancel: None,
                    });
                }
                OutboxAdmission::Queued { .. } => {}
            }
            let mut record = self
                .load(reference.item)
                .await?
                .ok_or(DeliveryError::CustodyMissing)?;
            record.header.status = DeliveryStatus::Sent;
            record.header.version = StatusVersion::new(1);
            record.header.submit_attempted = true;
            record.header.status_route = Some(encode(&route)?);
            if record.header.valid_until.is_some_and(|until| now >= until) {
                record.header.status = DeliveryStatus::Expired;
                record.payload = None;
            }
            let status = copy_wire(&record.header.status)?;
            let cancel = record.header.cancel;
            let terminal = record.header.status.is_terminal();
            self.save(&record).await?;
            if !terminal {
                let input = PushInput {
                    reference,
                    recipient: record.header.recipient,
                };
                self.scope
                    .admit::<TPush>(&crate::delivery_request_key(&input.reference.item)?, &input)
                    .await
                    .map_err(|error| DeliveryError::Storage(error.to_string()))?;
            }
            Ok(SubmitResult::Accepted {
                status,
                version: StatusVersion::new(1),
                cancel,
            })
        }
    }

    pub async fn resolve_push(
        &mut self,
        reference: &DeliveryItemRef,
    ) -> Result<Option<DeliveryEnvelope<D>>, DeliveryError> {
        let Some(record) = self.load(reference.item).await? else {
            return Ok(None);
        };
        if record.header.digest != reference.digest.as_str() {
            return Err(DeliveryError::DigestMismatch);
        }
        Ok(record.payload)
    }

    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send avoids async-fn/GAT lifetime generality errors in journal transactions"
    )]
    pub fn receipt<'call, TPublish>(
        &'call mut self,
        recipient: PeerId,
        reference: &'call DeliveryItemRef,
        answer: PushResult<D::Acceptance>,
        now: SyncTime,
    ) -> impl std::future::Future<Output = Result<StatusPublicationResult, DeliveryError>> + Send + 'call
    where
        S: JournalScope,
        TPublish: Task<Input = PublicationRef<D::StatusRoute>>,
    {
        async move {
            let mut record = self
                .load(reference.item)
                .await?
                .ok_or(DeliveryError::CustodyMissing)?;
            if recipient != record.header.recipient {
                return Ok(StatusPublicationResult::Refused {
                    reason: DeliveryRefusal::NotRecipient,
                });
            }
            if reference.digest.as_str() != record.header.digest {
                return Err(DeliveryError::DigestMismatch);
            }
            let status = match answer {
                PushResult::Accepted { .. } => DeliveryStatus::AtRecipient,
                PushResult::Replayed { status, .. } => {
                    if status.rank() < DeliveryStatus::<D::Acceptance>::AtRecipient.rank() {
                        return Err(DeliveryError::InvalidBatch);
                    }
                    status
                }
                PushResult::Conflict { existing } => DeliveryStatus::Refused {
                    reason: DeliveryRefusal::ItemConflict { existing },
                },
                PushResult::Refused {
                    reason: DeliveryRefusal::MailboxFull,
                } => {
                    return Ok(StatusPublicationResult::Refused {
                        reason: DeliveryRefusal::MailboxFull,
                    });
                }
                PushResult::Refused { reason } => DeliveryStatus::Refused { reason },
            };
            crate::delivery_sizes::check_status_bound::<D>(
                &status,
                record.header.acceptance_bound,
            )?;
            let changed = record.header.status.may_advance_to(&status);
            if !changed && record.payload.is_none() && record.header.handed_off_at.is_some() {
                return Ok(StatusPublicationResult::Applied);
            }
            if changed {
                record.header.status = status;
                record.header.version = next_version(record.header.version)?;
            }
            record.header.handed_off_at.get_or_insert(now);
            record.payload = None;
            self.save(&record).await?;
            if changed {
                self.owe_relay_status::<TPublish>(&record.header).await?;
            }
            Ok(StatusPublicationResult::Applied)
        }
    }

    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send avoids async-fn/GAT lifetime generality errors in journal transactions"
    )]
    pub fn recipient_report<'call, TPublish>(
        &'call mut self,
        recipient: PeerId,
        report: RecipientReport<D>,
        now: SyncTime,
    ) -> impl std::future::Future<Output = Result<ReportResult, DeliveryError>> + Send + 'call
    where
        S: JournalScope,
        TPublish: Task<Input = PublicationRef<D::StatusRoute>>,
    {
        async move {
            let mut record = self
                .load(report.item)
                .await?
                .ok_or(DeliveryError::CustodyMissing)?;
            if recipient != record.header.recipient {
                return Ok(ReportResult::Refused {
                    reason: DeliveryRefusal::NotRecipient,
                });
            }
            if report.report_seq.value() == 0 || report.report_seq.value() > i64::MAX as u64 {
                return Err(DeliveryError::InvalidBatch);
            }
            crate::delivery_sizes::check_outcome_bound::<D>(
                &report.outcome,
                record.header.acceptance_bound,
            )?;
            if report.report_seq <= record.header.last_report_seq {
                return Ok(ReportResult::Replayed {
                    report_seq: report.report_seq,
                });
            }
            let is_cancel = record.header.cancel_target.is_some();
            let (status, cancel) = match report.outcome {
                RecipientOutcome::Held => (DeliveryStatus::Held, None),
                RecipientOutcome::Delivered { acceptance } => {
                    (DeliveryStatus::Delivered { acceptance }, None)
                }
                RecipientOutcome::Uncertain => (DeliveryStatus::Uncertain, None),
                RecipientOutcome::Expired => (DeliveryStatus::Expired, None),
                RecipientOutcome::Dropped => (
                    DeliveryStatus::Refused {
                        reason: DeliveryRefusal::DroppedByPolicy,
                    },
                    None,
                ),
                RecipientOutcome::Refused { reason } => (DeliveryStatus::Refused { reason }, None),
                RecipientOutcome::Prevented { cancel } => {
                    self.validate_relay_cancel(&record.header, cancel).await?;
                    (
                        DeliveryStatus::Cancelled,
                        is_cancel.then_some(CancelResult::Prevented),
                    )
                }
                RecipientOutcome::TooLate { cancel, at } => {
                    self.validate_relay_cancel(&record.header, cancel).await?;
                    if is_cancel {
                        (
                            DeliveryStatus::Cancelled,
                            Some(CancelResult::TooLate { at }),
                        )
                    } else if record.header.status.is_terminal() {
                        (copy_wire(&record.header.status)?, None)
                    } else {
                        return Err(DeliveryError::InvalidBatch);
                    }
                }
            };
            record.header.last_report_seq = report.report_seq;
            let changed = record.header.status.may_advance_to(&status);
            if changed {
                record.header.status = status;
                record.header.version = next_version(record.header.version)?;
            }
            if let Some(cancel) = cancel {
                record.header.cancel = Some(cancel);
            }
            // A recipient's outcome also proves that its inbox committed the item.
            record.header.handed_off_at.get_or_insert(now);
            record.payload = None;
            self.save(&record).await?;
            if changed {
                self.owe_relay_status::<TPublish>(&record.header).await?;
            }
            Ok(ReportResult::Ack {
                report_seq: report.report_seq,
            })
        }
    }

    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send avoids async-fn/GAT lifetime generality errors in journal transactions"
    )]
    fn owe_relay_status<'call, TPublish>(
        &'call mut self,
        header: &'call OutboxHeader<D::Acceptance>,
    ) -> impl std::future::Future<Output = Result<(), DeliveryError>> + Send + 'call
    where
        S: JournalScope,
        TPublish: Task<Input = PublicationRef<D::StatusRoute>>,
    {
        async move {
            let sender = header.submitter.ok_or(DeliveryError::Corrupt {
                field: "relay submitter",
            })?;
            let route: D::StatusRoute = decode(header.status_route.clone(), "relay status route")?;
            StatusStream::<_, D>::new(self.scope, sender, route)
                .append_and_admit::<TPublish>(&StatusPublication {
                    item: header.id,
                    status: copy_wire(&header.status)?,
                    version: header.version,
                    cancel: header.cancel,
                })
                .await?;
            Ok(())
        }
    }

    async fn validate_relay_cancel(
        &mut self,
        header: &OutboxHeader<D::Acceptance>,
        cancel: ItemId,
    ) -> Result<(), DeliveryError> {
        let target = header.cancel_target.unwrap_or(header.id);
        let link = queries::read_outbox_cancel_link(self.scope, item_key(target))
            .await
            .map_err(sql_error)?
            .ok_or(DeliveryError::InvalidBatch)?;
        if link.cancel_item.as_deref() != Some(item_key(cancel).as_str())
            || header.cancel_target.is_some() && header.id != cancel
        {
            return Err(DeliveryError::InvalidBatch);
        }
        Ok(())
    }
}
