use crate::{
    DeliveryError, Outbox, OutboxAdmission, PublicationRef, StatusStream, custody_record::*,
    delivery_storage_rows as queries, outbox_record::*,
};
use durable_steps::{JournalScope, Task, storage::TransactionScope};
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

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
    /// Admit machine or project mail and its Push task in the custody transaction.
    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send avoids async-fn/GAT lifetime generality errors in journal transactions"
    )]
    pub fn submit_relay<'call, TPush, TRetention>(
        &'call mut self,
        submitter: crate::Submitter,
        item: DeliveryEnvelope<D>,
        relay: PeerId,
        route: D::StatusRoute,
        now: SyncTime,
    ) -> impl std::future::Future<Output = Result<SubmitResult<D::Acceptance>, DeliveryError>>
    + Send
    + 'call
    where
        S: JournalScope + durable_steps::storage::JournalStorage,
        TRetention: Task<Input = ItemId>,
        TPush: Task<Input = PushInput>,
    {
        async move {
            if matches!(submitter, Submitter::Fleet(_)) {
                return Ok(SubmitResult::Refused {
                    reason: DeliveryRefusal::NotSubmitter,
                });
            }
            self.admit_relay::<TPush, TRetention>(submitter, item, relay, route, now)
                .await
        }
    }

    /// Admit Fleet-origin mail from the Fleet's own handlers in its custody transaction.
    /// Hosts restrict access to those handlers; the sender binding verifies identity.
    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send avoids async-fn/GAT lifetime generality errors in journal transactions"
    )]
    pub fn submit_fleet<'call, TPush, TRetention>(
        &'call mut self,
        fleet: FleetId,
        item: DeliveryEnvelope<D>,
        relay: PeerId,
        route: D::StatusRoute,
        now: SyncTime,
    ) -> impl std::future::Future<Output = Result<SubmitResult<D::Acceptance>, DeliveryError>>
    + Send
    + 'call
    where
        S: JournalScope + durable_steps::storage::JournalStorage,
        TRetention: Task<Input = ItemId>,
        TPush: Task<Input = PushInput>,
    {
        async move {
            self.admit_relay::<TPush, TRetention>(Submitter::Fleet(fleet), item, relay, route, now)
                .await
        }
    }

    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send avoids async-fn/GAT lifetime generality errors in journal transactions"
    )]
    fn admit_relay<'call, TPush, TRetention>(
        &'call mut self,
        submitter: crate::Submitter,
        item: DeliveryEnvelope<D>,
        relay: PeerId,
        route: D::StatusRoute,
        now: SyncTime,
    ) -> impl std::future::Future<Output = Result<SubmitResult<D::Acceptance>, DeliveryError>>
    + Send
    + 'call
    where
        S: JournalScope + durable_steps::storage::JournalStorage,
        TRetention: Task<Input = ItemId>,
        TPush: Task<Input = PushInput>,
    {
        async move {
            let verified = match crate::sender_binding::verify_sender::<S, D, B>(
                self.scope,
                self.binding,
                item.sender(),
                crate::CustodyRow::Outbox(item.id()),
            )
            .await
            {
                Ok(verified) => verified,
                Err(DeliveryError::SenderBindingUnavailable { reason, .. }) => {
                    return Ok(SubmitResult::BindingUnavailable { reason });
                }
                Err(error) => return Err(error),
            };
            if submitter != verified.owner() {
                return Ok(SubmitResult::Refused {
                    reason: DeliveryRefusal::NotSubmitter,
                });
            }
            crate::delivery_sizes::check_status_route::<D>(&route)?;
            let reference = DeliveryItemRef {
                item: item.id(),
                digest: item.digest().clone(),
            };
            let answer = self
                .enqueue_envelope(item, relay, submitter, now, false)
                .await?;
            match answer {
                OutboxAdmission::BindingUnavailable { reason } => {
                    return Ok(SubmitResult::BindingUnavailable { reason });
                }
                OutboxAdmission::Refused { reason } => return Ok(SubmitResult::Refused { reason }),
                OutboxAdmission::Replayed { status, version } => {
                    let OutboxStatusState::Known { cancel, .. } =
                        self.status(reference.item).await?.state
                    else {
                        return Err(DeliveryError::CustodyMissing);
                    };
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
            crate::admit_record_removal::<_, TRetention>(self.scope, reference.item).await?;
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
            self.save(&record, now).await?;
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
        now: SyncTime,
    ) -> Result<Option<crate::ResolvedPush<D>>, DeliveryError> {
        let Some(record) = self.load(reference.item).await? else {
            return Ok(None);
        };
        if record.header.digest != reference.digest.as_str() {
            return Err(DeliveryError::DigestMismatch);
        }
        if record.header.valid_until.is_some_and(|until| now >= until) {
            return Ok(None);
        }
        let Some(item) = record.payload else {
            return Ok(None);
        };
        let submitter = record.header.submitter.ok_or(DeliveryError::Corrupt {
            field: "push custody submitter",
        })?;
        Ok(Some(crate::ResolvedPush { submitter, item }))
    }

    pub(crate) async fn finish_push_at_deadline<TPublish>(
        &mut self,
        item: ItemId,
        reach: durable_steps::storage::CallReach,
        now: SyncTime,
    ) -> Result<(), DeliveryError>
    where
        S: JournalScope,
        TPublish: Task<Input = PublicationRef<D::StatusRoute>>,
    {
        let mut record = self
            .load(item)
            .await?
            .ok_or(DeliveryError::CustodyMissing)?;
        let never_accepted = matches!(
            reach,
            durable_steps::storage::CallReach::NeverSent
                | durable_steps::storage::CallReach::NoEffect
        );
        if never_accepted {
            record.header.status = DeliveryStatus::Expired;
            record.header.version = next_version(record.header.version)?;
        }
        // Possible recipient reach waits for its truthful report. The body is
        // never resent after expiry, and this attempt is never called unsent.
        record.header.submit_attempted = true;
        record.payload = None;
        self.save(&record, now).await?;
        if never_accepted {
            self.owe_relay_status::<TPublish>(&record.header, now)
                .await?;
        }
        Ok(())
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
            if now >= crate::removal_deadline(reference.item) {
                return Ok(StatusPublicationResult::Refused {
                    reason: DeliveryRefusal::TooOld,
                });
            }
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
            let (status, cancel) = match answer {
                PushResult::BindingUnavailable { reason } => {
                    return Err(DeliveryError::SenderBindingUnavailable {
                        row: crate::CustodyRow::Outbox(reference.item),
                        reason,
                    });
                }
                PushResult::Accepted { .. } => (DeliveryStatus::AtRecipient, None),
                PushResult::Replayed { status, cancel } => {
                    if status.rank() < DeliveryStatus::<D::Acceptance>::AtRecipient.rank() {
                        return Err(DeliveryError::InvalidBatch);
                    }
                    match (record.header.cancel_target, &status, cancel) {
                        (None, _, None)
                        | (Some(_), DeliveryStatus::AtRecipient, Some(CancelResult::Pending))
                        | (
                            Some(_),
                            DeliveryStatus::Cancelled,
                            Some(CancelResult::Prevented | CancelResult::TooLate { .. }),
                        )
                        | (
                            Some(_),
                            DeliveryStatus::Expired | DeliveryStatus::Refused { .. },
                            None,
                        ) => {}
                        _ => return Err(DeliveryError::InvalidBatch),
                    }
                    if cancel.is_some() {
                        self.validate_relay_cancel(&record.header, record.header.id)
                            .await?;
                    }
                    (status, cancel)
                }
                PushResult::Conflict { existing } => (
                    DeliveryStatus::Refused {
                        reason: DeliveryRefusal::ItemConflict { existing },
                    },
                    None,
                ),
                PushResult::Refused {
                    reason: DeliveryRefusal::MailboxFull,
                } => {
                    return Ok(StatusPublicationResult::Refused {
                        reason: DeliveryRefusal::MailboxFull,
                    });
                }
                PushResult::Refused { reason } => (DeliveryStatus::Refused { reason }, None),
            };
            crate::delivery_sizes::check_status_bound::<D>(
                &status,
                record.header.acceptance_bound,
            )?;
            let status_changed = record.header.status.may_advance_to(&status);
            let cancel_changed = validate_cancel_revision(&record.header, cancel)?;
            let changed = status_changed || cancel_changed;
            if !changed && record.payload.is_none() && record.header.handed_off_at.is_some() {
                return Ok(StatusPublicationResult::Applied);
            }
            if status_changed {
                record.header.status = status;
            }
            if let Some(cancel) = cancel {
                record.header.cancel = Some(cancel);
            }
            if changed {
                record.header.version = next_version(record.header.version)?;
            }
            record.header.handed_off_at.get_or_insert(now);
            record.payload = None;
            self.save(&record, now).await?;
            if changed {
                self.owe_relay_status::<TPublish>(&record.header, now)
                    .await?;
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
            if now >= crate::removal_deadline(report.item) {
                return Ok(ReportResult::Refused {
                    reason: DeliveryRefusal::TooOld,
                });
            }
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
            let cancel_identity = match &report.outcome {
                RecipientOutcome::Prevented { cancel }
                | RecipientOutcome::TooLate { cancel, .. } => Some(*cancel),
                _ => None,
            };
            if let Some(cancel) = cancel_identity {
                match self.validate_relay_cancel(&record.header, cancel).await {
                    Ok(()) => {}
                    Err(DeliveryError::InvalidBatch) => {
                        return Ok(ReportResult::Refused {
                            reason: DeliveryRefusal::ProcessingFailed {
                                kind: DeliveryFailureKind::ExecutionFailed,
                            },
                        });
                    }
                    Err(error) => return Err(error),
                }
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
                RecipientOutcome::Prevented { .. } => (
                    DeliveryStatus::Cancelled,
                    is_cancel.then_some(CancelResult::Prevented),
                ),
                RecipientOutcome::TooLate { at, .. } => {
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
            let cancel_changed = match validate_cancel_revision(&record.header, cancel) {
                Ok(changed) => changed,
                Err(DeliveryError::InvalidBatch) => {
                    return Ok(ReportResult::Refused {
                        reason: DeliveryRefusal::ProcessingFailed {
                            kind: DeliveryFailureKind::ExecutionFailed,
                        },
                    });
                }
                Err(error) => return Err(error),
            };
            record.header.last_report_seq = report.report_seq;
            let status_changed = record.header.status.may_advance_to(&status);
            let changed = status_changed || cancel_changed;
            if status_changed {
                record.header.status = status;
            }
            if changed {
                record.header.version = next_version(record.header.version)?;
            }
            if let Some(cancel) = cancel {
                record.header.cancel = Some(cancel);
            }
            // A recipient's outcome also proves that its inbox committed the item.
            record.header.handed_off_at.get_or_insert(now);
            record.payload = None;
            self.save(&record, now).await?;
            if changed {
                self.owe_relay_status::<TPublish>(&record.header, now)
                    .await?;
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
        now: SyncTime,
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
                .append_and_admit::<TPublish>(
                    &StatusPublication {
                        item: header.id,
                        status: copy_wire(&header.status)?,
                        version: header.version,
                        cancel: header.cancel,
                    },
                    now,
                )
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

fn validate_cancel_revision<A>(
    header: &OutboxHeader<A>,
    next: Option<CancelResult>,
) -> Result<bool, DeliveryError> {
    let Some(next) = next else {
        return Ok(false);
    };
    if header.cancel_target.is_none()
        || header.status.is_terminal() && !matches!(header.status, DeliveryStatus::Cancelled)
    {
        return Err(DeliveryError::InvalidBatch);
    }
    // Status rank cannot express a Pending -> final cancel-result revision.
    // Final results are immutable, including the TooLate observation point.
    match header.cancel {
        Some(current) if current == next => Ok(false),
        Some(CancelResult::Pending) => Ok(true),
        Some(CancelResult::Prevented | CancelResult::TooLate { .. }) | None => {
            Err(DeliveryError::InvalidBatch)
        }
    }
}
