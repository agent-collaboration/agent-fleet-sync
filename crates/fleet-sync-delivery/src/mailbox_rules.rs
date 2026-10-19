use crate::{
    DeliveryError, DeliveryLimits, checked_queries as queries, custody_record::*,
    mail_storage::MailStorage,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

pub struct Mailbox<'s, S, D> {
    pub(crate) storage: MailStorage<'s, S>,
    pub(crate) limits: DeliveryLimits,
    payload: PhantomData<fn() -> D>,
}
impl<'s, S, D> Mailbox<'s, S, D> {
    pub fn new(scope: &'s mut S, recipient: PeerId, limits: DeliveryLimits) -> Self {
        Self {
            storage: MailStorage {
                scope,
                owner: recipient,
            },
            limits,
            payload: PhantomData,
        }
    }
}
impl<S, D> Mailbox<'_, S, D>
where
    S: TransactionScope,
    D: DeliveryTypes,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database> + Encode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    /// Allow new submissions on enrollment or readmission, preserving prior custody and answers.
    pub async fn open(&mut self) -> Result<(), DeliveryError> {
        let mut state = self.storage.state().await?;
        state.closed = false;
        self.storage.save_state(&state).await
    }

    pub async fn close(&mut self) -> Result<(), DeliveryError> {
        let mut state = self.storage.state().await?;
        state.closed = true;
        for row in queries::list_mail_items(self.storage.scope)
            .await
            .map_err(sql_error)?
        {
            if let MailRecord::Live(mut record) = self.storage.decode_mail::<D>(row, None)?
                && !record.fetched
                && !record.status.is_terminal()
            {
                record.status = DeliveryStatus::Refused {
                    reason: DeliveryRefusal::RecipientRevoked,
                };
                record.version = next_version(record.version)?;
                record.payload = None;
                self.owe(&record).await?;
                self.storage.save_live(&record).await?;
            }
        }
        self.storage.save_state(&state).await
    }
    pub async fn submit(
        &mut self,
        origin: PeerId,
        item: DeliveryEnvelope<D>,

        now: SyncTime,
    ) -> Result<SubmitResult<D::Acceptance>, DeliveryError> {
        self.limits.validate::<D>()?;
        crate::delivery_sizes::check_envelope(&item)?;
        let mut state = self.storage.state().await?;
        if let Some(existing) = self.storage.item::<D>(item.id()).await? {
            return if existing.digest() == item.digest() {
                Ok(SubmitResult::Replayed {
                    status: copy_wire(existing.status())?,
                    version: existing.version(),
                    cancel: existing.cancel(),
                })
            } else {
                Ok(SubmitResult::Refused {
                    reason: DeliveryRefusal::ItemConflict {
                        existing: existing.digest().clone(),
                    },
                })
            };
        }
        if state.closed {
            return Ok(SubmitResult::Refused {
                reason: DeliveryRefusal::RecipientRevoked,
            });
        }
        if item.recipient().peer != self.storage.owner {
            return Ok(SubmitResult::Refused {
                reason: DeliveryRefusal::RecipientUnknown,
            });
        }
        let bytes =
            u64::try_from(encode(&item)?.len()).map_err(|_| DeliveryError::SequenceExhausted)?;
        let stream = item.stream();
        let cancel_target = match &item {
            DeliveryEnvelope::Item(_) => None,
            DeliveryEnvelope::Cancel(cancel) => Some(cancel.target()),
        };
        let mut capacity_slot = true;
        if let Some(target) = cancel_target {
            if let Some(link) = queries::read_mail_cancel_link(self.storage.scope, item_key(target))
                .await
                .map_err(sql_error)?
            {
                if link.target.as_deref() != Some(item_key(target).as_str()) {
                    return Err(DeliveryError::Corrupt {
                        field: "cancel target link",
                    });
                }
                let cancel_id: ItemId = decode(
                    link.cancel_item.map(|id| format!("\"{id}\"")),
                    "cancel item link",
                )?;
                let cancel =
                    self.storage
                        .item::<D>(cancel_id)
                        .await?
                        .ok_or(DeliveryError::Corrupt {
                            field: "cancel link item missing",
                        })?;
                return Ok(SubmitResult::Replayed {
                    status: copy_wire(cancel.status())?,
                    version: cancel.version(),
                    cancel: cancel.cancel(),
                });
            }
            if let Some(MailRecord::Live(mut original)) = self.storage.item::<D>(target).await? {
                // Decision 54: this original already reserved both the count
                // slot and the bytes of its single cancel.
                let charge = u32::try_from(bytes).map_err(|_| DeliveryError::Corrupt {
                    field: "cancel exceeds settlement allowance",
                })?;
                original.settlement_bytes = original.settlement_bytes.checked_sub(charge).ok_or(
                    DeliveryError::Corrupt {
                        field: "cancel exceeds settlement allowance",
                    },
                )?;
                self.storage.save_live(&original).await?;
                capacity_slot = false;
            }
        }
        if capacity_slot {
            let accounting = queries::read_mail_accounting(self.storage.scope)
                .await
                .map_err(sql_error)?;
            let count = decode_decimal(accounting.items, "mail item count")?;
            let used = decode_decimal(accounting.bytes, "mail occupied bytes")?;
            if !fits_mailbox(self.limits, count, used, bytes) {
                return Ok(SubmitResult::Refused {
                    reason: DeliveryRefusal::MailboxFull,
                });
            }
        }
        let seq = state.allocate(stream)?;
        let status = if item.valid_until().is_some_and(|until| now >= until) {
            DeliveryStatus::Expired
        } else {
            DeliveryStatus::Sent { seq }
        };
        let terminal = status.is_terminal();
        let record = LiveMail {
            id: item.id(),
            digest: item.digest().clone(),
            origin,

            stream,
            seq,
            payload: if terminal { None } else { Some(item) },
            status,
            version: StatusVersion::new(1),
            last_report_seq: ReportSeq::new(0),
            accepted_at: now,
            fetched: false,
            released_at: terminal.then_some(now),
            cancel: cancel_target.map(|_| CancelResult::Pending),
            cancel_target,
            settlement_bytes: if capacity_slot {
                self.limits.settlement_allowance
            } else {
                0
            },
            capacity_slot,
        };
        self.storage.save_live(&record).await?;
        if let Some(target) = cancel_target {
            queries::write_mail_cancel_link(
                self.storage.scope,
                item_key(target),
                item_key(record.id),
            )
            .await
            .map_err(sql_error)?;
        }
        self.storage.save_state(&state).await?;
        self.owe(&record).await?;
        Ok(SubmitResult::Accepted {
            seq,
            status: copy_wire(&record.status)?,
            version: record.version,
            cancel: record.cancel,
        })
    }

    pub(crate) async fn owe(&mut self, record: &LiveMail<D>) -> Result<(), DeliveryError> {
        self.storage
            .publication(
                record.origin,
                &StatusPublication {
                    item: record.id,
                    status: copy_wire(&record.status)?,
                    version: record.version,
                    cancel: record.cancel,
                },
            )
            .await
    }

    pub async fn query(
        &mut self,
        items: &[ItemId],
    ) -> Result<QueryResult<D::Acceptance>, DeliveryError> {
        self.storage.state().await?;
        let mut statuses = Vec::with_capacity(items.len());
        for item in items {
            let state = match self.storage.item::<D>(*item).await? {
                Some(record) => ItemStatusState::Known {
                    digest: record.digest().clone(),
                    status: copy_wire(record.status())?,
                    version: record.version(),
                    cancel: record.cancel(),
                },
                None => ItemStatusState::Unknown,
            };
            statuses.push(ItemStatus { item: *item, state });
        }
        Ok(QueryResult { statuses })
    }

    async fn validate_cancel_report(
        &mut self,
        record: &LiveMail<D>,
        cancel: ItemId,
        is_cancel: bool,
    ) -> Result<(), DeliveryError> {
        if is_cancel {
            if cancel != record.id {
                return Err(DeliveryError::InvalidBatch);
            }
        } else {
            let link = queries::read_mail_cancel_link(self.storage.scope, item_key(record.id))
                .await
                .map_err(sql_error)?
                .ok_or(DeliveryError::InvalidBatch)?;
            if link.target.as_deref() != Some(item_key(record.id).as_str())
                || link.cancel_item.as_deref() != Some(item_key(cancel).as_str())
            {
                return Err(DeliveryError::InvalidBatch);
            }
        }
        Ok(())
    }

    pub async fn report(
        &mut self,
        recipient: PeerId,
        report: RecipientReport<D>,
    ) -> Result<ReportResult<D::Acceptance>, DeliveryError> {
        crate::delivery_sizes::check_outcome::<D>(&report.outcome)?;
        if recipient != self.storage.owner
            || report.report_seq.value() == 0
            || report.report_seq.value() > i64::MAX as u64
        {
            return Err(DeliveryError::InvalidBatch);
        }
        let Some(mut record) = self.storage.item::<D>(report.item).await? else {
            return Err(DeliveryError::Corrupt {
                field: "report item missing",
            });
        };
        if let MailRecord::Live(live) = &mut record
            && report.report_seq > live.last_report_seq
        {
            let is_cancel = live.cancel_target.is_some();
            let (status, cancel) = match report.outcome {
                RecipientOutcome::Held => (DeliveryStatus::Held, None),
                RecipientOutcome::Delivered { acceptance } => {
                    (DeliveryStatus::Delivered { acceptance }, None)
                }
                RecipientOutcome::Uncertain => (DeliveryStatus::Uncertain, None),
                RecipientOutcome::Expired => (DeliveryStatus::Expired, None),
                RecipientOutcome::Prevented { cancel } => {
                    self.validate_cancel_report(live, cancel, is_cancel).await?;
                    (
                        DeliveryStatus::Cancelled,
                        is_cancel.then_some(CancelResult::Prevented),
                    )
                }
                RecipientOutcome::TooLate { cancel, at } => {
                    self.validate_cancel_report(live, cancel, is_cancel).await?;
                    if is_cancel {
                        (
                            DeliveryStatus::Cancelled,
                            Some(CancelResult::TooLate { at }),
                        )
                    } else {
                        if !live.status.is_terminal() {
                            return Err(DeliveryError::InvalidBatch);
                        }
                        (copy_wire(&live.status)?, None)
                    }
                }
                RecipientOutcome::Dropped => (
                    DeliveryStatus::Refused {
                        reason: DeliveryRefusal::DroppedByPolicy,
                    },
                    None,
                ),
                RecipientOutcome::Refused { reason } => (DeliveryStatus::Refused { reason }, None),
            };
            live.last_report_seq = report.report_seq;
            if live.status.may_advance_to(&status) {
                live.status = status;
                live.version = next_version(live.version)?;
                if let Some(cancel) = cancel {
                    live.cancel = Some(cancel);
                }
                if live.status.is_terminal() {
                    live.payload = None;
                }
                self.owe(live).await?;
            }
            self.storage.save_live(live).await?;
        }
        Ok(ReportResult::Applied {
            item: report.item,
            acknowledged: report.report_seq,
            status: copy_wire(record.status())?,
            version: record.version(),
        })
    }
}

fn decode_decimal(value: Option<String>, field: &'static str) -> Result<u64, DeliveryError> {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(DeliveryError::Corrupt { field })
}
