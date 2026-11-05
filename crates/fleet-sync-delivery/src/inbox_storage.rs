use crate::{
    DeliveryConsumer, DeliveryError, DeliveryLimits, checked_queries as queries, custody_record::*,
    inbox_record::*,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

#[cfg(test)]
#[path = "inbox_storage_test.rs"]
mod tests;

pub struct Inbox<'s, S, D, B> {
    pub(crate) binding: &'s B,
    pub(crate) scope: &'s mut S,
    pub(crate) owner: PeerId,
    pub(crate) limits: DeliveryLimits,
    payload: PhantomData<fn() -> D>,
}
impl<'s, S, D, B> Inbox<'s, S, D, B> {
    pub fn new(scope: &'s mut S, owner: PeerId, limits: DeliveryLimits, binding: &'s B) -> Self {
        Self {
            binding,
            scope,
            owner,
            limits,
            payload: PhantomData,
        }
    }
}
impl<S, D, B> Inbox<'_, S, D, B>
where
    S: TransactionScope,
    D: DeliveryTypes,
    B: crate::SenderBinding<S, D>,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database> + Encode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub(crate) async fn load(
        &mut self,
        item: ItemId,
    ) -> Result<Option<InboxRecord<D>>, DeliveryError> {
        let row = queries::read_inbox_item(self.scope, item_key(item))
            .await
            .map_err(sql_error)?;
        match row {
            Some(row) => self.decode_item(row, Some(item)).await.map(Some),
            None => Ok(None),
        }
    }

    pub(crate) async fn decode_item(
        &mut self,
        row: queries::StoredInboxRow,
        expected: Option<ItemId>,
    ) -> Result<InboxRecord<D>, DeliveryError> {
        let header: InboxHeader<D> =
            crate::inbox_row::decode_header::<S, D, B>(self.scope, self.binding, &row).await?;
        let payload = row
            .payload
            .map(|raw| decode(Some(raw), "inbox custody payload"))
            .transpose()?;
        let record = InboxRecord { header, payload };
        record.validate(self.owner)?;
        if let Some(payload) = &record.payload {
            crate::delivery_sizes::check_envelope(payload).map_err(|_| DeliveryError::Corrupt {
                field: "stored inbox envelope bounds",
            })?;
        }
        let bytes = integer(
            row.payload_bytes,
            row.payload_bytes_class,
            "inbox payload bytes",
        )?;
        let live = integer(row.live, row.live_class, "inbox live flag")?;
        let settlement = integer(
            row.settlement_charge_bytes,
            row.settlement_charge_bytes_class,
            "inbox settlement bytes",
        )?;
        if expected.is_some_and(|item| item != record.header.id)
            || row.item_id.as_deref() != Some(item_key(record.header.id).as_str())
            || live != u64::from(!record.header.compacts_payload())
            || bytes
                != u64::try_from(
                    record
                        .payload
                        .as_ref()
                        .map(encode)
                        .transpose()?
                        .map_or(0, |raw| raw.len()),
                )
                .map_err(|_| DeliveryError::SequenceExhausted)?
            || settlement
                != if record.header.compacts_payload() {
                    0
                } else {
                    u64::from(record.header.settlement_bytes)
                }
        {
            return Err(DeliveryError::Corrupt {
                field: "inbox row metadata",
            });
        }
        Ok(record)
    }
    pub(crate) async fn save(
        &mut self,
        record: &InboxRecord<D>,
        now: SyncTime,
    ) -> Result<(), DeliveryError> {
        record.validate(self.owner)?;
        self.save_header(
            &record.header,
            record.payload.as_ref().map(encode).transpose()?,
            now,
        )
        .await
    }
    pub(crate) async fn header(
        &mut self,
        item: ItemId,
    ) -> Result<Option<InboxHeader<D>>, DeliveryError> {
        let Some(row) = queries::read_inbox_item(self.scope, item_key(item))
            .await
            .map_err(sql_error)?
        else {
            return Ok(None);
        };
        let header: InboxHeader<D> =
            crate::inbox_row::decode_header::<S, D, B>(self.scope, self.binding, &row).await?;
        crate::delivery_sizes::stored_acceptance_bound::<D>(header.acceptance_bound)?;
        if let InboxProgress::Final { outcome, .. } = &header.progress {
            crate::delivery_sizes::check_outcome_bound::<D>(outcome, header.acceptance_bound)
                .map_err(|_| DeliveryError::Corrupt {
                    field: "stored inbox outcome bounds",
                })?;
        }
        if header.id != item {
            return Err(DeliveryError::Corrupt {
                field: "inbox header identity",
            });
        }
        Ok(Some(header))
    }
    pub(crate) async fn save_header(
        &mut self,
        header: &InboxHeader<D>,
        payload: Option<String>,
        now: SyncTime,
    ) -> Result<(), DeliveryError> {
        header.validate(self.owner)?;
        if header.legacy_read_only {
            return Err(DeliveryError::Refused {
                reason: DeliveryRefusal::NotSubmitter,
            });
        }
        let source = header.sender.as_ref().ok_or(DeliveryError::CorruptRow {
            table: "_fleet_sync_inbox_items",
            key: item_key(header.id),
            field: "consumer_sender",
        })?;
        let verified = crate::sender_binding::verify_sender::<S, D, B>(
            self.scope,
            self.binding,
            source,
            crate::CustodyRow::Inbox(header.id),
        )
        .await?;
        if Some(verified.owner()) != header.submitter {
            return Err(DeliveryError::CorruptRow {
                table: "_fleet_sync_inbox_items",
                key: item_key(header.id),
                field: "submitter",
            });
        }
        // Only the recipient retains the full cancellation header; relay/outbox compacts keep identity/digest/status.
        let compact = header.compacts_payload();
        let payload = if compact { None } else { payload };
        queries::write_inbox_item(
            self.scope,
            item_key(header.id),
            u8::from(!compact).to_string(),
            payload.as_ref().map_or(0, String::len).to_string(),
            if compact { 0 } else { header.settlement_bytes }.to_string(),
            encode(header)?,
            payload,
            header.created_at.unix_millis().to_string(),
            now.unix_millis().to_string(),
            header.sender.as_ref().map(encode).transpose()?,
        )
        .await
        .map_err(sql_error)
    }
    pub(crate) async fn update_header(
        &mut self,
        header: &InboxHeader<D>,
        now: SyncTime,
    ) -> Result<(), DeliveryError> {
        let row = queries::read_inbox_item(self.scope, item_key(header.id))
            .await
            .map_err(sql_error)?
            .ok_or(DeliveryError::CustodyMissing)?;
        self.save_header(header, row.payload, now).await
    }
    pub async fn resolve_delivery(
        &mut self,
        reference: &DeliveryItemRef,
    ) -> Result<DeliveryItem<D>, DeliveryError> {
        self.resolve_local_delivery(reference)
            .await
            .map(|resolved| resolved.item)
    }
    /// Resolve the body and its stored receipt bound together. Hosts use the
    /// returned value to validate the answer before the call is checkpointed.
    pub async fn resolve_local_delivery(
        &mut self,
        reference: &DeliveryItemRef,
    ) -> Result<ResolvedDelivery<D>, DeliveryError> {
        let record = self
            .load(reference.item)
            .await?
            .ok_or(DeliveryError::Corrupt {
                field: "local delivery custody row missing",
            })?;
        if record.header.digest != reference.digest.as_str() {
            return Err(DeliveryError::Corrupt {
                field: "local delivery custody digest",
            });
        }
        let acceptance_bound =
            crate::delivery_sizes::stored_acceptance_bound::<D>(record.header.acceptance_bound)?;
        match record.payload {
            Some(DeliveryEnvelope::Item(item)) => Ok(ResolvedDelivery {
                item,
                acceptance_bound,
            }),
            None => Err(DeliveryError::CustodyMissing),
            Some(DeliveryEnvelope::Cancel(_)) => Err(DeliveryError::Corrupt {
                field: "cancel resolved as external delivery",
            }),
        }
    }
    pub(crate) async fn report_superseded(
        &mut self,
        reference: ReportRef,
    ) -> Result<bool, DeliveryError> {
        Ok(self
            .header(reference.item)
            .await?
            .is_some_and(|header| header.report_seq > reference.report_seq))
    }
    pub async fn resolve_report(
        &mut self,
        reference: ReportRef,
    ) -> Result<RecipientReport<D>, DeliveryError> {
        let row = queries::read_report(
            self.scope,
            item_key(reference.item),
            reference.report_seq.value().to_string(),
        )
        .await
        .map_err(sql_error)?
        .ok_or(DeliveryError::Corrupt {
            field: "report custody row missing",
        })?;
        let report: RecipientReport<D> = crate::report_row::decode_report(&row)?;
        crate::delivery_sizes::check_outcome::<D>(&report.outcome).map_err(|_| {
            DeliveryError::Corrupt {
                field: "stored report bounds",
            }
        })?;
        if report.item != reference.item || report.report_seq != reference.report_seq {
            return Err(DeliveryError::Corrupt {
                field: "report identity",
            });
        }
        Ok(report)
    }
    pub async fn retire_report(&mut self, reference: ReportRef) -> Result<(), DeliveryError> {
        queries::retire_report(
            self.scope,
            item_key(reference.item),
            reference.report_seq.value().to_string(),
        )
        .await
        .map_err(sql_error)
    }
    pub(crate) async fn record_outcome<C>(
        &mut self,
        record: &mut InboxRecord<D>,
        outcome: RecipientOutcome<D::Acceptance>,
        consumer: &C,
        observation: Option<&D::LocalObservation>,
        now: SyncTime,
    ) -> Result<Option<ReportRef>, DeliveryError>
    where
        C: DeliveryConsumer<S>,
        C::Protocol: ConsumerProtocol<Delivery = D>,
    {
        self.record_header_outcome(&mut record.header, outcome, consumer, observation, now)
            .await
    }
    pub(crate) async fn record_header_outcome<C>(
        &mut self,
        header: &mut InboxHeader<D>,
        outcome: RecipientOutcome<D::Acceptance>,
        consumer: &C,
        observation: Option<&D::LocalObservation>,
        now: SyncTime,
    ) -> Result<Option<ReportRef>, DeliveryError>
    where
        C: DeliveryConsumer<S>,
        C::Protocol: ConsumerProtocol<Delivery = D>,
    {
        crate::delivery_sizes::check_outcome_bound::<D>(&outcome, header.acceptance_bound)?;
        header.report_seq = header
            .report_seq
            .next()
            .filter(|seq| seq.value() <= i64::MAX as u64)
            .ok_or(DeliveryError::SequenceExhausted)?;
        let report = RecipientReport::<D> {
            item: header.id,
            report_seq: header.report_seq,
            outcome,
        };
        consumer
            .on_outcome(self.scope, header.id, &report.outcome, observation)
            .await?;
        if let Some(target) = header.cancel_target {
            consumer
                .on_cancel_outcome(self.scope, header.id, target, &report.outcome)
                .await?;
        }
        if matches!(header.route, InboxRoute::Local) {
            return Ok(None);
        }
        queries::write_report(
            self.scope,
            item_key(report.item),
            report.report_seq.value().to_string(),
            encode(&report)?,
            now.unix_millis().to_string(),
            now.unix_millis().to_string(),
        )
        .await
        .map_err(sql_error)?;
        Ok(Some(ReportRef {
            item: report.item,
            report_seq: report.report_seq,
        }))
    }
}
