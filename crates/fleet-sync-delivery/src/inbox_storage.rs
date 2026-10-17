use crate::{
    DeliveryError, DeliveryLimits, checked_queries as queries, custody_record::*, inbox_record::*,
};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

pub struct Inbox<'s, S, D> {
    pub(crate) scope: &'s mut S,
    pub(crate) owner: PeerId,
    pub(crate) limits: DeliveryLimits,
    payload: PhantomData<fn() -> D>,
}
impl<'s, S, D> Inbox<'s, S, D> {
    pub fn new(scope: &'s mut S, owner: PeerId, limits: DeliveryLimits) -> Self {
        Self {
            scope,
            owner,
            limits,
            payload: PhantomData,
        }
    }
}
impl<S, D> Inbox<'_, S, D>
where
    S: TransactionScope,
    D: DeliveryTypes,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database> + Encode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub(crate) async fn load(
        &mut self,
        item: ItemId,
    ) -> Result<Option<InboxRecord<D>>, DeliveryError> {
        queries::read_inbox_item(self.scope, item_key(item))
            .await
            .map_err(sql_error)?
            .map(|row| self.decode_item(row, Some(item)))
            .transpose()
    }
    pub(crate) fn decode_item(
        &self,
        row: queries::StoredInboxRow,
        expected: Option<ItemId>,
    ) -> Result<InboxRecord<D>, DeliveryError> {
        let header: InboxHeader<D> = decode(row.record, "inbox header")?;
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
        let bytes = integer(row.payload_bytes, row.payload_class, "inbox payload bytes")?;
        let live = integer(row.live, row.live_class, "inbox live flag")?;
        let settlement = integer(
            row.settlement_bytes,
            row.settlement_class,
            "inbox settlement bytes",
        )?;
        if expected.is_some_and(|item| item != record.header.id)
            || row.item_id.as_deref() != Some(item_key(record.header.id).as_str())
            || live != u64::from(!record.header.compact_local())
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
                != if record.header.compact_local() {
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
    pub(crate) async fn save(&mut self, record: &InboxRecord<D>) -> Result<(), DeliveryError> {
        record.validate(self.owner)?;
        self.save_header(
            &record.header,
            record.payload.as_ref().map(encode).transpose()?,
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
        let header: InboxHeader<D> = decode(row.record, "inbox header")?;
        if let InboxProgress::Final { outcome, .. } = &header.progress {
            crate::delivery_sizes::check_outcome::<D>(outcome).map_err(|_| {
                DeliveryError::Corrupt {
                    field: "stored inbox outcome bounds",
                }
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
    ) -> Result<(), DeliveryError> {
        header.validate(self.owner)?;
        // Only the recipient retains the full cancellation header; relay/outbox compacts keep identity/digest/status.
        let compact = header.compact_local();
        let payload = if compact { None } else { payload };
        queries::write_inbox_item(
            self.scope,
            item_key(header.id),
            u8::from(!compact).to_string(),
            payload.as_ref().map_or(0, String::len).to_string(),
            if compact { 0 } else { header.settlement_bytes }.to_string(),
            encode(header)?,
            payload,
        )
        .await
        .map_err(sql_error)
    }
    pub(crate) async fn update_header(
        &mut self,
        header: &InboxHeader<D>,
    ) -> Result<(), DeliveryError> {
        let row = queries::read_inbox_item(self.scope, item_key(header.id))
            .await
            .map_err(sql_error)?
            .ok_or(DeliveryError::CustodyMissing)?;
        self.save_header(header, row.payload).await
    }
    pub(crate) async fn state(&mut self) -> Result<InboxState, DeliveryError> {
        let state: InboxState = match queries::read_inbox_state(self.scope)
            .await
            .map_err(sql_error)?
        {
            Some(row) => decode(row.record, "inbox state")?,
            None => InboxState::new(self.owner),
        };
        state.validate(self.owner)?;
        Ok(state)
    }
    pub(crate) async fn save_state(&mut self, state: &InboxState) -> Result<(), DeliveryError> {
        state.validate(self.owner)?;
        queries::write_inbox_state(self.scope, encode(state)?)
            .await
            .map_err(sql_error)
    }
    pub async fn resolve_delivery(
        &mut self,
        reference: &DeliveryItemRef,
    ) -> Result<DeliveryItem<D>, DeliveryError> {
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
        match record.payload {
            Some(DeliveryEnvelope::Item(item)) => Ok(item),
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
        let report: RecipientReport<D> = decode(row.record, "recipient report")?;
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
    pub(crate) async fn record_outcome(
        &mut self,
        record: &mut InboxRecord<D>,
        outcome: RecipientOutcome<D::Acceptance>,
    ) -> Result<Option<ReportRef>, DeliveryError> {
        self.record_header_outcome(&mut record.header, outcome)
            .await
    }
    pub(crate) async fn record_header_outcome(
        &mut self,
        header: &mut InboxHeader<D>,
        outcome: RecipientOutcome<D::Acceptance>,
    ) -> Result<Option<ReportRef>, DeliveryError> {
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
        if matches!(header.route, InboxRoute::Local) {
            return Ok(None);
        }
        queries::write_report(
            self.scope,
            item_key(report.item),
            report.report_seq.value().to_string(),
            encode(&report)?,
        )
        .await
        .map_err(sql_error)?;
        Ok(Some(ReportRef {
            item: report.item,
            report_seq: report.report_seq,
        }))
    }
}
