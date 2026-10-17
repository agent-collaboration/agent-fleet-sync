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
        let header: InboxHeader<D::Acceptance> = decode(row.record, "inbox header")?;
        let payload = decode(row.payload, "inbox custody payload")?;
        let record = InboxRecord { header, payload };
        record.validate(self.owner)?;
        let bytes = integer(row.payload_bytes, row.payload_class, "inbox payload bytes")?;
        let live = integer(row.live, row.live_class, "inbox live flag")?;
        let settlement = integer(
            row.settlement_bytes,
            row.settlement_class,
            "inbox settlement bytes",
        )?;
        if expected.is_some_and(|item| item != record.payload.id())
            || row.item_id.as_deref() != Some(item_key(record.payload.id()).as_str())
            || live != 1
            || bytes
                != u64::try_from(encode(&record.payload)?.len())
                    .map_err(|_| DeliveryError::SequenceExhausted)?
            || settlement != u64::from(record.header.settlement_bytes)
        {
            return Err(DeliveryError::Corrupt {
                field: "inbox row metadata",
            });
        }
        Ok(record)
    }
    pub(crate) async fn save(&mut self, record: &InboxRecord<D>) -> Result<(), DeliveryError> {
        record.validate(self.owner)?;
        self.save_header(&record.header, Some(encode(&record.payload)?))
            .await
    }
    pub(crate) async fn header(
        &mut self,
        item: ItemId,
    ) -> Result<Option<InboxHeader<D::Acceptance>>, DeliveryError> {
        let Some(row) = queries::read_inbox_item(self.scope, item_key(item))
            .await
            .map_err(sql_error)?
        else {
            return Ok(None);
        };
        let header: InboxHeader<D::Acceptance> = decode(row.record, "inbox header")?;
        if header.id != item {
            return Err(DeliveryError::Corrupt {
                field: "inbox header identity",
            });
        }
        Ok(Some(header))
    }
    pub(crate) async fn save_header(
        &mut self,
        header: &InboxHeader<D::Acceptance>,
        payload: Option<String>,
    ) -> Result<(), DeliveryError> {
        header.validate(self.owner)?;
        queries::write_inbox_item(
            self.scope,
            item_key(header.id),
            "1".into(),
            payload.as_ref().map_or(0, String::len).to_string(),
            header.settlement_bytes.to_string(),
            encode(header)?,
            payload,
        )
        .await
        .map_err(sql_error)
    }
    pub(crate) async fn update_header(
        &mut self,
        header: &InboxHeader<D::Acceptance>,
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
        if record.payload.digest() != &reference.digest {
            return Err(DeliveryError::Corrupt {
                field: "local delivery custody digest",
            });
        }
        match record.payload {
            DeliveryEnvelope::Item(item) => Ok(item),
            DeliveryEnvelope::Cancel(_) => Err(DeliveryError::Corrupt {
                field: "cancel resolved as external delivery",
            }),
        }
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
        header: &mut InboxHeader<D::Acceptance>,
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
