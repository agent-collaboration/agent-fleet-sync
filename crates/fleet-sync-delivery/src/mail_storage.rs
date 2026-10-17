use crate::{DeliveryError, checked_queries as queries, custody_record::*};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub(crate) struct MailStorage<'s, S> {
    pub scope: &'s mut S,
    pub owner: PeerId,
}
impl<S> MailStorage<'_, S>
where
    S: TransactionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database> + Encode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub async fn state(&mut self) -> Result<MailState, DeliveryError> {
        let state: MailState = match queries::read_mail_state(self.scope)
            .await
            .map_err(sql_error)?
        {
            Some(row) => decode(row.record, "mail state")?,
            None => MailState::new(self.owner),
        };
        if state.peer != self.owner
            || state.items.value() > i64::MAX as u64
            || state.controls.value() > i64::MAX as u64
            || state.received_items > state.items
            || state.received_controls > state.controls
        {
            return Err(DeliveryError::Corrupt {
                field: "mail state owner or sequence",
            });
        }
        Ok(state)
    }
    pub async fn save_state(&mut self, state: &MailState) -> Result<(), DeliveryError> {
        queries::write_mail_state(self.scope, encode(state)?)
            .await
            .map_err(sql_error)
    }
    pub async fn item<D: DeliveryTypes>(
        &mut self,
        item: ItemId,
    ) -> Result<Option<MailRecord<D>>, DeliveryError> {
        queries::read_mail_item(self.scope, item_key(item))
            .await
            .map_err(sql_error)?
            .map(|row| self.decode_mail(row, Some(item)))
            .transpose()
    }
    pub fn decode_mail<D: DeliveryTypes>(
        &self,
        row: queries::StoredMailRow,
        expected: Option<ItemId>,
    ) -> Result<MailRecord<D>, DeliveryError> {
        let record: MailRecord<D> = decode(row.record, "mail item record")?;
        record.validate(self.owner)?;
        if row.item_id.as_deref() != Some(item_key(record.id()).as_str())
            || expected.is_some_and(|expected| expected != record.id())
        {
            return Err(DeliveryError::Corrupt {
                field: "mail item identity",
            });
        }
        let seq = if row.stream.is_none()
            && row.seq.is_none()
            && row.seq_class.as_deref() == Some("null")
        {
            None
        } else {
            Some(integer(row.seq, row.seq_class, "mail item sequence")?)
        };
        let live = integer(row.live, row.live_class, "mail live flag")?;
        let slot = integer(row.capacity_slot, row.slot_class, "mail capacity slot")?;
        let bytes = integer(row.payload_bytes, row.payload_class, "mail payload bytes")?;
        let settlement = integer(
            row.settlement_bytes,
            row.settlement_class,
            "mail settlement bytes",
        )?;
        match &record {
            MailRecord::Live(record) => {
                if live != 1
                    || slot != u64::from(record.capacity_slot)
                    || seq != Some(record.seq.value())
                    || row.stream.as_deref() != Some(stream_key(record.stream).as_str())
                    || bytes != payload_bytes(&record.payload)?
                    || settlement != u64::from(record.settlement_bytes)
                {
                    return Err(DeliveryError::Corrupt {
                        field: "mail row metadata",
                    });
                }
            }
            MailRecord::Compacted(_) => {
                if live != 0 || slot != 0 || bytes != 0 || settlement != 0 || seq == Some(0) {
                    return Err(DeliveryError::Corrupt {
                        field: "compacted mail metadata",
                    });
                }
                if seq.is_none() {
                    if !matches!(
                        record.status(),
                        DeliveryStatus::Refused {
                            reason: DeliveryRefusal::ProcessingFailed { .. }
                        }
                    ) || row.stream.is_some()
                    {
                        return Err(DeliveryError::Corrupt {
                            field: "settled mail position",
                        });
                    }
                } else if !matches!(row.stream.as_deref(), Some("items" | "controls")) {
                    return Err(DeliveryError::Corrupt {
                        field: "compacted mail stream",
                    });
                }
            }
        }
        Ok(record)
    }
    pub async fn save_live<D: DeliveryTypes>(
        &mut self,
        record: &LiveMail<D>,
    ) -> Result<(), DeliveryError> {
        let payload_size = payload_bytes(&record.payload)?;
        queries::write_mail_item(
            self.scope,
            item_key(record.id),
            Some(stream_key(record.stream)),
            Some(record.seq.value().to_string()),
            "1".into(),
            payload_size.to_string(),
            record.settlement_bytes.to_string(),
            encode(&MailRecord::<D>::Live(copy_wire(record)?))?,
            u8::from(record.capacity_slot).to_string(),
        )
        .await
        .map_err(sql_error)
    }
    pub async fn save_compacted<D: DeliveryTypes>(
        &mut self,
        record: &LiveMail<D>,
    ) -> Result<(), DeliveryError> {
        let compacted = MailRecord::<D>::Compacted(CompactedMail {
            id: record.id,
            digest: record.digest.clone(),
            status: copy_wire(&record.status)?,
            version: record.version,
            cancel: record.cancel,
        });
        compacted.validate(self.owner)?;
        queries::write_mail_item(
            self.scope,
            item_key(record.id),
            Some(stream_key(record.stream)),
            Some(record.seq.value().to_string()),
            "0".into(),
            "0".into(),
            "0".into(),
            encode(&compacted)?,
            "0".into(),
        )
        .await
        .map_err(sql_error)
    }
    pub async fn publication<A: serde::Serialize>(
        &mut self,
        target: PeerId,
        publication: &StatusPublication<A>,
    ) -> Result<(), DeliveryError> {
        queries::write_publication(
            self.scope,
            item_key(publication.item),
            encode(&crate::PublicationTarget::StatusStream { sender: target })?,
            publication.version.value().to_string(),
            encode(publication)?,
        )
        .await
        .map_err(sql_error)
    }
}
