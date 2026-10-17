use crate::{DeliveryError, checked_queries as queries, custody_record::*};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

use crate::Mailbox;
impl<S, D> Mailbox<'_, S, D>
where
    S: TransactionScope,
    D: DeliveryTypes,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database> + Encode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub async fn settle(
        &mut self,
        item: ItemId,
        digest: ItemDigest,
    ) -> Result<SettleResult<D::Acceptance>, DeliveryError> {
        let state = self.storage.state().await?;
        if let Some(record) = self.storage.item::<D>(item).await? {
            return if record.digest() == &digest {
                Ok(SettleResult::Replayed {
                    status: copy_wire(record.status())?,
                    version: record.version(),
                    cancel: record.cancel(),
                })
            } else {
                Ok(SettleResult::ItemConflict {
                    existing: record.digest().clone(),
                })
            };
        }
        let reason: FailureCode = DeliveryFailureKind::CustodyUnresolvable.into();
        let version = StatusVersion::new(1);
        let record = MailRecord::<D>::Compacted(CompactedMail {
            id: item,
            digest,
            status: DeliveryStatus::Refused {
                reason: DeliveryRefusal::ProcessingFailed {
                    code: reason.clone(),
                },
            },
            version,
            cancel: None,
        });
        queries::write_mail_item(
            self.storage.scope,
            item_key(item),
            None,
            None,
            "0".into(),
            "0".into(),
            "0".into(),
            encode(&record)?,
            "0".into(),
        )
        .await
        .map_err(sql_error)?;
        self.storage.save_state(&state).await?;
        Ok(SettleResult::Settled { reason, version })
    }
}
