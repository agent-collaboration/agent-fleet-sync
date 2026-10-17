use crate::{DeliveryError, Mailbox, checked_queries as queries, custody_record::*};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

impl<S, D> Mailbox<'_, S, D>
where
    S: TransactionScope,
    D: DeliveryTypes,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database> + Encode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    /// Expire never-fetched custody atomically with its owed publication and
    /// return the next deadline the host must schedule from this durable state.
    pub async fn expire_due(&mut self, now: SyncTime) -> Result<Option<SyncTime>, DeliveryError> {
        let mut next = None;
        for row in queries::read_mail_deadlines(self.storage.scope)
            .await
            .map_err(sql_error)?
        {
            let MailRecord::Live(mut record) = self.storage.decode_mail::<D>(row, None)? else {
                continue;
            };
            if record.fetched || record.status.is_terminal() {
                continue;
            }
            let Some(deadline) = record
                .payload
                .as_ref()
                .and_then(DeliveryEnvelope::valid_until)
            else {
                continue;
            };
            if deadline > now {
                next = Some(next.map_or(deadline, |current: SyncTime| current.min(deadline)));
                continue;
            }
            record.status = DeliveryStatus::Expired;
            record.version = next_version(record.version)?;
            record.payload = None;
            record.released_at = Some(now);
            self.storage.save_live(&record).await?;
            self.owe(&record).await?;
        }
        Ok(next)
    }
}
