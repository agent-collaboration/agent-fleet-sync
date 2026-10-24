use crate::{
    DeliveryError, checked_queries as queries, custody_record::*, outbox_rules::Outbox,
    status_stream::canonical_encode,
};
use durable_steps::storage::TransactionScope;
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
    pub async fn publish_status(
        &mut self,
        publication: StatusPublication<D::Acceptance>,
    ) -> Result<(), DeliveryError> {
        crate::delivery_sizes::check_status::<D>(&publication.status)?;
        if publication.version.value() == 0 || publication.status.rank() == 0 {
            return Err(DeliveryError::InvalidBatch);
        }
        let state = self.header_state(publication.item).await?;
        let mut header = match state {
            Some(crate::outbox_record::OutboxHeaderState::Item(header)) => header,
            Some(crate::outbox_record::OutboxHeaderState::Recovered(mut record)) => {
                crate::delivery_sizes::check_status_bound::<D>(
                    &publication.status,
                    record.acceptance_bound,
                )?;
                if publication.version <= record.version {
                    return Ok(());
                }
                validate_advance(&record.status, &publication.status)?;
                record.status = publication.status;
                record.version = publication.version;
                record.cancel = publication.cancel;
                return self.save_recovered(&record).await;
            }
            _ => return Ok(()),
        };
        crate::delivery_sizes::check_status_bound::<D>(
            &publication.status,
            header.acceptance_bound,
        )?;
        if publication.version <= header.version {
            return Ok(());
        }
        if publication.status.rank() < header.status.rank()
            || header.status.is_terminal()
                && canonical_encode(&publication.status)? != canonical_encode(&header.status)?
        {
            return Err(DeliveryError::Corrupt {
                field: "canonical publication regressed status",
            });
        }
        header.status = publication.status;
        header.version = publication.version;
        header.cancel = publication.cancel;
        header.submit_attempted = true;
        // A canonical relay status proves the relay's custody. The body stays
        // only in that custody and the receiving hop, never in sender history.
        self.save_header(&header, None).await
    }
    pub async fn status_cursor(&mut self) -> Result<StatusCursor, DeliveryError> {
        match queries::read_outbox_status(self.scope)
            .await
            .map_err(sql_error)?
        {
            Some(row) => decode(row.record, "outbox status cursor"),
            None => Ok(StatusCursor::new(0)),
        }
    }
    pub async fn apply_status_batch(
        &mut self,
        after: StatusCursor,
        batch: StatusFetchResult<D::Acceptance>,
    ) -> Result<(), DeliveryError> {
        let current = self.status_cursor().await?;
        if after > current || batch.through < after {
            return Err(DeliveryError::InvalidBatch);
        }
        let mut previous = after;
        for event in &batch.events {
            crate::delivery_sizes::check_status::<D>(&event.status)?;
            if event.cursor <= previous || event.cursor > batch.through {
                return Err(DeliveryError::InvalidBatch);
            }
            previous = event.cursor;
        }
        if batch.through != previous {
            return Err(DeliveryError::InvalidBatch);
        }
        for event in batch.events {
            self.publish_status(StatusPublication {
                item: event.item,
                status: event.status,
                version: event.version,
                cancel: event.cancel,
            })
            .await?;
        }
        queries::write_outbox_status(self.scope, encode(&current.max(batch.through))?)
            .await
            .map_err(sql_error)
    }
}

fn validate_advance<A: serde::Serialize>(
    current: &DeliveryStatus<A>,
    next: &DeliveryStatus<A>,
) -> Result<(), DeliveryError> {
    if next.rank() < current.rank()
        || current.is_terminal() && canonical_encode(current)? != canonical_encode(next)?
    {
        return Err(DeliveryError::Corrupt {
            field: "canonical publication regressed status",
        });
    }
    Ok(())
}
