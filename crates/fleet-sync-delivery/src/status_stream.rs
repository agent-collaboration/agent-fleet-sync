use crate::{DeliveryError, checked_queries as queries, custody_record::*};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum PublicationTarget {
    StatusStream { sender: PeerId },
    Peer { peer: PeerId },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PublicationRef {
    pub item: ItemId,
    pub version: StatusVersion,
    pub target: PublicationTarget,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StreamState {
    sender: PeerId,
    head: StatusCursor,
}

pub struct StatusStream<'s, S, D> {
    scope: &'s mut S,
    sender: PeerId,
    payload: PhantomData<fn() -> D>,
}
impl<'s, S, D> StatusStream<'s, S, D> {
    pub fn new(scope: &'s mut S, sender: PeerId) -> Self {
        Self {
            scope,
            sender,
            payload: PhantomData,
        }
    }
}
impl<S, D> StatusStream<'_, S, D>
where
    S: TransactionScope,
    D: DeliveryTypes,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    pub async fn append_and_admit<T: durable_steps::Task<Input = PublicationRef>>(
        &mut self,
        publication: &StatusPublication<D::Acceptance>,
    ) -> Result<StatusEvent<D::Acceptance>, DeliveryError>
    where
        S: durable_steps::JournalScope,
    {
        let (event, reference) = self.append(publication).await?;
        if let Some(reference) = reference {
            self.scope
                .admit::<T>(&crate::delivery_request_key(&reference)?, &reference)
                .await
                .map_err(|error| DeliveryError::Storage(error.to_string()))?;
        }
        Ok(event)
    }
    async fn state(&mut self) -> Result<StreamState, DeliveryError> {
        let state: StreamState = match queries::read_status_state(self.scope)
            .await
            .map_err(sql_error)?
        {
            Some(row) => decode(row.record, "status stream state")?,
            None => StreamState {
                sender: self.sender,
                head: StatusCursor::new(0),
            },
        };
        if state.sender != self.sender || state.head.value() > i64::MAX as u64 {
            return Err(DeliveryError::Corrupt {
                field: "status stream owner or cursor",
            });
        }
        Ok(state)
    }
    fn decode_event(
        &self,
        row: queries::StoredStatusEventRow,
    ) -> Result<StatusEvent<D::Acceptance>, DeliveryError> {
        let event: StatusEvent<D::Acceptance> = decode(row.record, "status stream event")?;
        if event.cursor.value() == 0
            || event.version.value() == 0
            || event.status.rank() == 0
            || row.item_id.as_deref() != Some(item_key(event.item).as_str())
            || integer(row.version, row.version_class, "status event version")?
                != event.version.value()
            || integer(row.cursor, row.cursor_class, "status event cursor")? != event.cursor.value()
        {
            return Err(DeliveryError::Corrupt {
                field: "status event identity",
            });
        }
        Ok(event)
    }
    pub async fn append(
        &mut self,
        publication: &StatusPublication<D::Acceptance>,
    ) -> Result<(StatusEvent<D::Acceptance>, Option<PublicationRef>), DeliveryError> {
        crate::delivery_sizes::check_status::<D>(&publication.status)?;
        if publication.version.value() == 0
            || publication.version.value() > i64::MAX as u64
            || publication.status.rank() == 0
        {
            return Err(DeliveryError::InvalidBatch);
        }
        let mut state = self.state().await?;
        if let Some(row) = queries::read_status_revision(
            self.scope,
            item_key(publication.item),
            publication.version.value().to_string(),
        )
        .await
        .map_err(sql_error)?
        {
            let event = self.decode_event(row)?;
            let existing = StatusPublication {
                item: event.item,
                status: copy_wire(&event.status)?,
                version: event.version,
                cancel: event.cancel,
            };
            if canonical_encode(&existing)? != canonical_encode(publication)? {
                return Err(DeliveryError::CallConflict);
            }
            return Ok((event, None));
        }
        let cursor = state
            .head
            .next()
            .filter(|cursor| cursor.value() <= i64::MAX as u64)
            .ok_or(DeliveryError::SequenceExhausted)?;
        let event = StatusEvent {
            cursor,
            item: publication.item,
            status: copy_wire(&publication.status)?,
            version: publication.version,
            cancel: publication.cancel,
        };
        queries::write_status_event(
            self.scope,
            cursor.value().to_string(),
            item_key(event.item),
            event.version.value().to_string(),
            encode(&event)?,
        )
        .await
        .map_err(sql_error)?;
        let target = PublicationTarget::Peer { peer: self.sender };
        queries::write_publication(
            self.scope,
            item_key(publication.item),
            encode(&target)?,
            publication.version.value().to_string(),
            encode(publication)?,
        )
        .await
        .map_err(sql_error)?;
        state.head = cursor;
        queries::write_status_state(self.scope, encode(&state)?)
            .await
            .map_err(sql_error)?;
        Ok((
            event,
            Some(PublicationRef {
                item: publication.item,
                version: publication.version,
                target,
            }),
        ))
    }
    pub async fn fetch(
        &mut self,
        sender: PeerId,
        after: StatusCursor,
        limit: u32,
    ) -> Result<StatusFetchResult<D::Acceptance>, DeliveryError> {
        if sender != self.sender {
            return Err(DeliveryError::InvalidBatch);
        }
        let state = self.state().await?;
        if after > state.head {
            return Err(DeliveryError::InvalidBatch);
        }
        let rows =
            queries::read_status_events(self.scope, after.value().to_string(), limit.to_string())
                .await
                .map_err(sql_error)?;
        let mut events = Vec::with_capacity(rows.len());
        let mut through = after;
        for row in rows {
            let event = self.decode_event(row)?;
            if event.cursor <= through || event.cursor > state.head {
                return Err(DeliveryError::Corrupt {
                    field: "status stream order",
                });
            }
            through = event.cursor;
            events.push(event);
        }
        Ok(StatusFetchResult { events, through })
    }
}

pub(crate) fn canonical_encode(value: &impl Serialize) -> Result<String, DeliveryError> {
    let mut value =
        serde_json::to_value(value).map_err(|error| DeliveryError::Encoding(error.to_string()))?;
    value.sort_all_objects();
    encode(&value)
}
