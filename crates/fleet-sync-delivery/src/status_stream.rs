use crate::{DeliveryError, checked_queries as queries, custody_record::*};
use crate::{status_row::PublicationRow, stored_outcome::stored_enum};
use durable_steps::storage::TransactionScope;
use fleet_sync_protocol::*;
use serde::{Deserialize, Serialize};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::marker::PhantomData;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum PublicationTarget<R> {
    StatusStream { sender: PeerId, route: R },
    Peer { peer: PeerId, route: R },
}
impl<R> PublicationTarget<R> {
    pub(crate) const fn machine(&self) -> PeerId {
        match self {
            Self::StatusStream { sender, .. } => *sender,
            Self::Peer { peer, .. } => *peer,
        }
    }
    pub(crate) const fn route(&self) -> &R {
        match self {
            Self::StatusStream { route, .. } | Self::Peer { route, .. } => route,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PublicationRef<R> {
    pub item: ItemId,
    pub version: StatusVersion,
    pub target: PublicationTarget<R>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StreamState<R> {
    sender: PeerId,
    owner_route: R,
    head: StatusCursor,
    floor: StatusCursor,
}

pub struct StatusStream<'s, S, D: DeliveryTypes> {
    scope: &'s mut S,
    sender: PeerId,
    owner_route: D::StatusRoute,
    payload: PhantomData<fn() -> D>,
}
impl<'s, S, D: DeliveryTypes> StatusStream<'s, S, D> {
    pub fn new(scope: &'s mut S, sender: PeerId, owner_route: D::StatusRoute) -> Self {
        Self {
            scope,
            sender,
            owner_route,
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
    pub async fn append_and_admit<T: durable_steps::Task<Input = PublicationRef<D::StatusRoute>>>(
        &mut self,
        publication: &StatusPublication<D::Acceptance>,
        now: SyncTime,
    ) -> Result<StatusEvent<D::Acceptance>, DeliveryError>
    where
        S: durable_steps::JournalScope,
    {
        let (event, reference) = self.append(publication, now).await?;
        if let Some(reference) = reference {
            self.scope
                .admit::<T>(
                    &crate::status_publication::publication_request_key(&reference)?,
                    &reference,
                )
                .await
                .map_err(|error| DeliveryError::Storage(error.to_string()))?;
        }
        Ok(event)
    }
    async fn state(&mut self) -> Result<StreamState<D::StatusRoute>, DeliveryError> {
        crate::delivery_sizes::check_status_route::<D>(&self.owner_route)?;
        let state: StreamState<D::StatusRoute> =
            match queries::read_status_state(self.scope, self.sender.as_uuid().to_string())
                .await
                .map_err(sql_error)?
            {
                Some(row) => {
                    stored_row_id(&row.id, "_fleet_sync_status_state", self.sender.to_string())?;
                    StreamState {
                        sender: stored_enum(row.machine_id, "status stream machine")?,
                        owner_route: decode(row.owner_route, "status stream route")?,
                        head: StatusCursor::new(integer(
                            row.head,
                            row.head_class,
                            "status stream head",
                        )?),
                        floor: StatusCursor::new(integer(
                            row.floor,
                            row.floor_class,
                            "status stream floor",
                        )?),
                    }
                }
                None => StreamState {
                    sender: self.sender,
                    owner_route: self.owner_route.clone(),
                    head: StatusCursor::new(0),
                    floor: StatusCursor::new(0),
                },
            };
        crate::delivery_sizes::check_status_route::<D>(&state.owner_route).map_err(|_| {
            DeliveryError::Corrupt {
                field: "stored stream route bounds",
            }
        })?;
        if state.sender != self.sender
            || state.head.value() > i64::MAX as u64
            || state.floor > state.head
            || canonical_encode(&state.owner_route)? != canonical_encode(&self.owner_route)?
        {
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
        let publication = row.publication()?;
        let event = StatusEvent {
            cursor: StatusCursor::new(integer(
                row.cursor.clone(),
                row.cursor_class.clone(),
                "status cursor",
            )?),
            item: publication.item,
            status: publication.status,
            version: publication.version,
            cancel: publication.cancel,
        };
        if event.cursor.value() == 0
            || row.machine_id.as_deref() != Some(self.sender.as_uuid().to_string().as_str())
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
        now: SyncTime,
    ) -> Result<
        (
            StatusEvent<D::Acceptance>,
            Option<PublicationRef<D::StatusRoute>>,
        ),
        DeliveryError,
    > {
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
            self.sender.as_uuid().to_string(),
        )
        .await
        .map_err(sql_error)?
        {
            let event = self.decode_event(row)?;
            if event.cursor <= state.floor || event.cursor > state.head {
                return Err(DeliveryError::Corrupt {
                    field: "status stream event cursor",
                });
            }
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
            self.sender.as_uuid().to_string(),
            now.unix_millis().to_string(),
            now.unix_millis().to_string(),
        )
        .await
        .map_err(sql_error)?;
        let target = PublicationTarget::Peer {
            peer: self.sender,
            route: state.owner_route.clone(),
        };
        queries::write_publication(
            self.scope,
            item_key(publication.item),
            canonical_encode(&target)?,
            publication.version.value().to_string(),
            encode(publication)?,
            now.unix_millis().to_string(),
            now.unix_millis().to_string(),
        )
        .await
        .map_err(sql_error)?;
        state.head = cursor;
        queries::write_status_state(
            self.scope,
            encode(&state)?,
            self.sender.as_uuid().to_string(),
            now.unix_millis().to_string(),
            now.unix_millis().to_string(),
        )
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
    /// Retire a durably applied prefix in the host's transaction.
    pub async fn acknowledge(
        &mut self,
        through: StatusCursor,
        now: SyncTime,
    ) -> Result<StatusAckResult, DeliveryError> {
        let mut state = self.state().await?;
        if through > state.head {
            return Ok(StatusAckResult::Refused { head: state.head });
        }
        if through > state.floor {
            queries::drop_status_events(
                self.scope,
                through.value().to_string(),
                self.sender.as_uuid().to_string(),
            )
            .await
            .map_err(sql_error)?;
            state.floor = through;
            queries::write_status_state(
                self.scope,
                encode(&state)?,
                self.sender.as_uuid().to_string(),
                now.unix_millis().to_string(),
                now.unix_millis().to_string(),
            )
            .await
            .map_err(sql_error)?;
        }
        Ok(StatusAckResult::Acknowledged { floor: state.floor })
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
        let rows = queries::read_status_events(
            self.scope,
            after.max(state.floor).value().to_string(),
            limit.to_string(),
            self.sender.as_uuid().to_string(),
        )
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
