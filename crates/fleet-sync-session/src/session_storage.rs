use crate::{checked_session_queries as checked, *};
use fleet_sync_protocol::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

pub(crate) fn store_error(error: impl std::fmt::Display) -> StoreError {
    StoreError::Unavailable {
        message: error.to_string(),
    }
}
fn corrupt(field: &str) -> StoreError {
    StoreError::Corrupt {
        field: field.into(),
    }
}
fn required(value: Option<String>, field: &str) -> Result<String, StoreError> {
    value.ok_or_else(|| corrupt(field))
}
fn canonical_counter(text: String, field: &str) -> Result<u64, StoreError> {
    let value = text.parse::<u64>().map_err(|_| corrupt(field))?;
    if value.to_string() != text {
        return Err(corrupt(field));
    }
    Ok(value)
}
fn decode_session(row: checked::StoredSessionRow) -> Result<SessionRecord, StoreError> {
    let peer = PeerId::try_from(required(row.peer, "peer")?).map_err(|_| corrupt("peer"))?;
    let session_id = SessionId::try_from(required(row.session_id, "session id")?)
        .map_err(|_| corrupt("session id"))?;
    let state = match (
        row.state_kind.as_deref(),
        row.connection_id,
        row.detached_since,
        row.end_reason,
    ) {
        (Some("active"), Some(connection), None, None) => SessionState::Active {
            connection: ConnectionId::try_from(connection)
                .map_err(|_| corrupt("session connection"))?,
        },
        (Some("detached"), None, Some(since), None) => {
            let milliseconds = since
                .parse::<i64>()
                .map_err(|_| corrupt("session detached time"))?;
            if milliseconds.to_string() != since {
                return Err(corrupt("session detached time"));
            }
            SessionState::Detached {
                since: SyncTime::from_unix_millis(milliseconds),
            }
        }
        (Some("ended"), None, None, Some(reason)) => SessionState::Ended {
            reason: match reason.as_str() {
                "newSession" => EndReason::NewSession,
                "peerRevoked" => EndReason::PeerRevoked,
                _ => return Err(corrupt("session end reason")),
            },
        },
        _ => return Err(corrupt("session state")),
    };
    Ok(SessionRecord {
        peer,
        session_id,
        state,
        notice_head: NoticePosition::new(canonical_counter(
            required(row.notice_head, "notice head")?,
            "notice head",
        )?),
    })
}
pub(crate) async fn next_attach_sequence<S>(
    scope: &mut S,
    peer: PeerId,
) -> Result<AttachSequence, SessionError>
where
    S: SessionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'v> String: Type<S::Database> + Encode<'v, S::Database>,
    for<'v> Option<String>: Type<S::Database> + Decode<'v, S::Database>,
    for<'r> &'r str: ColumnIndex<<S::Database as Database>::Row>,
{
    let previous = match checked::read_attach_sequence(scope, peer.to_string())
        .await
        .map_err(store_error)?
    {
        None => 0,
        Some(row) => {
            let text = required(row.sequence, "attach sequence")?;
            let value = text
                .parse::<u64>()
                .map_err(|_| corrupt("attach sequence"))?;
            if value.to_string() != text || AttachSequence::new(value).is_err() {
                return Err(corrupt("attach sequence").into());
            }
            value
        }
    };
    let next = previous
        .checked_add(1)
        .and_then(|value| AttachSequence::new(value).ok())
        .ok_or(SessionError::AttachSequenceExhausted)?;
    checked::write_attach_sequence(scope, peer.to_string(), next.value().to_string())
        .await
        .map_err(store_error)?;
    Ok(next)
}
pub(crate) async fn load_session<S>(
    scope: &mut S,
    key: &SessionKey,
) -> Result<Option<SessionRecord>, StoreError>
where
    S: SessionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    checked::load_session(
        scope,
        key.peer.as_uuid().to_string(),
        key.session_id.as_uuid().to_string(),
    )
    .await
    .map_err(store_error)?
    .map(decode_session)
    .transpose()
}
pub(crate) async fn save_session<S>(scope: &mut S, record: &SessionRecord) -> Result<(), StoreError>
where
    S: SessionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    checked::save_session(
        scope,
        record.peer.as_uuid().to_string(),
        record.session_id.as_uuid().to_string(),
        serde_json::to_string(record).map_err(store_error)?,
    )
    .await
    .map_err(store_error)
}
pub(crate) async fn sessions_for_peer<S>(
    scope: &mut S,
    peer: &PeerId,
) -> Result<Vec<SessionRecord>, StoreError>
where
    S: SessionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    checked::sessions_for_peer(scope, peer.as_uuid().to_string())
        .await
        .map_err(store_error)?
        .into_iter()
        .map(decode_session)
        .collect()
}
pub(crate) async fn save_notice<S, P: ConsumerProtocol>(
    scope: &mut S,
    key: &SessionKey,
    notice: &StoredNotice<P>,
) -> Result<(), StoreError>
where
    S: SessionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    checked::save_notice(
        scope,
        key.peer.as_uuid().to_string(),
        key.session_id.as_uuid().to_string(),
        notice.position.value().to_string(),
        serde_json::to_string(&notice.body).map_err(store_error)?,
    )
    .await
    .map_err(store_error)
}
pub(crate) async fn load_notices_after<S, P: ConsumerProtocol>(
    scope: &mut S,
    key: &SessionKey,
    after: NoticePosition,
) -> Result<Vec<StoredNotice<P>>, StoreError>
where
    S: SessionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let rows = checked::notices_after(
        scope,
        key.peer.as_uuid().to_string(),
        key.session_id.as_uuid().to_string(),
    )
    .await
    .map_err(store_error)?;
    let mut notices = Vec::new();
    for row in rows {
        let decimal = required(row.position, "notice position")?;
        let position = decimal
            .parse::<u64>()
            .map_err(|_| corrupt("notice position"))?;
        if position == 0 || decimal != position.to_string() {
            return Err(corrupt("notice position"));
        }
        let body = match (
            row.notice_kind.as_deref(),
            row.database_id,
            row.consumer_body,
        ) {
            (Some("leaseChanged"), Some(database), None) => Notice::LeaseChanged {
                database: DatabaseId::try_from(database).map_err(|_| corrupt("notice database"))?,
            },
            (Some("consumer"), None, Some(body)) => Notice::Consumer(
                serde_json::from_str(&body).map_err(|_| corrupt("consumer notice"))?,
            ),
            _ => return Err(corrupt("notice body")),
        };
        if position > after.value() {
            notices.push(StoredNotice {
                position: NoticePosition::new(position),
                body,
            });
        }
    }
    Ok(notices)
}

#[cfg(test)]
#[path = "session_storage_test.rs"]
mod tests;
