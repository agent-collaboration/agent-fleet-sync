use crate::session_storage::{load_notices_after, load_session, save_notice, save_session};
use crate::*;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

use fleet_sync_protocol::*;

/// Call inside the transaction that creates this notice. Effects run after commit.
pub async fn publish_notice<S, P: ConsumerProtocol>(
    scope: &mut S,
    key: &SessionKey,
    body: Notice<P>,
) -> Result<Vec<SessionEffect<P>>, SessionError>
where
    S: SessionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let mut record = load_session(scope, key)
        .await?
        .ok_or(SessionError::SessionEnded)?;
    if matches!(record.state, SessionState::Ended { .. }) {
        return Err(SessionError::SessionEnded);
    }
    let position = record
        .notice_head
        .next()
        .ok_or(SessionError::NoticePositionExhausted)?;
    let notice = StoredNotice { position, body };
    save_notice(scope, key, &notice).await?;
    record.notice_head = position;
    save_session(scope, &record).await?;
    Ok(match record.state {
        SessionState::Active { connection } => vec![SessionEffect::Send {
            connection,
            frame: ServerFrame::Notice {
                position,
                body: notice.body,
            },
        }],
        SessionState::Detached { .. } | SessionState::Ended { .. } => Vec::new(),
    })
}

pub async fn notices_after<S, P: ConsumerProtocol>(
    scope: &mut S,
    key: &SessionKey,
    after: NoticePosition,
) -> Result<Vec<StoredNotice<P>>, SessionError>
where
    S: SessionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    Ok(load_notices_after(scope, key, after).await?)
}
