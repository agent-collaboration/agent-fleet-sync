use crate::*;
use fleet_sync_protocol::*;

/// Call inside the transaction that creates this notice. Effects run after commit.
pub async fn publish_notice<S: SessionScope<P>, P: ConsumerProtocol>(
    scope: &mut S,
    key: &SessionKey,
    body: Notice<P>,
) -> Result<Vec<SessionEffect<P>>, SessionError> {
    let mut record = scope
        .load_session(key)
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
    scope.save_notice(key, &notice).await?;
    record.notice_head = position;
    scope.save_session(&record).await?;
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

pub async fn notices_after<S: SessionScope<P>, P: ConsumerProtocol>(
    scope: &mut S,
    key: &SessionKey,
    after: NoticePosition,
) -> Result<Vec<StoredNotice<P>>, SessionError> {
    Ok(scope.notices_after(key, after).await?)
}
