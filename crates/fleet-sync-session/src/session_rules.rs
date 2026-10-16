use crate::*;
use fleet_sync_protocol::*;

pub async fn connection_opened<S: SessionScope<P>, P: ConsumerProtocol>(
    scope: &mut S,
    binding: &ConnectionBinding,
    now: SyncTime,
) -> Result<Vec<SessionEffect<P>>, SessionError> {
    if scope.registration(binding.connection).await?.is_some() {
        return Ok(Vec::new());
    }
    scope
        .save_registration(binding.connection, ConnectionRegistration::Opening)
        .await?;
    if binding.expires_at <= now {
        return close_connection(
            scope,
            binding,
            CloseReason::CredentialRejected(AuthRefusal::Expired),
            now,
        )
        .await;
    }
    Ok(Vec::new())
}

pub async fn frame_received<S: SessionScope<P>, P: ConsumerProtocol>(
    scope: &mut S,
    binding: &ConnectionBinding,
    frame: &[u8],
    now: SyncTime,
) -> Result<Vec<SessionEffect<P>>, SessionError> {
    let Some(registration) = scope.registration(binding.connection).await? else {
        return Ok(Vec::new());
    };
    if registration == ConnectionRegistration::Closed {
        return Ok(Vec::new());
    }
    if binding.expires_at <= now {
        return close_connection(
            scope,
            binding,
            CloseReason::CredentialRejected(AuthRefusal::Expired),
            now,
        )
        .await;
    }
    let frame = match serde_json::from_slice::<ClientFrame<P>>(frame) {
        Ok(frame) => frame,
        Err(_) => return close_connection(scope, binding, CloseReason::MalformedFrame, now).await,
    };
    if let ClientFrame::Hello {
        session_id,
        schema_support,
    } = frame
    {
        return attach_session(
            scope,
            binding,
            registration,
            session_id,
            schema_support,
            now,
        )
        .await;
    }
    let ConnectionRegistration::Attached { session_id, .. } = registration else {
        return close_connection(scope, binding, CloseReason::MalformedFrame, now).await;
    };
    let key = SessionKey {
        peer: binding.peer,
        session_id,
    };
    let Some(record) = scope.load_session(&key).await? else {
        return Ok(Vec::new());
    };
    if record.state
        != (SessionState::Active {
            connection: binding.connection,
        })
    {
        return Ok(Vec::new());
    }
    Ok(match frame {
        ClientFrame::Request { frame, body } => vec![SessionEffect::Route {
            session: key,
            frame,
            operation: body,
        }],
        ClientFrame::Answer { frame, body } => vec![SessionEffect::RouteAnswer {
            session: key,
            frame,
            answer: body,
        }],
        ClientFrame::Hello { .. } => Vec::new(),
    })
}

async fn attach_session<S: SessionScope<P>, P: ConsumerProtocol>(
    scope: &mut S,
    binding: &ConnectionBinding,
    registration: ConnectionRegistration,
    session_id: SessionId,
    schema_support: SchemaSupport,
    now: SyncTime,
) -> Result<Vec<SessionEffect<P>>, SessionError> {
    let key = SessionKey {
        peer: binding.peer,
        session_id,
    };
    match registration {
        ConnectionRegistration::Closed => return Ok(Vec::new()),
        ConnectionRegistration::Attached {
            session_id: attached,
            schema_support: declared,
        } => {
            if attached != session_id || declared != schema_support {
                return close_connection(scope, binding, CloseReason::MalformedFrame, now).await;
            }
            let Some(record) = scope.load_session(&key).await? else {
                return Ok(Vec::new());
            };
            if record.state
                != (SessionState::Active {
                    connection: binding.connection,
                })
            {
                return Ok(Vec::new());
            }
            return welcome(scope, binding, session_id).await;
        }
        ConnectionRegistration::Opening => {}
    }
    let existing = scope.load_session(&key).await?;
    if existing
        .as_ref()
        .is_some_and(|record| matches!(record.state, SessionState::Ended { .. }))
    {
        return close_connection(scope, binding, CloseReason::SessionEnded, now).await;
    }
    let mut effects = Vec::new();
    for mut previous in scope.sessions_for_peer(&binding.peer).await? {
        if previous.session_id == session_id || matches!(previous.state, SessionState::Ended { .. })
        {
            continue;
        }
        if let SessionState::Active { connection } = previous.state {
            scope
                .save_registration(connection, ConnectionRegistration::Closed)
                .await?;
            effects.extend(closed_effects(connection, CloseReason::SessionEnded));
        }
        previous.state = SessionState::Ended {
            reason: EndReason::NewSession,
        };
        scope.save_session(&previous).await?;
    }
    let mut record = existing.unwrap_or(SessionRecord {
        peer: binding.peer,
        session_id,
        state: SessionState::Detached { since: now },
        notice_head: NoticePosition::default(),
    });
    if let SessionState::Active {
        connection: previous,
    } = record.state
        && previous != binding.connection
    {
        scope
            .save_registration(previous, ConnectionRegistration::Closed)
            .await?;
        effects.extend(closed_effects(previous, CloseReason::ConnectionReplaced));
    }
    record.state = SessionState::Active {
        connection: binding.connection,
    };
    scope.save_session(&record).await?;
    scope
        .save_registration(
            binding.connection,
            ConnectionRegistration::Attached {
                session_id,
                schema_support,
            },
        )
        .await?;
    effects.extend(welcome(scope, binding, session_id).await?);
    Ok(effects)
}

async fn welcome<S: SessionScope<P>, P: ConsumerProtocol>(
    scope: &mut S,
    binding: &ConnectionBinding,
    session_id: SessionId,
) -> Result<Vec<SessionEffect<P>>, SessionError> {
    let databases = scope.database_statuses(&binding.peer).await?;
    Ok(vec![SessionEffect::Send {
        connection: binding.connection,
        frame: ServerFrame::Welcome {
            session_id,
            databases,
        },
    }])
}

pub async fn connection_closed<S: SessionScope<P>, P: ConsumerProtocol>(
    scope: &mut S,
    binding: &ConnectionBinding,
    _cause: CloseCause,
    now: SyncTime,
) -> Result<Vec<SessionEffect<P>>, SessionError> {
    let registration = scope.registration(binding.connection).await?;
    scope
        .save_registration(binding.connection, ConnectionRegistration::Closed)
        .await?;
    if let Some(ConnectionRegistration::Attached { session_id, .. }) = registration {
        let key = SessionKey {
            peer: binding.peer,
            session_id,
        };
        if let Some(mut record) = scope.load_session(&key).await?
            && record.state
                == (SessionState::Active {
                    connection: binding.connection,
                })
        {
            record.state = SessionState::Detached { since: now };
            scope.save_session(&record).await?;
        }
    }
    Ok(Vec::new())
}

pub async fn end_sessions<S: SessionScope<P>, P: ConsumerProtocol>(
    scope: &mut S,
    peer: &PeerId,
    reason: EndReason,
    _now: SyncTime,
) -> Result<Vec<SessionEffect<P>>, SessionError> {
    let mut effects = Vec::new();
    for mut record in scope.sessions_for_peer(peer).await? {
        if matches!(record.state, SessionState::Ended { .. }) {
            continue;
        }
        if let SessionState::Active { connection } = record.state {
            scope
                .save_registration(connection, ConnectionRegistration::Closed)
                .await?;
            let close = match reason {
                EndReason::NewSession => CloseReason::SessionEnded,
                EndReason::PeerRevoked => CloseReason::CredentialRejected(AuthRefusal::Revoked),
            };
            effects.extend(closed_effects(connection, close));
        }
        record.state = SessionState::Ended { reason };
        scope.save_session(&record).await?;
    }
    Ok(effects)
}

fn closed_effects<P: ConsumerProtocol>(
    connection: ConnectionId,
    reason: CloseReason,
) -> Vec<SessionEffect<P>> {
    vec![
        SessionEffect::Send {
            connection,
            frame: ServerFrame::Closed {
                reason: reason.clone(),
            },
        },
        SessionEffect::Close { connection, reason },
    ]
}

async fn close_connection<S: SessionScope<P>, P: ConsumerProtocol>(
    scope: &mut S,
    binding: &ConnectionBinding,
    reason: CloseReason,
    now: SyncTime,
) -> Result<Vec<SessionEffect<P>>, SessionError> {
    connection_closed(scope, binding, CloseCause::Dropped, now).await?;
    Ok(closed_effects(binding.connection, reason))
}
