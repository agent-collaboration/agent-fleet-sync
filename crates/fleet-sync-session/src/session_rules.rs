use crate::session_storage::{load_session, save_session, sessions_for_peer};
use crate::*;
use serde::Deserialize;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

use fleet_sync_protocol::*;

// Identify correlation independently from the consumer's body types. Unknown
// fields are checked by ClientFrame's full decoder after this envelope is read.
#[derive(Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
enum ClientEnvelope {
    Hello {},
    Request { frame: FrameId },
    Answer { frame: FrameId },
}
impl ClientEnvelope {
    fn frame(&self) -> Option<FrameId> {
        match self {
            Self::Hello {} => None,
            Self::Request { frame } | Self::Answer { frame } => Some(*frame),
        }
    }
}

fn invalid_shape_field(error: &serde_json::Error) -> String {
    // Serde's Display wording is version-dependent. Keep the body fallback and
    // cap reflected client field names without splitting a Unicode character.
    const MAX_FIELD_CHARACTERS: usize = 64;
    let diagnostic = error.to_string();
    ["missing field `", "unknown field `", "duplicate field `"]
        .iter()
        .find_map(|prefix| {
            let detail = diagnostic.strip_prefix(prefix)?;
            if *prefix == "unknown field `" {
                // The field itself can contain backticks or suffix-like text.
                // Split at Serde's final diagnostic suffix, not its first tick.
                detail
                    .rsplit_once("`, expected ")
                    .or_else(|| detail.rsplit_once("`, there are no fields"))
                    .map(|(field, _)| field)
            } else {
                detail.rsplit_once('`').map(|(field, _)| field)
            }
        })
        // Serde reports no field location for some type/variant failures.
        .unwrap_or("body")
        .chars()
        .take(MAX_FIELD_CHARACTERS)
        .collect()
}

pub async fn connection_opened<S, P: ConsumerProtocol>(
    scope: &mut S,
    binding: &ConnectionBinding,
    now: SyncTime,
) -> Result<Vec<SessionEffect<P>>, SessionError>
where
    S: SessionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    if scope.registration(&binding.connection).is_some() {
        return Ok(Vec::new());
    }
    scope.set_registration(&binding.connection, ConnectionRegistration::Opening);
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

pub async fn frame_received<S, P: ConsumerProtocol>(
    scope: &mut S,
    binding: &ConnectionBinding,
    frame: &[u8],
    now: SyncTime,
) -> Result<Vec<SessionEffect<P>>, SessionError>
where
    S: SessionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let Some(registration) = scope.registration(&binding.connection) else {
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
    let envelope = match serde_json::from_slice::<ClientEnvelope>(frame) {
        Ok(envelope) => envelope,
        Err(_) => return close_connection(scope, binding, CloseReason::MalformedFrame, now).await,
    };
    let frame = match serde_json::from_slice::<ClientFrame<P>>(frame) {
        Ok(frame) => frame,
        Err(error) => {
            if let Some(frame) = envelope.frame() {
                return Ok(vec![SessionEffect::Send {
                    connection: binding.connection,
                    frame: ServerFrame::Answer {
                        frame,
                        body: OperationAnswer::InvalidShape {
                            field: invalid_shape_field(&error),
                        },
                    },
                }]);
            }
            return close_connection(scope, binding, CloseReason::MalformedFrame, now).await;
        }
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
    let Some(record) = load_session(scope, &key).await? else {
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

async fn attach_session<S, P: ConsumerProtocol>(
    scope: &mut S,
    binding: &ConnectionBinding,
    registration: ConnectionRegistration,
    session_id: SessionId,
    schema_support: SchemaSupport,
    now: SyncTime,
) -> Result<Vec<SessionEffect<P>>, SessionError>
where
    S: SessionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let key = SessionKey {
        peer: binding.peer,
        session_id,
    };
    match registration {
        ConnectionRegistration::Closed => return Ok(Vec::new()),
        ConnectionRegistration::Attaching {
            session_id: attached,
            schema_support: declared,
            ..
        } => {
            if attached != session_id || declared != schema_support {
                return close_connection(scope, binding, CloseReason::MalformedFrame, now).await;
            }
            return Ok(Vec::new());
        }
        ConnectionRegistration::Attached {
            session_id: attached,
            schema_support: declared,
        } => {
            if attached != session_id || declared != schema_support {
                return close_connection(scope, binding, CloseReason::MalformedFrame, now).await;
            }
            let Some(record) = load_session(scope, &key).await? else {
                return Ok(Vec::new());
            };
            if record.state
                != (SessionState::Active {
                    connection: binding.connection,
                })
            {
                return Ok(Vec::new());
            }
            return Ok(welcome(binding, session_id));
        }
        ConnectionRegistration::Opening => {}
    }
    let existing = load_session(scope, &key).await?;
    if existing
        .as_ref()
        .is_some_and(|record| matches!(record.state, SessionState::Ended { .. }))
    {
        return close_connection(scope, binding, CloseReason::SessionEnded, now).await;
    }
    let mut effects = Vec::new();
    for mut previous in sessions_for_peer(scope, &binding.peer).await? {
        if previous.session_id == session_id || matches!(previous.state, SessionState::Ended { .. })
        {
            continue;
        }
        if let SessionState::Active { connection } = previous.state {
            scope.set_registration(&connection, ConnectionRegistration::Closed);
            effects.extend(closed_effects(connection, CloseReason::SessionEnded));
        }
        previous.state = SessionState::Ended {
            reason: EndReason::NewSession,
        };
        save_session(scope, &previous).await?;
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
        scope.set_registration(&previous, ConnectionRegistration::Closed);
        effects.extend(closed_effects(previous, CloseReason::ConnectionReplaced));
    }
    record.state = SessionState::Active {
        connection: binding.connection,
    };
    save_session(scope, &record).await?;
    let attach_seq = crate::session_storage::next_attach_sequence(scope, binding.peer).await?;
    scope.set_registration(
        &binding.connection,
        ConnectionRegistration::Attaching {
            session_id,
            schema_support,
            attach_seq,
        },
    );
    scope
        .publish_schema_support(
            PublishSchemaSupportInput {
                connection: binding.connection,
                expires_at: binding.expires_at,
                report: ReportSchemaSupport {
                    peer: binding.peer,
                    session_id,
                    attach_seq,
                    support: schema_support,
                },
            },
            now,
        )
        .await?;
    Ok(effects)
}

fn welcome<P: ConsumerProtocol>(
    binding: &ConnectionBinding,
    session_id: SessionId,
) -> Vec<SessionEffect<P>> {
    vec![SessionEffect::Welcome {
        connection: binding.connection,
        peer: binding.peer,
        session_id,
    }]
}

pub async fn connection_closed<S, P: ConsumerProtocol>(
    scope: &mut S,
    binding: &ConnectionBinding,
    _cause: CloseCause,
    now: SyncTime,
) -> Result<Vec<SessionEffect<P>>, SessionError>
where
    S: SessionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let registration = scope.registration(&binding.connection);
    scope.set_registration(&binding.connection, ConnectionRegistration::Closed);
    if let Some(
        ConnectionRegistration::Attached { session_id, .. }
        | ConnectionRegistration::Attaching { session_id, .. },
    ) = registration
    {
        let key = SessionKey {
            peer: binding.peer,
            session_id,
        };
        if let Some(mut record) = load_session(scope, &key).await?
            && record.state
                == (SessionState::Active {
                    connection: binding.connection,
                })
        {
            record.state = SessionState::Detached { since: now };
            save_session(scope, &record).await?;
        }
    }
    Ok(Vec::new())
}

pub async fn end_sessions<S, P: ConsumerProtocol>(
    scope: &mut S,
    peer: &PeerId,
    reason: EndReason,
    _now: SyncTime,
) -> Result<Vec<SessionEffect<P>>, SessionError>
where
    S: SessionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    let mut effects = Vec::new();
    for mut record in sessions_for_peer(scope, peer).await? {
        if matches!(record.state, SessionState::Ended { .. }) {
            continue;
        }
        if let SessionState::Active { connection } = record.state {
            scope.set_registration(&connection, ConnectionRegistration::Closed);
            let close = match reason {
                EndReason::NewSession => CloseReason::SessionEnded,
                EndReason::PeerRevoked => CloseReason::CredentialRejected(AuthRefusal::Revoked),
            };
            effects.extend(closed_effects(connection, close));
        }
        record.state = SessionState::Ended { reason };
        save_session(scope, &record).await?;
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

async fn close_connection<S, P: ConsumerProtocol>(
    scope: &mut S,
    binding: &ConnectionBinding,
    reason: CloseReason,
    now: SyncTime,
) -> Result<Vec<SessionEffect<P>>, SessionError>
where
    S: SessionScope,
    <S::Database as Database>::Arguments: IntoArguments<S::Database>,
    for<'value> String: Type<S::Database> + Encode<'value, S::Database>,
    for<'value> Option<String>: Type<S::Database> + Decode<'value, S::Database>,
    for<'row> &'row str: ColumnIndex<<S::Database as Database>::Row>,
{
    connection_closed::<S, P>(scope, binding, CloseCause::Dropped, now).await?;
    Ok(closed_effects(binding.connection, reason))
}
