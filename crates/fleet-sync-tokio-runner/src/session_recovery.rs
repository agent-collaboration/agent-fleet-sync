use fleet_sync_protocol::{AuthRefusal, CloseReason, SessionId};
use runtime_primitives::{Backoff, SyncTime};
use std::time::Duration;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialRejectedRecovery {
    Terminal,
    Retry { backoff: Backoff },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionEndedRecovery {
    Terminal,
    StartNewSession,
}

/// Explicitly opt in with `SessionPump::with_recovery_policy`.
/// Revoked credentials are terminal under every policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionRecoveryPolicy {
    pub credential_rejected: CredentialRejectedRecovery,
    pub session_ended: SessionEndedRecovery,
    pub welcome_timeout: Option<Duration>,
}

impl SessionRecoveryPolicy {
    /// Disable recovery for credential refusals and ended sessions.
    pub const fn terminal() -> Self {
        Self {
            credential_rejected: CredentialRejectedRecovery::Terminal,
            session_ended: SessionEndedRecovery::Terminal,
            welcome_timeout: None,
        }
    }
}

impl Default for SessionRecoveryPolicy {
    fn default() -> Self {
        Self {
            credential_rejected: CredentialRejectedRecovery::Retry {
                backoff: Backoff {
                    initial: Duration::from_secs(1),
                    max: Duration::from_secs(5 * 60),
                    factor: 2,
                },
            },
            session_ended: SessionEndedRecovery::StartNewSession,
            welcome_timeout: None,
        }
    }
}

/// Link transition, published without waiting for an observer. A slow broadcast
/// receiver may lag and miss intermediate events. No authentication material.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionLinkEventKind {
    Connecting { attempt: u32 },
    Welcomed { session_id: SessionId },
    Disconnected { reason: SessionDropReason },
    CredentialRejected { reason: AuthRefusal },
    SessionRestarted { old: SessionId, new: SessionId },
    Revoked,
    WelcomeTimeout,
    ReplacedByNewerSession,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionLinkStatus {
    Refused {
        reason: SessionDropReason,
    },
    Connecting {
        attempt: u32,
    },
    Welcomed {
        session_id: SessionId,
    },
    Disconnected {
        reason: Option<CloseReason>,
    },
    CredentialRejected {
        reason: AuthRefusal,
        retry_at: Option<SyncTime>,
    },
    Revoked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionDropReason {
    ReplacedByNewerSession,
    IdleTimeout,
    SendTimeout,
    Disconnected { reason: Option<CloseReason> },
    CredentialRejected { reason: AuthRefusal },
    Revoked,
    WelcomeTimeout,
}

/// A transition correlated with one monotonic connection attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionLinkEvent {
    pub connection: u64,
    pub kind: SessionLinkEventKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionDrop {
    pub reason: SessionDropReason,
    pub at: SyncTime,
    pub connection: u64,
}

/// The pump's retained, revisioned snapshot. Contact timestamps describe inbound
/// session bytes; transports may handle control Ping/Pong frames privately.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionLinkState {
    pub status: SessionLinkStatus,
    pub connection: u64,
    pub since: SyncTime,
    pub last_frame_at: Option<SyncTime>,
    pub last_drop: Option<SessionDrop>,
    pub revision: u64,
}

impl Default for SessionLinkState {
    fn default() -> Self {
        Self {
            status: SessionLinkStatus::Disconnected { reason: None },
            connection: 0,
            since: SyncTime::from_unix_millis(0),
            last_frame_at: None,
            last_drop: None,
            revision: 0,
        }
    }
}

impl SessionLinkState {
    pub(crate) fn observe(
        &mut self,
        event: &SessionLinkEventKind,
        retry_at: Option<SyncTime>,
        connection: u64,
        at: SyncTime,
        last_frame_at: Option<SyncTime>,
    ) -> bool {
        let previous_status = self.status.clone();
        let previous_connection = self.connection;
        let drop_reason = match event {
            SessionLinkEventKind::Connecting { attempt } => {
                self.status = SessionLinkStatus::Connecting { attempt: *attempt };
                None
            }
            SessionLinkEventKind::Welcomed { session_id } => {
                self.status = SessionLinkStatus::Welcomed {
                    session_id: *session_id,
                };
                None
            }
            SessionLinkEventKind::Disconnected { reason } => {
                self.status = SessionLinkStatus::Disconnected {
                    reason: match reason {
                        SessionDropReason::Disconnected { reason } => reason.clone(),
                        _ => None,
                    },
                };
                Some(reason.clone())
            }
            SessionLinkEventKind::CredentialRejected { reason } => {
                self.status = SessionLinkStatus::CredentialRejected {
                    reason: *reason,
                    retry_at,
                };
                Some(SessionDropReason::CredentialRejected { reason: *reason })
            }
            SessionLinkEventKind::Revoked => {
                self.status = SessionLinkStatus::Revoked;
                Some(SessionDropReason::Revoked)
            }
            SessionLinkEventKind::WelcomeTimeout => {
                self.status = SessionLinkStatus::Disconnected { reason: None };
                Some(SessionDropReason::WelcomeTimeout)
            }
            SessionLinkEventKind::ReplacedByNewerSession => {
                self.status = SessionLinkStatus::Refused {
                    reason: SessionDropReason::ReplacedByNewerSession,
                };
                Some(SessionDropReason::ReplacedByNewerSession)
            }
            SessionLinkEventKind::SessionRestarted { .. } => None,
        };
        let next_drop = drop_reason.map(|reason| SessionDrop {
            reason,
            at,
            connection,
        });
        let status_changed = self.status != previous_status || connection != previous_connection;
        let drop_changed = next_drop
            .as_ref()
            .is_some_and(|drop| self.last_drop.as_ref() != Some(drop));
        if !status_changed && !drop_changed {
            return false;
        }
        self.connection = connection;
        self.last_frame_at = last_frame_at;
        if status_changed {
            self.since = at;
        }
        self.revision = self.revision.saturating_add(1);
        if let Some(drop) = next_drop {
            self.last_drop = Some(drop);
        }
        true
    }
}
