use fleet_sync_protocol::{AuthRefusal, CloseReason, SessionId};
pub use fleet_sync_session::CredentialRejectedRecovery;
use runtime_primitives::{Backoff, SyncTime};
use std::time::Duration;

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
pub enum SessionLinkEvent {
    Connecting { attempt: u32 },
    Welcomed { session_id: SessionId },
    Disconnected { reason: Option<CloseReason> },
    CredentialRejected { reason: AuthRefusal },
    SessionRestarted { old: SessionId, new: SessionId },
    Revoked,
    WelcomeTimeout,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionLinkStatus {
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
    Disconnected { reason: Option<CloseReason> },
    CredentialRejected { reason: AuthRefusal },
    Revoked,
    WelcomeTimeout,
}

/// A retained status snapshot, even when the event stream lags. The last drop
/// reason remains available after reconnect and Welcome until another drop.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionLinkState {
    pub status: SessionLinkStatus,
    pub last_drop_reason: Option<SessionDropReason>,
}

impl Default for SessionLinkState {
    fn default() -> Self {
        Self {
            status: SessionLinkStatus::Disconnected { reason: None },
            last_drop_reason: None,
        }
    }
}

impl SessionLinkState {
    pub(crate) fn observe(&mut self, event: &SessionLinkEvent, retry_at: Option<SyncTime>) {
        match event {
            SessionLinkEvent::Connecting { attempt } => {
                self.status = SessionLinkStatus::Connecting { attempt: *attempt };
            }
            SessionLinkEvent::Welcomed { session_id } => {
                self.status = SessionLinkStatus::Welcomed {
                    session_id: *session_id,
                };
            }
            SessionLinkEvent::Disconnected { reason } => {
                self.status = SessionLinkStatus::Disconnected {
                    reason: reason.clone(),
                };
                self.last_drop_reason = Some(SessionDropReason::Disconnected {
                    reason: reason.clone(),
                });
            }
            SessionLinkEvent::CredentialRejected { reason } => {
                self.status = SessionLinkStatus::CredentialRejected {
                    reason: *reason,
                    retry_at,
                };
                self.last_drop_reason =
                    Some(SessionDropReason::CredentialRejected { reason: *reason });
            }
            SessionLinkEvent::Revoked => {
                self.status = SessionLinkStatus::Revoked;
                self.last_drop_reason = Some(SessionDropReason::Revoked);
            }
            SessionLinkEvent::WelcomeTimeout => {
                self.status = SessionLinkStatus::Disconnected { reason: None };
                self.last_drop_reason = Some(SessionDropReason::WelcomeTimeout);
            }
            SessionLinkEvent::SessionRestarted { .. } => {}
        }
    }
}
