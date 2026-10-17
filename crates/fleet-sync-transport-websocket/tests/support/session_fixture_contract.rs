use fleet_sync_protocol::*;
use fleet_sync_session::{SessionRecord, SessionState};
use serde::{Deserialize, Serialize};

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct FixtureProtocol;
impl ConsumerProtocol for FixtureProtocol {
    type Delivery = fleet_sync_protocol::NoDelivery;
    type Request = Increment;
    type Answer = IncrementAnswer;
    type ServerRequest = ();
    type ClientAnswer = ();
    type Notice = ();
    type Annotation = ();
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Increment {
    pub operation: RequestId,
    pub amount: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum IncrementAnswer {
    Applied { value: u64 },
    Conflict,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IncrementReceipt {
    pub operation: RequestId,
    pub amount: u64,
    pub value: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FixtureState {
    pub receipts: Vec<IncrementReceipt>,
    pub value: u64,
    pub now: SyncTime,
    pub lease_expires_at: SyncTime,
    pub revoked: bool,
}
impl Default for FixtureState {
    fn default() -> Self {
        Self {
            receipts: vec![],
            value: 0,
            now: SyncTime::from_unix_millis(0),
            lease_expires_at: SyncTime::from_unix_millis(1_000),
            revoked: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ControlRequest {
    Snapshot,
    WaitForOpening,
    DropNextAnswer,
    SetUnreadableCopy { unreadable: bool },
    FailNextWelcome,
    AdvanceClock { now: SyncTime },
    SetLease { expires_at: SyncTime },
    Revoke,
    Allow,
    HoldNextClose,
    ReleaseHeldClose,
    Stop,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FixtureSnapshot {
    pub value: u64,
    pub receipt_count: usize,
    pub sessions: Vec<SessionRecord>,
    pub registrations: Vec<(ConnectionId, Option<SessionId>)>,
    pub held_closes: usize,
}
impl FixtureSnapshot {
    pub fn active_connection(&self, id: SessionId) -> Option<ConnectionId> {
        self.sessions.iter().find_map(|record| {
            if record.session_id == id {
                match record.state {
                    SessionState::Active { connection } => Some(connection),
                    _ => None,
                }
            } else {
                None
            }
        })
    }
}
