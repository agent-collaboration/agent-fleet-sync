use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

pub use runtime_primitives::{CallId, InvalidName, OperationId, StepName};

macro_rules! validated_name {
    ($name:ident, $field:literal) => {
        #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, InvalidName> {
                let value = value.into();
                if value.is_empty() {
                    return Err(InvalidName { field: $field });
                }
                Ok(Self(value))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl TryFrom<String> for $name {
            type Error = InvalidName;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

validated_name!(RequestKey, "request_key");

pub use runtime_primitives::{SyncTime, WakeFailure};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallReplay {
    Deduplicated,
    NeverRepeat,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DriveLease {
    pub token: Uuid,
    pub until: SyncTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Error, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OperationFailure {
    #[error("step sequence changed at position {position}: expected {expected}, found {found}")]
    Nondeterministic {
        position: u64,
        expected: String,
        found: String,
    },
    #[error("operation version {recorded} cannot run as version {registered}")]
    VersionMismatch { recorded: u32, registered: u32 },
    #[error("call outcome is uncertain for step {step:?}")]
    Uncertain { step: StepName },
    #[error("operation refused: {code}")]
    Refused { code: String, detail: String },
    #[error("operation payload could not be decoded: {message}")]
    InvalidPayload { message: String },
    #[error("step requested retry after {after_ms} ms")]
    Retry { after_ms: u64 },
    #[error("step storage failed: {error}")]
    Storage { error: StoreError },
}

#[derive(Clone, Debug, Eq, PartialEq, Error, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StoreError {
    #[error("storage backend: {message}")]
    Backend { message: String },
    #[error("invalid persisted field {field}: {message}")]
    Corrupt { field: String, message: String },
    #[error("operation {id:?} does not exist")]
    MissingOperation { id: OperationId },
    #[error("drive token no longer owns operation {id:?}")]
    StaleDriveToken { id: OperationId },
    #[error("transaction refused: {reason}")]
    Aborted { reason: String },
    #[error("local step rolled back: {failure}")]
    StepRollback { failure: Box<OperationFailure> },
}

impl StoreError {
    pub fn backend(error: impl std::fmt::Display) -> Self {
        Self::Backend {
            message: error.to_string(),
        }
    }
}

#[derive(Clone, Debug, Error)]
pub enum StepError {
    #[error("step will retry after {after:?}")]
    Retry { after: Duration },
    #[error(transparent)]
    Failed(#[from] OperationFailure),
    #[error(transparent)]
    Storage(#[from] StoreError),
}

impl From<StepError> for OperationFailure {
    fn from(error: StepError) -> Self {
        match error {
            StepError::Retry { after } => Self::Retry {
                after_ms: u64::try_from(after.as_millis()).unwrap_or(u64::MAX),
            },
            StepError::Failed(failure) => failure,
            StepError::Storage(error) => Self::Storage { error },
        }
    }
}

#[derive(Debug, Error)]
pub enum StartRefusal {
    #[error("request key has different content; existing operation {existing:?}")]
    RequestConflict { existing: OperationId },
    #[error("task kind is not registered")]
    UnknownKind,
    #[error(transparent)]
    Storage(#[from] StoreError),
}

impl From<StartRefusal> for StepError {
    fn from(error: StartRefusal) -> Self {
        match error {
            StartRefusal::Storage(error) => Self::Storage(error),
            StartRefusal::RequestConflict { existing } => Self::Failed(OperationFailure::Refused {
                code: "request_conflict".into(),
                detail: format!("existing operation {}", existing.0),
            }),
            StartRefusal::UnknownKind => Self::Failed(OperationFailure::Refused {
                code: "unknown_kind".into(),
                detail: "task kind is not registered".into(),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Admitted {
    New(OperationId),
    Existing(OperationId),
}

impl Admitted {
    pub fn id(&self) -> OperationId {
        match self {
            Self::New(id) | Self::Existing(id) => *id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum OperationState {
    Running {
        next_attempt_at: SyncTime,
        attempt: u32,
        drive: Option<DriveLease>,
    },
    Completed {
        result: Vec<u8>,
    },
    Failed {
        reason: OperationFailure,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationStatus {
    pub id: OperationId,
    pub kind: String,
    pub state: OperationState,
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum CallFailure {
    #[error("call was not sent: {reason}")]
    NotSent { reason: NotSentReason },
    #[error("call answer was lost")]
    AnswerLost,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", content = "answer", rename_all = "snake_case")]
pub enum CallResult<TAnswer> {
    Answered(TAnswer),
    NeverSent { last_seen: Option<SyncTime> },
}

/// A domain decision taken before a running operation replays its history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Resume<TOutput> {
    Continue,
    Finish(TOutput),
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum NotSentReason {
    #[error("receiver unreachable (last seen {last_seen:?})")]
    Unreachable { last_seen: Option<SyncTime> },
    #[error("receiver overloaded")]
    Overloaded,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DriveReport {
    pub completed: Vec<OperationId>,
    pub failed: Vec<(OperationId, OperationFailure)>,
    pub retried: Vec<OperationId>,
    pub yielded: Vec<OperationId>,
    pub wake_failure: Option<WakeFailure>,
}
