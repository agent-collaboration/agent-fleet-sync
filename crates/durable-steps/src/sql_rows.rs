use crate::{
    DriveLease, OperationId, RequestKey, StepName, StoreError, SyncTime,
    storage::{
        CallAttempt, CallIntent, CallReach, OperationExecution, OperationRecord, StepRecord,
        StepState,
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub(crate) fn corrupt(field: &str, error: impl std::fmt::Display) -> StoreError {
    StoreError::Corrupt {
        field: field.into(),
        message: error.to_string(),
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct StoredOperationRow {
    #[sqlx(rename = "operation_id!")]
    pub operation_id: String,
    #[sqlx(rename = "kind!")]
    pub kind: String,
    #[sqlx(rename = "kind_version!: i64")]
    pub kind_version: i64,
    #[sqlx(rename = "request_key!")]
    pub request_key: String,
    pub input: Option<Vec<u8>>,
    #[sqlx(rename = "input_digest!")]
    pub input_digest: Vec<u8>,
    #[sqlx(rename = "state!")]
    pub state: String,
    pub result: Option<Vec<u8>>,
    pub failure: Option<String>,
    #[sqlx(rename = "finished_at?: i64")]
    pub finished_at: Option<i64>,
    #[sqlx(rename = "next_attempt_at?: i64")]
    pub next_attempt_at: Option<i64>,
    #[sqlx(rename = "attempt!: i64")]
    pub attempt: i64,
    pub drive_token: Option<String>,
    #[sqlx(rename = "drive_until?: i64")]
    pub drive_until: Option<i64>,
}
impl TryFrom<StoredOperationRow> for OperationRecord {
    type Error = StoreError;
    fn try_from(row: StoredOperationRow) -> Result<Self, StoreError> {
        let id = OperationId::try_from(row.operation_id)
            .map_err(|error| corrupt("operation_id", error))?;
        if row.kind.is_empty() || row.input_digest.len() != 32 {
            return Err(corrupt("operation", "invalid kind or digest"));
        }
        let execution = match row.state.as_str() {
            "running"
                if row.result.is_none() && row.failure.is_none() && row.finished_at.is_none() =>
            {
                let input = row
                    .input
                    .ok_or_else(|| corrupt("input", "missing running input"))?;
                if Sha256::digest(&input).to_vec() != row.input_digest {
                    return Err(corrupt(
                        "input_digest",
                        "input differs from its admission digest",
                    ));
                }
                let drive = match (row.drive_token, row.drive_until) {
                    (None, None) => None,
                    (Some(token), Some(until)) => Some(DriveLease {
                        token: Uuid::parse_str(&token).map_err(|e| corrupt("drive_token", e))?,
                        until: SyncTime::from_unix_millis(until),
                    }),
                    _ => {
                        return Err(corrupt(
                            "drive",
                            "token and deadline must be present together",
                        ));
                    }
                };
                OperationExecution::Running {
                    input,
                    next_attempt_at: SyncTime::from_unix_millis(
                        row.next_attempt_at
                            .ok_or_else(|| corrupt("next_attempt_at", "missing deadline"))?,
                    ),
                    attempt: u32::try_from(row.attempt).map_err(|e| corrupt("attempt", e))?,
                    drive,
                }
            }
            "completed"
                if row.input.is_none()
                    && row.failure.is_none()
                    && row.next_attempt_at.is_none()
                    && row.drive_token.is_none()
                    && row.drive_until.is_none() =>
            {
                OperationExecution::Completed {
                    result: row
                        .result
                        .ok_or_else(|| corrupt("result", "missing completed result"))?,
                    finished_at: SyncTime::from_unix_millis(
                        row.finished_at
                            .ok_or_else(|| corrupt("finished_at", "missing terminal time"))?,
                    ),
                }
            }
            "failed"
                if row.input.is_none()
                    && row.result.is_none()
                    && row.next_attempt_at.is_none()
                    && row.drive_token.is_none()
                    && row.drive_until.is_none() =>
            {
                OperationExecution::Failed {
                    reason: serde_json::from_str(
                        &row.failure
                            .ok_or_else(|| corrupt("failure", "missing typed failure"))?,
                    )
                    .map_err(|e| corrupt("failure", e))?,
                    finished_at: SyncTime::from_unix_millis(
                        row.finished_at
                            .ok_or_else(|| corrupt("finished_at", "missing terminal time"))?,
                    ),
                }
            }
            _ => return Err(corrupt("state", "unknown or inconsistent operation state")),
        };
        Ok(Self {
            id,
            kind: row.kind,
            version: u32::try_from(row.kind_version).map_err(|e| corrupt("kind_version", e))?,
            request_key: RequestKey::new(row.request_key).map_err(|e| corrupt("request_key", e))?,
            input_digest: row.input_digest,
            execution,
        })
    }
}
impl StoredOperationRow {
    pub fn encode(record: OperationRecord) -> Result<Self, StoreError> {
        let mut row = Self {
            operation_id: record.id.as_uuid().to_string(),
            kind: record.kind,
            kind_version: i64::from(record.version),
            request_key: record.request_key.into(),
            input: None,
            input_digest: record.input_digest,
            state: String::new(),
            result: None,
            failure: None,
            finished_at: None,
            next_attempt_at: None,
            attempt: 0,
            drive_token: None,
            drive_until: None,
        };
        match record.execution {
            OperationExecution::Running {
                input,
                next_attempt_at,
                attempt,
                drive,
            } => {
                row.state = "running".into();
                row.input = Some(input);
                row.next_attempt_at = Some(next_attempt_at.unix_millis());
                row.attempt = i64::from(attempt);
                if let Some(lease) = drive {
                    row.drive_token = Some(lease.token.to_string());
                    row.drive_until = Some(lease.until.unix_millis());
                }
            }
            OperationExecution::Completed {
                result,
                finished_at,
            } => {
                row.state = "completed".into();
                row.result = Some(result);
                row.finished_at = Some(finished_at.unix_millis());
            }
            OperationExecution::Failed {
                reason,
                finished_at,
            } => {
                row.state = "failed".into();
                row.failure = Some(serde_json::to_string(&reason).map_err(StoreError::backend)?);
                row.finished_at = Some(finished_at.unix_millis());
            }
        }
        Ok(row)
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct StoredStepRow {
    #[sqlx(rename = "operation_id!")]
    pub operation_id: String,
    #[sqlx(rename = "position!: i64")]
    pub position: i64,
    #[sqlx(rename = "step_name!")]
    pub step_name: String,
    #[sqlx(rename = "kind!")]
    pub kind: String,
    pub intent: Option<String>,
    pub output: Option<Vec<u8>>,
    #[sqlx(rename = "first_attempt_at?: i64")]
    pub first_attempt_at: Option<i64>,
    #[sqlx(rename = "may_have_reached?: i64")]
    pub may_have_reached: Option<i64>,
}
#[derive(Serialize, Deserialize)]
struct StoredCallIntent {
    intent: CallIntent,
    reach: CallReach,
    last_seen: Option<SyncTime>,
}
impl StoredStepRow {
    pub fn encode(step: StepRecord) -> Result<Self, StoreError> {
        let (kind, call, output) = match step.state {
            StepState::Local { output } => ("local", None, Some(output)),
            StepState::CallPending { intent, attempt } => {
                ("call_pending", Some((intent, attempt)), None)
            }
            StepState::CallDone {
                intent,
                attempt,
                output,
            } => ("call_done", Some((intent, attempt)), Some(output)),
        };
        let (intent, first_attempt_at, may_have_reached) = match call {
            None => (None, None, None),
            Some((intent, attempt)) => (
                Some(
                    serde_json::to_string(&StoredCallIntent {
                        intent,
                        reach: attempt.reach,
                        last_seen: attempt.last_seen,
                    })
                    .map_err(StoreError::backend)?,
                ),
                Some(attempt.first_attempt_at.unix_millis()),
                Some(i64::from(attempt.reach != CallReach::NeverSent)),
            ),
        };
        Ok(Self {
            operation_id: step.operation.as_uuid().to_string(),
            position: i64::try_from(step.position).map_err(StoreError::backend)?,
            step_name: step.name.into(),
            kind: kind.into(),
            intent,
            output,
            first_attempt_at,
            may_have_reached,
        })
    }
}
impl TryFrom<StoredStepRow> for StepRecord {
    type Error = StoreError;
    fn try_from(row: StoredStepRow) -> Result<Self, StoreError> {
        let state = match (
            row.kind.as_str(),
            row.intent,
            row.output,
            row.first_attempt_at,
            row.may_have_reached,
        ) {
            ("local", None, Some(output), None, None) => StepState::Local { output },
            (
                kind @ ("call_pending" | "call_done"),
                Some(intent),
                output,
                Some(first),
                Some(may),
            ) => {
                let stored: StoredCallIntent =
                    serde_json::from_str(&intent).map_err(|e| corrupt("call_intent", e))?;
                if may != i64::from(stored.reach != CallReach::NeverSent) {
                    return Err(corrupt("may_have_reached", "inconsistent call attempt"));
                }
                let attempt = CallAttempt {
                    first_attempt_at: SyncTime::from_unix_millis(first),
                    last_seen: stored.last_seen,
                    reach: stored.reach,
                };
                match (kind, output) {
                    ("call_pending", None) => StepState::CallPending {
                        intent: stored.intent,
                        attempt,
                    },
                    ("call_done", Some(output)) => StepState::CallDone {
                        intent: stored.intent,
                        attempt,
                        output,
                    },
                    _ => return Err(corrupt("step_output", "inconsistent call output")),
                }
            }
            _ => return Err(corrupt("step_state", "unknown or inconsistent step state")),
        };
        Ok(Self {
            operation: OperationId::try_from(row.operation_id)
                .map_err(|error| corrupt("operation_id", error))?,
            position: u64::try_from(row.position).map_err(|e| corrupt("position", e))?,
            name: StepName::new(row.step_name).map_err(|e| corrupt("step_name", e))?,
            state,
        })
    }
}
#[derive(sqlx::FromRow)]
pub(crate) struct StoredIdRow {
    #[sqlx(rename = "operation_id!")]
    pub operation_id: String,
}
#[derive(sqlx::FromRow)]
pub(crate) struct StoredTimeRow {
    #[sqlx(rename = "at?: i64")]
    pub at: Option<i64>,
}
#[derive(sqlx::FromRow)]
pub(crate) struct StoredCountRow {
    #[sqlx(rename = "count!: i64")]
    pub count: i64,
}
