//! Store-adapter contract used by the core, separate from task admission.
//!
//! Every method runs through the same borrowed transaction as application writes.
//! Task implementations use `JournalScope`; journal mutation is an adapter responsibility.

use crate::{
    CallReplay, CallRetryReason, DriveLease, OperationId, OperationState, OperationStatus,
    RequestKey, StepName, StoreError, SyncTime,
};
use serde::{Deserialize, Serialize};
use std::future::Future;
use uuid::Uuid;

/// The consumer's SQL executor inside the same transaction as its journal.
/// Real SQL-backed scopes implement this; the volatile store does not.
pub trait TransactionScope: Send {
    type Database: sqlx_core::database::Database;
    type Executor<'query>: sqlx_core::executor::Executor<'query, Database = Self::Database>
    where
        Self: 'query;

    fn executor(&mut self) -> Self::Executor<'_>;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationRecord {
    pub id: OperationId,
    pub kind: String,
    pub version: u32,
    pub request_key: RequestKey,
    pub input_digest: Vec<u8>,
    pub execution: OperationExecution,
}

/// Inputs exist only while running. Finished rows contain no input or steps.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum OperationExecution {
    Running {
        input: Vec<u8>,
        next_attempt_at: SyncTime,
        attempt: u32,
        drive: Option<DriveLease>,
    },
    Completed {
        result: Vec<u8>,
        finished_at: SyncTime,
    },
    Failed {
        reason: crate::OperationFailure,
        finished_at: SyncTime,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationOutcome {
    Completed { result: Vec<u8> },
    Failed { reason: crate::OperationFailure },
}

impl OperationRecord {
    pub fn status(&self) -> OperationStatus {
        let state = match &self.execution {
            OperationExecution::Running {
                next_attempt_at,
                attempt,
                drive,
                ..
            } => OperationState::Running {
                next_attempt_at: *next_attempt_at,
                attempt: *attempt,
                drive: drive.clone(),
            },
            OperationExecution::Completed { result, .. } => OperationState::Completed {
                result: result.clone(),
            },
            OperationExecution::Failed { reason, .. } => OperationState::Failed {
                reason: reason.clone(),
            },
        };
        OperationStatus {
            id: self.id,
            kind: self.kind.clone(),
            state,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum StepState {
    Local {
        output: Vec<u8>,
    },
    CallPending {
        intent: CallIntent,
        attempt: CallAttempt,
    },
    CallDone {
        intent: CallIntent,
        attempt: CallAttempt,
        output: Vec<u8>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CallIntent {
    pub target: String,
    pub request_digest: Vec<u8>,
    pub replay: CallReplay,
    pub unsent_for: Option<std::time::Duration>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CallAttempt {
    pub first_attempt_at: SyncTime,
    pub last_seen: Option<SyncTime>,
    pub reach: CallReach,
    #[serde(default)]
    pub retry_reason: Option<CallRetryReason>,
}

/// Dispatching is committed before sending. On recovery its unanswered attempt
/// may have reached the receiver, even if the host never returned AnswerLost.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallReach {
    NeverSent,
    /// A receiver replied, with no unresolved possible effect in this history.
    /// This is not NeverSent and cannot satisfy an unsent timeout.
    NoEffect,
    Dispatching,
    MayHaveReached,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StepRecord {
    pub operation: OperationId,
    pub position: u64,
    pub name: StepName,
    pub state: StepState,
}

pub trait JournalStorage: Send {
    fn operation(
        &mut self,
        id: OperationId,
    ) -> impl Future<Output = Result<Option<OperationRecord>, StoreError>> + Send;
    fn due(
        &mut self,
        now: SyncTime,
    ) -> impl Future<Output = Result<Vec<OperationId>, StoreError>> + Send;
    fn claim(
        &mut self,
        id: OperationId,
        now: SyncTime,
        lease: DriveLease,
    ) -> impl Future<Output = Result<Option<OperationRecord>, StoreError>> + Send;
    fn check_token(
        &mut self,
        id: OperationId,
        token: Uuid,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
    fn retry(
        &mut self,
        id: OperationId,
        token: Uuid,
        next_attempt_at: SyncTime,
        attempt: u32,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
    fn finish(
        &mut self,
        id: OperationId,
        token: Uuid,
        outcome: OperationOutcome,
        at: SyncTime,
        keep_result: bool,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
    fn step(
        &mut self,
        id: OperationId,
        position: u64,
    ) -> impl Future<Output = Result<Option<StepRecord>, StoreError>> + Send;
    fn save_step(
        &mut self,
        step: StepRecord,
        token: Uuid,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
    fn step_count(
        &mut self,
        id: OperationId,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;
    fn earliest(&mut self) -> impl Future<Output = Result<Option<SyncTime>, StoreError>> + Send;
}
