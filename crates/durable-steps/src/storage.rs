//! Store-adapter contract used by the core, separate from task admission.
//!
//! Every method runs through the same borrowed transaction as application writes.
//! Task implementations use `JournalScope`; journal mutation is an adapter responsibility.

use crate::{
    CallReplay, DriveLease, OperationId, OperationState, OperationStatus, RequestKey, StepName,
    StoreError, SyncTime,
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
    pub status: OperationStatus,
    pub version: u32,
    pub request_key: RequestKey,
    pub input: Vec<u8>,
    pub input_digest: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum StepState {
    Local {
        output: Vec<u8>,
    },
    CallPending {
        target: String,
        request_digest: Vec<u8>,
        replay: CallReplay,
    },
    CallDone {
        target: String,
        request_digest: Vec<u8>,
        replay: CallReplay,
        output: Vec<u8>,
    },
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
    fn set_state(
        &mut self,
        id: OperationId,
        token: Uuid,
        state: OperationState,
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
