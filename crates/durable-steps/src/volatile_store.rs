//! Volatile conformance store. Reopening this handle never proves process durability.

use crate::{
    Admitted, AtomicStore, BoxFuture, DriveLease, Host, JournalScope, OperationId, RequestKey,
    StartRefusal, StoreError, SyncTime, Task,
    storage::{JournalStorage, OperationExecution, OperationOutcome, OperationRecord, StepRecord},
};
use futures_util::lock::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

#[derive(Clone, Default)]
struct MemoryState {
    operations: BTreeMap<OperationId, OperationRecord>,
    requests: BTreeMap<(String, RequestKey), OperationId>,
    steps: BTreeMap<(OperationId, u64), StepRecord>,
    business: BTreeMap<String, Vec<u8>>,
    wake: Option<SyncTime>,
}

pub struct VolatileStore<H: Host> {
    host: Arc<H>,
    state: Arc<Mutex<MemoryState>>,
}

impl<H: Host> Clone for VolatileStore<H> {
    fn clone(&self) -> Self {
        Self {
            host: Arc::clone(&self.host),
            state: Arc::clone(&self.state),
        }
    }
}

impl<H: Host> VolatileStore<H> {
    pub fn new(host: Arc<H>) -> Self {
        Self {
            host,
            state: Arc::new(Mutex::new(MemoryState::default())),
        }
    }

    /// Exposes the committed wake for adapter conformance, without advancing time.
    pub async fn committed_wake(&self) -> Option<SyncTime> {
        self.state.lock().await.wake
    }
}

pub struct VolatileScope<'t, H: Host> {
    host: &'t H,
    state: &'t mut MemoryState,
}

impl<H: Host> VolatileScope<'_, H> {
    pub fn read_business<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, StoreError> {
        self.state
            .business
            .get(key)
            .map(|bytes| {
                serde_json::from_slice(bytes).map_err(|error| StoreError::Corrupt {
                    field: key.into(),
                    message: error.to_string(),
                })
            })
            .transpose()
    }

    pub fn write_business<T: Serialize>(&mut self, key: &str, value: &T) -> Result<(), StoreError> {
        self.state.business.insert(
            key.into(),
            serde_json::to_vec(value).map_err(StoreError::backend)?,
        );
        Ok(())
    }
}

impl<H: Host> AtomicStore for VolatileStore<H> {
    type Scope<'t> = VolatileScope<'t, H>;

    async fn transaction<T, F>(&self, run: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: for<'s, 't> FnOnce(&'s mut Self::Scope<'t>) -> BoxFuture<'s, Result<T, StoreError>>
            + Send
            + 'static,
    {
        let mut committed = self.state.lock().await;
        // A deep, owned snapshot means an error or dropped future cannot leak writes.
        let mut staged = committed.clone();
        let mut scope = VolatileScope {
            host: &*self.host,
            state: &mut staged,
        };
        let result = run(&mut scope).await?;
        *committed = staged;
        Ok(result)
    }
}

impl<H: Host> JournalScope for VolatileScope<'_, H> {
    async fn admit<T: Task>(
        &mut self,
        request_key: &RequestKey,
        input: &T::Input,
    ) -> Result<Admitted, StartRefusal> {
        let canonical = crate::canonical_payload::encode(input).map_err(StoreError::backend)?;
        let digest = Sha256::digest(&canonical).to_vec();
        let lookup = (T::KIND.to_owned(), request_key.clone());
        if let Some(existing) = self.state.requests.get(&lookup) {
            let record = self
                .state
                .operations
                .get(existing)
                .ok_or(StoreError::MissingOperation { id: *existing })?;
            return if record.input_digest == digest {
                Ok(Admitted::Existing(*existing))
            } else {
                Err(StartRefusal::RequestConflict {
                    existing: *existing,
                })
            };
        }
        let id = OperationId::new(self.host.new_id()).map_err(StoreError::backend)?;
        if self.state.operations.contains_key(&id) {
            return Err(StoreError::Aborted {
                reason: "host generated an existing operation id".into(),
            }
            .into());
        }
        let now = self.host.now();
        self.state.operations.insert(
            id,
            OperationRecord {
                id,
                kind: T::KIND.into(),
                version: T::VERSION,
                request_key: request_key.clone(),
                input_digest: digest,
                execution: OperationExecution::Running {
                    input: canonical,
                    next_attempt_at: now,
                    attempt: 0,
                    drive: None,
                },
            },
        );
        self.state.requests.insert(lookup, id);
        self.ensure_wake(now).await?;
        Ok(Admitted::New(id))
    }

    async fn ensure_wake(&mut self, at: SyncTime) -> Result<(), StoreError> {
        self.state.wake = Some(self.state.wake.map_or(at, |current| current.min(at)));
        Ok(())
    }
}

impl<H: Host> JournalStorage for VolatileScope<'_, H> {
    async fn operation(&mut self, id: OperationId) -> Result<Option<OperationRecord>, StoreError> {
        Ok(self.state.operations.get(&id).cloned())
    }

    async fn due(&mut self, now: SyncTime) -> Result<Vec<OperationId>, StoreError> {
        Ok(self
            .state
            .operations
            .values()
            .filter_map(|record| match &record.execution {
                OperationExecution::Running {
                    next_attempt_at,
                    drive,
                    ..
                } if *next_attempt_at <= now
                    && drive.as_ref().is_none_or(|lease| lease.until <= now) =>
                {
                    Some(record.id)
                }
                _ => None,
            })
            .collect())
    }

    async fn claim(
        &mut self,
        id: OperationId,
        now: SyncTime,
        lease: DriveLease,
    ) -> Result<Option<OperationRecord>, StoreError> {
        let Some(record) = self.state.operations.get_mut(&id) else {
            return Ok(None);
        };
        match &mut record.execution {
            OperationExecution::Running {
                next_attempt_at,
                drive,
                ..
            } if *next_attempt_at <= now
                && drive.as_ref().is_none_or(|active| active.until <= now) =>
            {
                *drive = Some(lease);
                Ok(Some(record.clone()))
            }
            _ => Ok(None),
        }
    }

    async fn check_token(&mut self, id: OperationId, token: Uuid) -> Result<(), StoreError> {
        match self
            .state
            .operations
            .get(&id)
            .map(|record| &record.execution)
        {
            Some(OperationExecution::Running {
                drive: Some(lease), ..
            }) if lease.token == token => Ok(()),
            _ => Err(StoreError::StaleDriveToken { id }),
        }
    }

    async fn retry(
        &mut self,
        id: OperationId,
        token: Uuid,
        next: SyncTime,
        attempt: u32,
    ) -> Result<(), StoreError> {
        self.check_token(id, token).await?;
        let record = self
            .state
            .operations
            .get_mut(&id)
            .ok_or(StoreError::MissingOperation { id })?;
        match &mut record.execution {
            OperationExecution::Running {
                next_attempt_at,
                attempt: current,
                drive,
                ..
            } => {
                *next_attempt_at = next;
                *current = attempt;
                *drive = None;
                Ok(())
            }
            _ => Err(StoreError::StaleDriveToken { id }),
        }
    }

    async fn finish(
        &mut self,
        id: OperationId,
        token: Uuid,
        outcome: OperationOutcome,
        at: SyncTime,
        keep_result: bool,
    ) -> Result<(), StoreError> {
        self.check_token(id, token).await?;
        self.state
            .steps
            .retain(|(operation, _), _| *operation != id);
        if keep_result {
            let record = self
                .state
                .operations
                .get_mut(&id)
                .ok_or(StoreError::MissingOperation { id })?;
            record.execution = match outcome {
                OperationOutcome::Completed { result } => OperationExecution::Completed {
                    result,
                    finished_at: at,
                },
                OperationOutcome::Failed { reason } => OperationExecution::Failed {
                    reason,
                    finished_at: at,
                },
            };
        } else {
            let record = self
                .state
                .operations
                .remove(&id)
                .ok_or(StoreError::MissingOperation { id })?;
            self.state
                .requests
                .remove(&(record.kind, record.request_key));
        }
        Ok(())
    }

    async fn step(
        &mut self,
        id: OperationId,
        position: u64,
    ) -> Result<Option<StepRecord>, StoreError> {
        Ok(self.state.steps.get(&(id, position)).cloned())
    }

    async fn save_step(&mut self, step: StepRecord, token: Uuid) -> Result<(), StoreError> {
        self.check_token(step.operation, token).await?;
        if self.state.steps.values().any(|existing| {
            existing.operation == step.operation
                && existing.position != step.position
                && existing.name == step.name
        }) {
            return Err(StoreError::Aborted {
                reason: "step name reused at another position".into(),
            });
        }
        self.state
            .steps
            .insert((step.operation, step.position), step);
        Ok(())
    }

    async fn step_count(&mut self, id: OperationId) -> Result<u64, StoreError> {
        u64::try_from(
            self.state
                .steps
                .keys()
                .filter(|(operation, _)| *operation == id)
                .count(),
        )
        .map_err(StoreError::backend)
    }

    async fn earliest(&mut self) -> Result<Option<SyncTime>, StoreError> {
        Ok(self
            .state
            .operations
            .values()
            .filter_map(|record| match &record.execution {
                OperationExecution::Running {
                    next_attempt_at,
                    drive,
                    ..
                } => Some(drive.as_ref().map_or(*next_attempt_at, |lease| {
                    (*next_attempt_at).max(lease.until)
                })),
                _ => None,
            })
            .min())
    }
}
