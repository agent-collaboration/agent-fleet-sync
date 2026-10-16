use crate::{
    AtomicStore, BoxFuture, DriveLease, DriveReport, Host, JournalScope, OperationFailure,
    OperationId, OperationState, OperationStatus, RequestKey, StartRefusal, StoreError, Task,
    operation_context::EngineContext,
    storage::{JournalStorage, OperationExecution, OperationOutcome},
};
use runtime_primitives::Backoff;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

trait RegisteredTask<S: AtomicStore, H: Host>: Send + Sync {
    fn version(&self) -> u32;
    fn keep_result(&self) -> bool;
    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        key: &'s RequestKey,
        failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>>;
    fn execute<'a>(
        &'a self,
        context: &'a mut EngineContext<'_, S, H>,
        input: Vec<u8>,
    ) -> BoxFuture<'a, Result<Vec<u8>, OperationFailure>>;
}

struct TaskRegistration<T>(T);

impl<S: AtomicStore, H: Host, T: Task<Store = S>> RegisteredTask<S, H> for TaskRegistration<T> {
    fn version(&self) -> u32 {
        T::VERSION
    }
    fn keep_result(&self) -> bool {
        T::KEEP_RESULT
    }
    fn on_failed<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        key: &'s RequestKey,
        failure: &'s OperationFailure,
    ) -> BoxFuture<'s, Result<(), StoreError>> {
        self.0.on_failed(scope, key, failure)
    }
    fn execute<'a>(
        &'a self,
        context: &'a mut EngineContext<'_, S, H>,
        input: Vec<u8>,
    ) -> BoxFuture<'a, Result<Vec<u8>, OperationFailure>> {
        Box::pin(async move {
            let input: T::Input = serde_json::from_slice(&input).map_err(|error| {
                OperationFailure::InvalidPayload {
                    message: error.to_string(),
                }
            })?;
            let output = self.0.run(context, &input).await?;
            serde_json::to_vec(&output).map_err(|error| OperationFailure::InvalidPayload {
                message: error.to_string(),
            })
        })
    }
}

pub struct Engine<S: AtomicStore, H: Host> {
    store: Arc<S>,
    host: Arc<H>,
    tasks: BTreeMap<&'static str, Arc<dyn RegisteredTask<S, H>>>,
    drive_lease: Duration,
    initial_backoff: Duration,
    maximum_backoff: Duration,
}

impl<S: AtomicStore, H: Host> Engine<S, H> {
    pub fn new(store: Arc<S>, host: Arc<H>) -> Self {
        Self {
            store,
            host,
            tasks: BTreeMap::new(),
            drive_lease: Duration::from_secs(30),
            initial_backoff: Duration::from_millis(10),
            maximum_backoff: Duration::from_secs(60),
        }
    }

    pub fn register<T: Task<Store = S>>(&mut self, task: T) -> Result<(), StoreError> {
        if T::KIND.is_empty() || self.tasks.contains_key(T::KIND) {
            return Err(StoreError::Aborted {
                reason: "task kind must be nonempty and registered once".into(),
            });
        }
        self.tasks.insert(T::KIND, Arc::new(TaskRegistration(task)));
        Ok(())
    }

    pub async fn start<T: Task<Store = S>>(
        &self,
        request_key: &RequestKey,
        input: T::Input,
    ) -> Result<OperationId, StartRefusal> {
        if !self.tasks.contains_key(T::KIND) {
            return Err(StartRefusal::UnknownKind);
        }
        let key = request_key.clone();
        let admitted = self
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    match scope.admit::<T>(&key, &input).await {
                        Err(StartRefusal::Storage(error)) => Err(error),
                        result => Ok(result),
                    }
                })
            })
            .await??;
        self.rearm().await?;
        self.drive_due().await?;
        Ok(admitted.id())
    }

    pub async fn status(&self, id: &OperationId) -> Result<OperationStatus, StoreError> {
        let id = *id;
        self.store
            .transaction(move |scope| {
                Box::pin(async move {
                    scope
                        .operation(id)
                        .await?
                        .map(|record| record.status())
                        .ok_or(StoreError::MissingOperation { id })
                })
            })
            .await
    }

    pub async fn drive_due(&self) -> Result<DriveReport, StoreError> {
        let now = self.host.now();
        let due = self
            .store
            .transaction(move |scope| Box::pin(async move { scope.due(now).await }))
            .await?;
        let mut report = DriveReport {
            wake_failure: self.rearm().await?,
            ..DriveReport::default()
        };
        for id in due {
            let now = self.host.now();
            let lease = DriveLease {
                token: self.host.new_id(),
                until: now.after(self.drive_lease),
            };
            let token = lease.token;
            let claimed = self
                .store
                .transaction(move |scope| {
                    Box::pin(async move {
                        let claimed = scope.claim(id, now, lease).await?;
                        if let Some(earliest) = scope.earliest().await? {
                            scope.ensure_wake(earliest).await?;
                        }
                        Ok(claimed)
                    })
                })
                .await?;
            report.wake_failure = self.rearm().await?.or(report.wake_failure);
            let Some(record) = claimed else {
                report.yielded.push(id);
                continue;
            };
            let mut context = EngineContext {
                store: &*self.store,
                host: Arc::clone(&self.host),
                operation: id,
                token,
                position: 0,
                failure: None,
                wake_failure: None,
            };
            let task = self.tasks.get(record.kind.as_str()).cloned();
            let (input, attempt) = match record.execution {
                OperationExecution::Running { input, attempt, .. } => (input, attempt),
                _ => {
                    return Err(StoreError::Corrupt {
                        field: "claim".into(),
                        message: "claimed a finished operation".into(),
                    });
                }
            };
            let execution = match &task {
                Some(task) if task.version() == record.version => {
                    task.execute(&mut context, input).await
                }
                Some(task) => Err(OperationFailure::VersionMismatch {
                    recorded: record.version,
                    registered: task.version(),
                }),
                None => Err(OperationFailure::Refused {
                    code: "unknown_kind".into(),
                    detail: record.kind,
                }),
            };
            let execution = match context.failure.take() {
                Some(failure) => Err(failure),
                None => execution,
            };
            let position = context.position;
            report.wake_failure = context.wake_failure.or(report.wake_failure);
            let mut state = match execution {
                Ok(result) => OperationState::Completed { result },
                Err(OperationFailure::Retry { after_ms }) => OperationState::Running {
                    next_attempt_at: self
                        .host
                        .now()
                        .after(self.retry_after(attempt, Duration::from_millis(after_ms))),
                    attempt: attempt.saturating_add(1),
                    drive: None,
                },
                Err(OperationFailure::Storage {
                    error: StoreError::StaleDriveToken { .. },
                }) => {
                    report.yielded.push(id);
                    continue;
                }
                Err(OperationFailure::Storage { .. }) => OperationState::Running {
                    next_attempt_at: self
                        .host
                        .now()
                        .after(self.retry_after(attempt, Duration::ZERO)),
                    attempt: attempt.saturating_add(1),
                    drive: None,
                },
                Err(reason) => OperationState::Failed { reason },
            };
            let finished_at = self.host.now();
            let finished = self
                .store
                .transaction(move |scope| {
                    Box::pin(async move {
                        scope.check_token(id, token).await?;
                        if matches!(state, OperationState::Completed { .. })
                            && scope.step_count(id).await? != position
                        {
                            state = OperationState::Failed {
                                reason: OperationFailure::Nondeterministic {
                                    position,
                                    expected: "another recorded step".into(),
                                    found: "end of task".into(),
                                },
                            };
                        }
                        match &state {
                            OperationState::Running {
                                next_attempt_at,
                                attempt,
                                ..
                            } => scope.retry(id, token, *next_attempt_at, *attempt).await?,
                            OperationState::Completed { result } => {
                                scope
                                    .finish(
                                        id,
                                        token,
                                        OperationOutcome::Completed {
                                            result: result.clone(),
                                        },
                                        finished_at,
                                        task.as_ref().is_none_or(|task| task.keep_result()),
                                    )
                                    .await?
                            }
                            OperationState::Failed { reason } => {
                                if let Some(task) = &task {
                                    task.on_failed(scope, &record.request_key, reason).await?;
                                }
                                scope
                                    .finish(
                                        id,
                                        token,
                                        OperationOutcome::Failed {
                                            reason: reason.clone(),
                                        },
                                        finished_at,
                                        task.as_ref().is_none_or(|task| task.keep_result()),
                                    )
                                    .await?;
                            }
                        }
                        if let Some(earliest) = scope.earliest().await? {
                            scope.ensure_wake(earliest).await?;
                        }
                        Ok(state)
                    })
                })
                .await;
            report.wake_failure = self.rearm().await?.or(report.wake_failure);
            match finished {
                Err(StoreError::StaleDriveToken { .. }) => report.yielded.push(id),
                Err(error) => return Err(error),
                Ok(OperationState::Completed { .. }) => report.completed.push(id),
                Ok(OperationState::Failed { reason }) => report.failed.push((id, reason)),
                Ok(OperationState::Running { .. }) => report.retried.push(id),
            }
        }
        Ok(report)
    }

    async fn rearm(&self) -> Result<Option<crate::WakeFailure>, StoreError> {
        let earliest = self
            .store
            .transaction(|scope| Box::pin(async move { scope.earliest().await }))
            .await?;
        Ok(match earliest {
            Some(at) => self.host.wake_at(at).await.err(),
            None => None,
        })
    }

    fn retry_after(&self, attempt: u32, requested: Duration) -> Duration {
        Backoff {
            initial: self.initial_backoff,
            max: self.maximum_backoff,
            factor: 2,
        }
        .delay(attempt)
        .max(requested)
        .min(self.maximum_backoff)
    }
}
