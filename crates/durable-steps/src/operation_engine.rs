use crate::{
    AtomicStore, BoxFuture, DriveLease, DriveReport, Host, JournalScope, OperationFailure,
    OperationId, OperationState, OperationStatus, RequestKey, StartRefusal, StoreError, Task,
    operation_context::EngineContext, storage::JournalStorage,
};
use runtime_primitives::Backoff;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

trait RegisteredTask<S: AtomicStore, H: Host>: Send + Sync {
    fn version(&self) -> u32;
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
    tasks: BTreeMap<&'static str, Box<dyn RegisteredTask<S, H>>>,
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
        self.tasks.insert(T::KIND, Box::new(TaskRegistration(task)));
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
                        .map(|record| record.status)
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
        let mut report = DriveReport::default();
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
            };
            let execution = match self.tasks.get(record.status.kind.as_str()) {
                Some(task) if task.version() == record.version => {
                    task.execute(&mut context, record.input).await
                }
                Some(task) => Err(OperationFailure::VersionMismatch {
                    recorded: record.version,
                    registered: task.version(),
                }),
                None => Err(OperationFailure::Refused {
                    code: "unknown_kind".into(),
                    detail: record.status.kind,
                }),
            };
            let execution = match context.failure.take() {
                Some(failure) => Err(failure),
                None => execution,
            };
            let position = context.position;
            let attempt = match record.status.state {
                OperationState::Running { attempt, .. } => attempt,
                _ => 0,
            };
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
                        scope.set_state(id, token, state.clone()).await?;
                        if let Some(earliest) = scope.earliest().await? {
                            scope.ensure_wake(earliest).await?;
                        }
                        Ok(state)
                    })
                })
                .await;
            match finished {
                Err(StoreError::StaleDriveToken { .. }) => report.yielded.push(id),
                Err(error) => return Err(error),
                Ok(OperationState::Completed { .. }) => report.completed.push(id),
                Ok(OperationState::Failed { reason }) => report.failed.push((id, reason)),
                Ok(OperationState::Running { .. }) => report.retried.push(id),
            }
        }
        let earliest = self
            .store
            .transaction(|scope| Box::pin(async move { scope.earliest().await }))
            .await?;
        if let Some(earliest) = earliest {
            report.wake_failure = self.host.wake_at(earliest).await.err();
        }
        Ok(report)
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
