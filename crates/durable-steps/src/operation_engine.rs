use crate::{
    AtomicStore, BoxFuture, DriveLease, DriveReport, Host, JournalScope, OperationFailure,
    OperationId, OperationState, OperationStatus, RequestKey, Resume, SettleError, StartRefusal,
    StoreError, Task,
    operation_context::EngineContext,
    storage::{JournalStorage, OperationExecution, OperationOutcome},
};
use futures_util::{StreamExt, lock::Mutex};
use runtime_primitives::Backoff;

#[path = "operation_drive.rs"]
mod operation_drive;
#[path = "operation_settlement.rs"]
mod operation_settlement;

// Eight slots leave seven peers able to progress beside one held call, while
// bounding suspended task contexts, encoded inputs and host dispatches.
// The permits are shared across overlapping drive_due/start calls on this Engine.
const MAX_CONCURRENT_OPERATIONS: usize = 8;
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Weak},
    time::Duration,
};

trait RegisteredTask<S: AtomicStore, H: Host>: Send + Sync {
    fn version(&self) -> u32;
    fn keep_result(&self) -> bool;
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<Vec<u8>>, StoreError>>;
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
    fn before_resume<'s, 't>(
        &'s self,
        scope: &'s mut S::Scope<'t>,
        key: &'s RequestKey,
    ) -> BoxFuture<'s, Result<Resume<Vec<u8>>, StoreError>> {
        Box::pin(async move {
            match self.0.before_resume(scope, key).await? {
                Resume::Continue => Ok(Resume::Continue),
                Resume::Finish(output) => {
                    serde_json::to_vec(&output)
                        .map(Resume::Finish)
                        .map_err(|error| StoreError::StepRollback {
                            failure: Box::new(OperationFailure::InvalidPayload {
                                message: error.to_string(),
                            }),
                        })
                }
            }
        })
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
    drive_capacity: [Arc<Mutex<()>>; MAX_CONCURRENT_OPERATIONS],
    operation_drives: Mutex<BTreeMap<OperationId, Weak<Mutex<()>>>>,
}

/// A test-only hold on existing execution capacity; dropping it permits claims.
#[cfg(feature = "test-support")]
pub struct DriveHold {
    _permits: Vec<futures_util::lock::OwnedMutexGuard<()>>,
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
            drive_capacity: std::array::from_fn(|_| Arc::new(Mutex::new(()))),
            operation_drives: Mutex::new(BTreeMap::new()),
        }
    }

    /// Hold every execution slot before journal claims, while domain writes remain live.
    #[cfg(feature = "test-support")]
    pub async fn hold_drives_for_test(&self) -> DriveHold {
        let mut permits = Vec::with_capacity(MAX_CONCURRENT_OPERATIONS);
        for slot in &self.drive_capacity {
            permits.push(Arc::clone(slot).lock_owned().await);
        }
        DriveHold { _permits: permits }
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

    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send makes higher-ranked store tasks spawnable; engine_send pins this contract"
    )]
    pub fn start<T: Task<Store = S>>(
        &self,
        request_key: &RequestKey,
        input: T::Input,
    ) -> impl Future<Output = Result<OperationId, StartRefusal>> + Send {
        async move {
            let operation = self.admit::<T>(request_key, input).await?;
            self.drive_due().await?;
            Ok(operation)
        }
    }

    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send makes higher-ranked store tasks spawnable; engine_send pins this contract"
    )]
    pub fn status(
        &self,
        id: &OperationId,
    ) -> impl Future<Output = Result<OperationStatus, StoreError>> + Send {
        async move {
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
    }

    #[expect(
        clippy::manual_async_fn,
        reason = "explicit Send makes higher-ranked store tasks spawnable; engine_send pins this contract"
    )]
    pub fn drive_due(&self) -> impl Future<Output = Result<DriveReport, StoreError>> + Send {
        async move {
            let now = self.host.now();
            let due = self
                .store
                .transaction(move |scope| Box::pin(async move { scope.due(now).await }))
                .await?;
            let mut report = DriveReport {
                wake_failure: self.rearm().await?,
                ..DriveReport::default()
            };
            // Execute independently, then fold reports in the original due order.
            // Collect before propagating errors so another in-flight call is not
            // abandoned merely because a sibling's storage transaction failed.
            let mut outcomes = futures_util::stream::iter(due.into_iter().enumerate())
                .map(|(index, id)| async move { (index, self.drive_operation(id).await) })
                .buffer_unordered(MAX_CONCURRENT_OPERATIONS)
                .collect::<Vec<_>>()
                .await;
            outcomes.sort_by_key(|(index, _)| *index);
            for (_, outcome) in outcomes {
                let operation = outcome?;
                report.completed.extend(operation.completed);
                report.failed.extend(operation.failed);
                report.retried.extend(operation.retried);
                report.yielded.extend(operation.yielded);
                report.wake_failure = operation.wake_failure.or(report.wake_failure);
            }
            Ok(report)
        }
    }

    async fn before_resume(
        &self,
        task: Arc<dyn RegisteredTask<S, H>>,
        id: OperationId,
        token: uuid::Uuid,
        key: RequestKey,
    ) -> Result<Resume<()>, StoreError> {
        let finished_at = self.host.now();
        self.store
            .transaction(move |scope| {
                Box::pin(async move {
                    scope.check_token(id, token).await?;
                    let resumed = match task.before_resume(scope, &key).await? {
                        Resume::Continue => Resume::Continue,
                        Resume::Finish(result) => {
                            scope
                                .finish(
                                    id,
                                    token,
                                    OperationOutcome::Completed { result },
                                    finished_at,
                                    task.keep_result(),
                                )
                                .await?;
                            Resume::Finish(())
                        }
                    };
                    if let Some(earliest) = scope.earliest().await? {
                        scope.ensure_wake(earliest).await?;
                    }
                    Ok(resumed)
                })
            })
            .await
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
