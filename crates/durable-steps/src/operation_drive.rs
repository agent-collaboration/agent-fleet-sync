use super::*;
use futures_util::future::select_all;

impl<S: AtomicStore, H: Host> Engine<S, H> {
    pub(super) async fn drive_operation(&self, id: OperationId) -> Result<DriveReport, StoreError> {
        let mut report = DriveReport::default();
        let activity = self.operation_drives.attach(id).await;
        let Some(mut active_drive) = activity.try_lock() else {
            report.yielded.push(id);
            return Ok(report);
        };
        // A competing drive yields before waiting for unrelated capacity.
        // Capacity is shared by every drive of this Engine. A permit is not the
        // storage owner: each journal transaction acquires/releases its own lock.
        let (_capacity, _, _) =
            select_all(self.drive_capacity.iter().map(|slot| Box::pin(slot.lock()))).await;
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
            return Ok(report);
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
        let kind = record.kind.clone();
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
                let resumed = self
                    .before_resume(Arc::clone(task), id, token, record.request_key.clone())
                    .await;
                if let Ok(Resume::Finish(result)) = &resumed {
                    *active_drive = Some(OperationReceipt::new(
                        kind.clone(),
                        OperationOutcome::Completed {
                            result: result.clone(),
                        },
                    ));
                }
                report.wake_failure = self.rearm().await?.or(report.wake_failure);
                match resumed {
                    Ok(Resume::Continue) => task.execute(&mut context, input).await,
                    Ok(Resume::Finish(_)) => {
                        report.completed.push(id);
                        return Ok(report);
                    }
                    Err(StoreError::StepRollback { failure }) => Err(*failure),
                    Err(error) => Err(OperationFailure::Storage { error }),
                }
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
                return Ok(report);
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
        // Publish only a committed terminal outcome, before any later await.
        // The driving caller still observes a post-commit rearm storage error.
        let outcome = match &finished {
            Ok(OperationState::Completed { result }) => Some(OperationOutcome::Completed {
                result: result.clone(),
            }),
            Ok(OperationState::Failed { reason }) => Some(OperationOutcome::Failed {
                reason: reason.clone(),
            }),
            _ => None,
        };
        if let Some(outcome) = outcome {
            *active_drive = Some(OperationReceipt::new(kind, outcome));
        }
        report.wake_failure = self.rearm().await?.or(report.wake_failure);
        match finished {
            Err(StoreError::StaleDriveToken { .. }) => report.yielded.push(id),
            Err(error) => return Err(error),
            Ok(OperationState::Completed { .. }) => report.completed.push(id),
            Ok(OperationState::Failed { reason }) => report.failed.push((id, reason)),
            Ok(OperationState::Running { .. }) => report.retried.push(id),
        }
        Ok(report)
    }
}
