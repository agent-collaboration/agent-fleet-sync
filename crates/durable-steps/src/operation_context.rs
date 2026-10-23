use crate::{
    AtomicStore, CallResult, CallTarget, Host, JournalScope, OperationContext, OperationFailure,
    OperationId, StepError, StepName, StoreError,
    storage::{JournalStorage, StepRecord, StepState},
};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) struct EngineContext<'engine, S: AtomicStore, H: Host> {
    pub store: &'engine S,
    pub host: Arc<H>,
    pub operation: OperationId,
    pub token: Uuid,
    pub position: u64,
    pub failure: Option<OperationFailure>,
    pub wake_failure: Option<crate::WakeFailure>,
}

fn rollback(error: StepError) -> StoreError {
    StoreError::StepRollback {
        failure: Box::new(error.into()),
    }
}

pub(crate) fn step_error(error: StoreError) -> StepError {
    match error {
        StoreError::StepRollback { failure } => match *failure {
            OperationFailure::Retry { after_ms } => StepError::Retry {
                after: std::time::Duration::from_millis(after_ms),
            },
            OperationFailure::Storage { error } => StepError::Storage(error),
            failure => StepError::Failed(failure),
        },
        error => StepError::Storage(error),
    }
}

pub(crate) fn invalid_payload(error: impl std::fmt::Display) -> StoreError {
    rollback(StepError::Failed(OperationFailure::InvalidPayload {
        message: error.to_string(),
    }))
}

pub(crate) fn changed_step(
    position: u64,
    expected: impl Into<String>,
    found: impl Into<String>,
) -> StoreError {
    rollback(StepError::Failed(OperationFailure::Nondeterministic {
        position,
        expected: expected.into(),
        found: found.into(),
    }))
}

pub(crate) async fn validate_name<S: JournalStorage>(
    scope: &mut S,
    id: OperationId,
    position: u64,
    name: &StepName,
) -> Result<(), StoreError> {
    if let Some(saved) = scope.step(id, position).await? {
        if saved.name != *name {
            return Err(changed_step(position, saved.name.as_str(), name.as_str()));
        }
    } else {
        let count = scope.step_count(id).await?;
        for previous in 0..count {
            if let Some(saved) = scope.step(id, previous).await?
                && saved.name == *name
            {
                return Err(changed_step(
                    position,
                    "a distinct step name",
                    name.as_str(),
                ));
            }
        }
    }
    Ok(())
}

impl<S: AtomicStore, H: Host> OperationContext for EngineContext<'_, S, H> {
    type Store = S;
    fn id(&self) -> &OperationId {
        &self.operation
    }
    fn now(&self) -> crate::SyncTime {
        self.host.now()
    }

    async fn local<T, F>(&mut self, name: StepName, run: F) -> Result<T, StepError>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: for<'s, 't> FnOnce(&'s mut S::Scope<'t>) -> crate::BoxFuture<'s, Result<T, StepError>>
            + Send
            + 'static,
    {
        let id = self.operation;
        let token = self.token;
        let position = self.position;
        let result = self
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    scope.check_token(id, token).await?;
                    validate_name(scope, id, position, &name).await?;
                    if let Some(saved) = scope.step(id, position).await? {
                        return match saved.state {
                            StepState::Local { output } => {
                                serde_json::from_slice(&output).map_err(invalid_payload)
                            }
                            _ => Err(changed_step(position, "call", "local")),
                        };
                    }
                    let output = run(scope).await.map_err(rollback)?;
                    let bytes = serde_json::to_vec(&output).map_err(invalid_payload)?;
                    scope
                        .save_step(
                            StepRecord {
                                operation: id,
                                position,
                                name,
                                state: StepState::Local { output: bytes },
                            },
                            token,
                        )
                        .await?;
                    if let Some(earliest) = scope.earliest().await? {
                        scope.ensure_wake(earliest).await?;
                    }
                    Ok(output)
                })
            })
            .await
            .map_err(step_error);
        let result = match result {
            Ok(output) => self.rearm().await.map(|()| output),
            Err(error) => Err(error),
        };
        self.record_result(&result);
        result
    }

    async fn call<C: CallTarget>(
        &mut self,
        name: StepName,
        target: C,
        request: C::Request,
    ) -> Result<C::Answer, StepError> {
        let result = self
            .execute_call(name, target, request, None)
            .await
            .and_then(|result| match result {
                CallResult::Answered(answer) => Ok(answer),
                CallResult::NeverSent { .. } => Err(StepError::Storage(StoreError::Corrupt {
                    field: "call_result".into(),
                    message: "ordinary call gave up".into(),
                })),
            });
        self.record_result(&result);
        result
    }
    async fn call_or_give_up<C: CallTarget>(
        &mut self,
        name: StepName,
        target: C,
        request: C::Request,
        unsent_for: std::time::Duration,
    ) -> Result<CallResult<C::Answer>, StepError> {
        let result = self
            .execute_call(name, target, request, Some(unsent_for))
            .await;
        self.record_result(&result);
        result
    }
}

impl<S: AtomicStore, H: Host> EngineContext<'_, S, H> {
    async fn rearm(&mut self) -> Result<(), StepError> {
        let earliest = self
            .store
            .transaction(|scope| Box::pin(async move { scope.earliest().await }))
            .await?;
        if let Some(at) = earliest {
            let requested = match crate::operation_engine::future_wake_deadline(at, self.host.now())
            {
                Ok(at) => self.host.wake_at(at).await,
                Err(error) => Err(error),
            };
            self.wake_failure = requested.err().or(self.wake_failure.take());
        }
        Ok(())
    }
    fn record_result<T>(&mut self, result: &Result<T, StepError>) {
        match result {
            Ok(_) => self.position += 1,
            Err(error) => self.failure = Some(error.clone().into()),
        }
    }
}
