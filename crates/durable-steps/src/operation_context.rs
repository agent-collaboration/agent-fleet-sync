use crate::{
    AtomicStore, CallFailure, CallId, CallReplay, CallTarget, Host, JournalScope, OperationContext,
    OperationFailure, OperationId, StepError, StepName, StoreError,
    storage::{JournalStorage, StepRecord, StepState},
};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) struct EngineContext<'engine, S: AtomicStore, H: Host> {
    pub store: &'engine S,
    pub host: Arc<H>,
    pub operation: OperationId,
    pub token: Uuid,
    pub position: u64,
    pub failure: Option<OperationFailure>,
}

fn rollback(error: StepError) -> StoreError {
    StoreError::StepRollback {
        failure: Box::new(error.into()),
    }
}

fn step_error(error: StoreError) -> StepError {
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

fn invalid_payload(error: impl std::fmt::Display) -> StoreError {
    rollback(StepError::Failed(OperationFailure::InvalidPayload {
        message: error.to_string(),
    }))
}

fn changed_step(
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

async fn validate_name<S: JournalStorage>(
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

enum CallPreparation {
    Replay(Vec<u8>),
    Dispatch,
    Uncertain,
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
        self.record_result(&result);
        result
    }

    async fn call<C: CallTarget>(
        &mut self,
        name: StepName,
        target: C,
        request: C::Request,
    ) -> Result<C::Answer, StepError> {
        let id = self.operation;
        let token = self.token;
        let position = self.position;
        let address = format!("{}:{}", C::KIND, target.address());
        let digest =
            Sha256::digest(crate::canonical_payload::encode(&request).map_err(|error| {
                StepError::Failed(OperationFailure::InvalidPayload {
                    message: error.to_string(),
                })
            })?)
            .to_vec();
        let step_name = name.clone();
        let expected_address = address.clone();
        let expected_digest = digest.clone();
        let preparation = self
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    scope.check_token(id, token).await?;
                    validate_name(scope, id, position, &step_name).await?;
                    match scope.step(id, position).await? {
                        Some(saved) => match saved.state {
                            StepState::CallPending {
                                target,
                                request_digest,
                                replay,
                            } if target == expected_address
                                && request_digest == expected_digest
                                && replay == C::REPLAY =>
                            {
                                Ok(if replay == CallReplay::NeverRepeat {
                                    CallPreparation::Uncertain
                                } else {
                                    CallPreparation::Dispatch
                                })
                            }
                            StepState::CallDone {
                                target,
                                request_digest,
                                replay,
                                output,
                            } if target == expected_address
                                && request_digest == expected_digest
                                && replay == C::REPLAY =>
                            {
                                Ok(CallPreparation::Replay(output))
                            }
                            _ => Err(changed_step(
                                position,
                                "recorded call identity/content",
                                "changed call identity/content",
                            )),
                        },
                        None => {
                            scope
                                .save_step(
                                    StepRecord {
                                        operation: id,
                                        position,
                                        name: step_name,
                                        state: StepState::CallPending {
                                            target: expected_address,
                                            request_digest: expected_digest,
                                            replay: C::REPLAY,
                                        },
                                    },
                                    token,
                                )
                                .await?;
                            Ok(CallPreparation::Dispatch)
                        }
                    }
                })
            })
            .await
            .map_err(step_error);

        let result = match preparation {
            Err(error) => Err(error),
            Ok(CallPreparation::Uncertain) => Err(StepError::Failed(OperationFailure::Uncertain {
                step: name,
            })),
            Ok(CallPreparation::Replay(output)) => {
                serde_json::from_slice(&output).map_err(|error| {
                    StepError::Failed(OperationFailure::InvalidPayload {
                        message: error.to_string(),
                    })
                })
            }
            Ok(CallPreparation::Dispatch) => {
                match self
                    .host
                    .dispatch(
                        target,
                        request,
                        CallId {
                            operation: id,
                            step: name.clone(),
                        },
                    )
                    .await
                {
                    Ok(answer) => {
                        let output = serde_json::to_vec(&answer).map_err(|error| {
                            StepError::Failed(OperationFailure::InvalidPayload {
                                message: error.to_string(),
                            })
                        })?;
                        self.store
                            .transaction(move |scope| {
                                Box::pin(async move {
                                    scope.check_token(id, token).await?;
                                    scope
                                        .save_step(
                                            StepRecord {
                                                operation: id,
                                                position,
                                                name,
                                                state: StepState::CallDone {
                                                    target: address,
                                                    request_digest: digest,
                                                    replay: C::REPLAY,
                                                    output,
                                                },
                                            },
                                            token,
                                        )
                                        .await
                                })
                            })
                            .await
                            .map_err(StepError::Storage)
                            .map(|()| answer)
                    }
                    Err(CallFailure::Refused { reason }) => {
                        Err(StepError::Failed(OperationFailure::Refused {
                            code: "call_refused".into(),
                            detail: reason,
                        }))
                    }
                    Err(CallFailure::AnswerLost) if C::REPLAY == CallReplay::NeverRepeat => {
                        Err(StepError::Failed(OperationFailure::Uncertain {
                            step: name,
                        }))
                    }
                    Err(CallFailure::AnswerLost) => Err(StepError::Retry {
                        after: std::time::Duration::ZERO,
                    }),
                    Err(CallFailure::Retry { after }) => Err(StepError::Retry { after }),
                }
            }
        };
        self.record_result(&result);
        result
    }
}

impl<S: AtomicStore, H: Host> EngineContext<'_, S, H> {
    fn record_result<T>(&mut self, result: &Result<T, StepError>) {
        match result {
            Ok(_) => self.position += 1,
            Err(error) => self.failure = Some(error.clone().into()),
        }
    }
}
