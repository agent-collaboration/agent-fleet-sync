//! Persist a dispatch before sending; save its answer before advancing the task.

use crate::{
    AtomicStore, CallFailure, CallId, CallReplay, CallResult, CallTarget, Host, NotSentReason,
    OperationFailure, StepError, StepName,
    operation_context::{EngineContext, changed_step, invalid_payload, step_error, validate_name},
    storage::{CallAttempt, CallIntent, CallReach, JournalStorage, StepRecord, StepState},
};
use sha2::{Digest, Sha256};
use std::time::Duration;

enum Preparation {
    Replay(Vec<u8>),
    Dispatch(CallAttempt),
    Reconcile(CallAttempt),
    Uncertain,
}

enum AnswerSource {
    Dispatch,
    Reconciliation,
}

impl<S: AtomicStore, H: Host> EngineContext<'_, S, H> {
    pub(crate) async fn execute_call<C: CallTarget>(
        &mut self,
        name: StepName,
        target: C,
        request: C::Request,
        unsent_for: Option<Duration>,
    ) -> Result<CallResult<C::Answer>, StepError> {
        let id = self.operation;
        let token = self.token;
        let position = self.position;
        let now = self.host.now();
        let request_bytes = crate::canonical_payload::encode(&request).map_err(invalid_payload)?;
        let intent = CallIntent {
            target: String::from_utf8(
                crate::canonical_payload::encode(&(C::KIND, &target)).map_err(invalid_payload)?,
            )
            .map_err(|error| step_error(invalid_payload(error)))?,
            request_digest: Sha256::digest(&request_bytes).to_vec(),
            replay: if C::DEDUPLICATES {
                CallReplay::Deduplicated
            } else {
                CallReplay::NeverRepeat
            },
            unsent_for,
        };
        let expected = intent.clone();
        let step_name = name.clone();
        let preparation = self
            .store
            .transaction(move |scope| {
                Box::pin(async move {
                    scope.check_token(id, token).await?;
                    validate_name(scope, id, position, &step_name).await?;
                    let mut attempt = match scope.step(id, position).await? {
                        Some(saved) => match saved.state {
                            StepState::CallDone { intent, output, .. } if intent == expected => {
                                return Ok(Preparation::Replay(output));
                            }
                            StepState::CallPending {
                                intent,
                                mut attempt,
                            } if intent == expected => {
                                if attempt.reach == CallReach::Dispatching {
                                    attempt.reach = CallReach::MayHaveReached;
                                }
                                if !C::DEDUPLICATES
                                    && !C::RECONCILES
                                    && attempt.reach == CallReach::MayHaveReached
                                {
                                    return Ok(Preparation::Uncertain);
                                }
                                attempt
                            }
                            _ => {
                                return Err(changed_step(
                                    position,
                                    "recorded call identity/content",
                                    "changed call identity/content",
                                ));
                            }
                        },
                        None => CallAttempt {
                            first_attempt_at: now,
                            last_seen: None,
                            reach: CallReach::NeverSent,
                            retry_reason: None,
                        },
                    };
                    let previous = attempt.clone();
                    if matches!(attempt.reach, CallReach::NeverSent | CallReach::NoEffect) {
                        attempt.reach = CallReach::Dispatching;
                    }
                    scope
                        .save_step(
                            StepRecord {
                                operation: id,
                                position,
                                name: step_name,
                                state: StepState::CallPending {
                                    intent: expected,
                                    attempt,
                                },
                            },
                            token,
                        )
                        .await?;
                    Ok(
                        if !C::DEDUPLICATES
                            && C::RECONCILES
                            && previous.reach == CallReach::MayHaveReached
                        {
                            Preparation::Reconcile(previous)
                        } else {
                            Preparation::Dispatch(previous)
                        },
                    )
                })
            })
            .await
            .map_err(step_error)?;

        let call = CallId {
            operation: id,
            step: name.clone(),
        };
        let (mut attempt, answer, answer_source) = match preparation {
            Preparation::Replay(bytes) => {
                return serde_json::from_slice(&bytes)
                    .map_err(|error| step_error(invalid_payload(error)));
            }
            Preparation::Uncertain => {
                return Err(StepError::Failed(OperationFailure::Uncertain {
                    step: name,
                }));
            }
            Preparation::Dispatch(mut attempt) => {
                // Host dispatch consumes its inputs. Preserve the exact checked
                // wire values before an effect, without adding Clone bounds.
                let reconciliation_input = if !C::DEDUPLICATES && C::RECONCILES {
                    let (_, saved_target): (String, C) = serde_json::from_str(&intent.target)
                        .map_err(|error| step_error(invalid_payload(error)))?;
                    let saved_request = serde_json::from_slice::<C::Request>(&request_bytes)
                        .map_err(|error| step_error(invalid_payload(error)))?;
                    Some((saved_target, saved_request))
                } else {
                    None
                };
                let mut answer = self.host.dispatch(target, request, call.clone()).await;
                let mut answer_source = AnswerSource::Dispatch;
                if matches!(answer, Err(CallFailure::AnswerLost))
                    && let Some((target, request)) = reconciliation_input
                {
                    attempt.reach = CallReach::MayHaveReached;
                    answer_source = AnswerSource::Reconciliation;
                    answer = self
                        .host
                        .reconcile(target, request, call)
                        .await
                        .and_then(|answer| answer.ok_or(CallFailure::AnswerLost));
                }
                (attempt, answer, answer_source)
            }
            Preparation::Reconcile(attempt) => {
                // Recovery asks only for an existing answer. No branch here
                // may fall back to dispatching this non-repeating effect.
                match self.host.reconcile(target, request, call).await {
                    Ok(Some(answer)) => (attempt, Ok(answer), AnswerSource::Reconciliation),
                    Ok(None) => {
                        return Err(StepError::Failed(OperationFailure::Uncertain {
                            step: name,
                        }));
                    }
                    Err(error) => (attempt, Err(error), AnswerSource::Reconciliation),
                }
            }
        };
        attempt.retry_reason = None;
        let result = match answer {
            Ok(answer) => Some(CallResult::Answered(answer)),
            Err(CallFailure::AnswerLost) => {
                attempt.reach = CallReach::MayHaveReached;
                None
            }
            Err(CallFailure::Retryable { reason }) => {
                if matches!(attempt.reach, CallReach::NeverSent | CallReach::NoEffect) {
                    attempt.reach = CallReach::NoEffect;
                }
                // A later refusal cannot erase an earlier possible effect.
                attempt.retry_reason = Some(reason);
                None
            }
            Err(CallFailure::NotSent { reason }) => {
                if let NotSentReason::Unreachable { last_seen } = reason {
                    attempt.last_seen = last_seen;
                }
                if attempt.reach == CallReach::NeverSent
                    && unsent_for.is_some_and(|bound| {
                        self.host.now() > attempt.first_attempt_at.after(bound)
                    })
                {
                    Some(CallResult::NeverSent {
                        last_seen: attempt.last_seen,
                    })
                } else {
                    None
                }
            }
        };
        let state = match &result {
            Some(result) => StepState::CallDone {
                intent,
                attempt: attempt.clone(),
                output: serde_json::to_vec(result)
                    .map_err(|error| step_error(invalid_payload(error)))?,
            },
            None => StepState::CallPending {
                intent,
                attempt: attempt.clone(),
            },
        };
        let uncertain_step = name.clone();
        self.store
            .transaction(move |scope| {
                Box::pin(async move {
                    scope
                        .save_step(
                            StepRecord {
                                operation: id,
                                position,
                                name,
                                state,
                            },
                            token,
                        )
                        .await
                })
            })
            .await?;
        match result {
            Some(result) => Ok(result),
            None if matches!(answer_source, AnswerSource::Reconciliation)
                && attempt.retry_reason.is_some() =>
            {
                // The effect's possible reach stays fenced. Only the read-only
                // lookup is retried, through the engine's ordinary backoff.
                Err(StepError::Retry {
                    after: Duration::ZERO,
                })
            }
            None if !C::DEDUPLICATES && attempt.reach == CallReach::MayHaveReached => {
                Err(StepError::Failed(OperationFailure::Uncertain {
                    step: uncertain_step,
                }))
            }
            None => Err(StepError::Retry {
                after: Duration::ZERO,
            }),
        }
    }
}
