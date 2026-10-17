use durable_steps::{CallFailure, NotSentReason, turso_store::TursoScope};
use fleet_sync_tokio_runner::{CallEnvelope, CallRouter, TokioHost, TokioObject};
use serde_json::Value;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

pub type ServiceHost = TokioHost<ServiceCalls>;
pub type ServiceObject = TokioObject<ServiceHost>;
pub type ServiceScope<'transaction> = TursoScope<'transaction, ServiceHost>;

pub(crate) struct RoutedCall {
    pub envelope: CallEnvelope,
    pub answer: oneshot::Sender<Result<Value, CallFailure>>,
}

/// A volatile carrier for calls already recorded by the engine. The journal
/// retries a lost carrier under the same CallId after a process restart.
#[derive(Clone)]
pub struct ServiceCalls {
    pub(crate) sender: mpsc::Sender<RoutedCall>,
    pub(crate) answer_timeout: Duration,
}
impl CallRouter for ServiceCalls {
    async fn dispatch(&self, envelope: CallEnvelope) -> Result<Value, CallFailure> {
        let (answer, received) = oneshot::channel();
        self.sender
            .try_send(RoutedCall { envelope, answer })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => CallFailure::NotSent {
                    reason: NotSentReason::Overloaded,
                },
                mpsc::error::TrySendError::Closed(_) => unreachable(),
            })?;
        match tokio::time::timeout(self.answer_timeout, received).await {
            Ok(Ok(answer)) => answer,
            _ => Err(CallFailure::AnswerLost),
        }
    }
}
pub(crate) fn unreachable() -> CallFailure {
    CallFailure::NotSent {
        reason: NotSentReason::Unreachable { last_seen: None },
    }
}
pub(crate) fn lost(_: impl std::fmt::Display) -> CallFailure {
    CallFailure::AnswerLost
}
